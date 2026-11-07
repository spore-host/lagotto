package watcher

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/spore-host/lagotto/pkg/testutil"
)

// Hermetic tests for the #240 email-subscription lifecycle, against
// testutil.FakeSNS, which models pending confirmation and filter-policy
// delivery.

const alertsARN = "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts"

func emailWatch(id string, status WatchStatus, addrs ...string) Watch {
	w := Watch{WatchID: id, Status: status, InstanceTypePattern: "p5.48xlarge", Action: ActionNotify,
		Regions: []string{"us-west-2"}, CreatedAt: time.Now().Add(-time.Hour), UpdatedAt: time.Now()}
	for _, a := range addrs {
		w.NotifyChannels = append(w.NotifyChannels, NotifyChannel{Type: "email", Target: a})
	}
	return w
}

// TestEnsure_SubscribesWithFilterPolicy: the subscription is email protocol,
// to the normalized address, with a MessageAttributes-scoped filter policy
// that selects only that address.
func TestEnsure_SubscribesWithFilterPolicy(t *testing.T) {
	f := testutil.NewFakeSNS(alertsARN)
	res, err := NewEmailSubscriptions(f, alertsARN).Ensure(context.Background(), "  Alice@Example.com ")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || res.State != SubPending || res.Address != "alice@example.com" {
		t.Errorf("result = %+v, want created, pending, alice@example.com", res)
	}
	if len(f.Subscribes) != 1 {
		t.Fatalf("Subscribe calls = %d, want 1", len(f.Subscribes))
	}
	in := f.Subscribes[0]
	if aws.ToString(in.TopicArn) != alertsARN || aws.ToString(in.Protocol) != "email" || aws.ToString(in.Endpoint) != "alice@example.com" {
		t.Errorf("Subscribe(%s, %s, %s), want the alerts topic, email, alice@example.com",
			aws.ToString(in.TopicArn), aws.ToString(in.Protocol), aws.ToString(in.Endpoint))
	}
	want := map[string]string{
		"FilterPolicy":      `{"lagotto_email":["alice@example.com"]}`,
		"FilterPolicyScope": "MessageAttributes",
	}
	if !reflect.DeepEqual(in.Attributes, want) {
		t.Errorf("Subscribe attributes = %v, want %v", in.Attributes, want)
	}
}

// TestEnsure_NoDuplicateSubscription: an address that already has a
// subscription, pending or confirmed and in any spelling, is reused.
func TestEnsure_NoDuplicateSubscription(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	res, err := es.Ensure(ctx, "ALICE@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.State != SubPending {
		t.Errorf("second Ensure while pending = %+v, want reused and pending", res)
	}
	f.Confirm("alice@example.com")
	res, err = es.Ensure(ctx, "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.State != SubConfirmed || res.Unfiltered {
		t.Errorf("Ensure after confirmation = %+v, want reused, confirmed, filtered", res)
	}
	if len(f.Subscribes) != 1 || len(f.Subs) != 1 {
		t.Errorf("Subscribe calls = %d, subscriptions = %d; want 1 and 1", len(f.Subscribes), len(f.Subs))
	}
}

// TestEnsure_ReusesUnfilteredHandSubscription: a hand-made subscription with
// no filter policy is reused and reported, not replaced or duplicated.
func TestEnsure_ReusesUnfilteredHandSubscription(t *testing.T) {
	f := testutil.NewFakeSNS(alertsARN)
	f.AddSub("alice@example.com", nil, true)
	res, err := NewEmailSubscriptions(f, alertsARN).Ensure(context.Background(), "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || !res.Unfiltered || len(f.Subscribes) != 0 {
		t.Errorf("result = %+v, Subscribe calls = %d; want reused, unfiltered, no Subscribe", res, len(f.Subscribes))
	}
}

// TestEmailPublish_CarriesAddressAttribute: every notification kind's email
// publish carries lagotto_email set to that channel's address, one publish
// per address.
func TestEmailPublish_CarriesAddressAttribute(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	n := &Notifier{snsClient: f, subs: f, topicArn: alertsARN}
	w := emailWatch("w-two", StatusActive, "alice@example.com", "Bob@Example.com")
	m := &MatchResult{InstanceType: "p5.48xlarge", Region: "us-west-2", MatchedAt: time.Now()}

	sends := map[string]func() error{
		"match":       func() error { return n.Notify(ctx, &w, m) },
		"quota cap":   func() error { return n.NotifyQuotaCap(ctx, &w, 1, "capped") },
		"expired":     func() error { return n.NotifyExpired(ctx, &w) },
		"unsupported": func() error { return n.NotifyUnsupported(ctx, &w, "retired") },
	}
	for name, send := range sends {
		f.Published = nil
		if err := send(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var got []string
		for _, p := range f.Published {
			attr, ok := p.MessageAttributes[EmailAttribute]
			if !ok {
				t.Errorf("%s: an email publish has no %s attribute", name, EmailAttribute)
				continue
			}
			if aws.ToString(attr.DataType) != "String" {
				t.Errorf("%s: %s DataType = %q, want String", name, EmailAttribute, aws.ToString(attr.DataType))
			}
			got = append(got, aws.ToString(attr.StringValue))
		}
		if want := []string{"alice@example.com", "bob@example.com"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: published for %v, want %v", name, got, want)
		}
	}
}

// TestEmailFilter_IsolatesAddresses is the cross-user leak (#240): alice and
// bob are both subscribed and confirmed; an event of alice's watch reaches
// alice only, bob's reaches bob only, and a publish without the attribute (an
// sns: channel aimed at the alerts topic) reaches neither.
func TestEmailFilter_IsolatesAddresses(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	for _, a := range []string{"alice@example.com", "bob@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
		f.Confirm(a)
	}
	n := &Notifier{snsClient: f, subs: f, topicArn: alertsARN}
	alice := emailWatch("w-alice", StatusActive, "alice@example.com")
	bob := emailWatch("w-bob", StatusActive, "bob@example.com")

	for _, tc := range []struct {
		w    *Watch
		want string
	}{{&alice, "alice@example.com"}, {&bob, "bob@example.com"}} {
		f.Published = nil
		if err := n.NotifyExpired(ctx, tc.w); err != nil {
			t.Fatal(err)
		}
		if len(f.Published) != 1 {
			t.Fatalf("%s: publishes = %d, want 1", tc.w.WatchID, len(f.Published))
		}
		if got := f.Deliveries(f.Published[0]); !reflect.DeepEqual(got, []string{tc.want}) {
			t.Errorf("%s's expiry email is delivered to %v, want only %s", tc.w.WatchID, got, tc.want)
		}
	}

	// An sns: channel pointed at the alerts topic carries no attribute.
	raw := emailWatch("w-raw", StatusActive)
	raw.NotifyChannels = []NotifyChannel{{Type: "sns", Target: alertsARN}}
	f.Published = nil
	if err := n.NotifyExpired(ctx, &raw); err != nil {
		t.Fatal(err)
	}
	if got := f.Deliveries(f.Published[0]); len(got) != 0 {
		t.Errorf("an unattributed publish to the alerts topic is delivered to %v, want no email subscriber", got)
	}
}

// TestReconcile_UnsubscribesOnlyUnusedManaged: an address is removed only
// when no active watch names it; ended watches do not count; subscriptions
// lagotto did not create and pending ones are left.
func TestReconcile_UnsubscribesOnlyUnusedManaged(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	for _, a := range []string{"alice@example.com", "bob@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
		f.Confirm(a)
	}
	if _, err := es.Ensure(ctx, "dave@example.com"); err != nil { // never confirmed
		t.Fatal(err)
	}
	f.AddSub("carol@example.com", nil, true) // hand-made, unfiltered
	f.AddSub("erin@example.com", map[string]string{"FilterPolicy": `{"team":["x"]}`}, true)

	// alice: one watch ended, another still active. bob: only an ended watch.
	watches := []Watch{
		emailWatch("w-a1", StatusExpired, "alice@example.com"),
		emailWatch("w-a2", StatusActive, "Alice@Example.com"),
		emailWatch("w-b1", StatusCancelled, "bob@example.com"),
	}
	res, err := es.Reconcile(ctx, watches, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"bob@example.com"}) {
		t.Errorf("Removed = %v, want [bob@example.com]: alice is still named by active w-a2", res.Removed)
	}
	if !reflect.DeepEqual(res.PendingUnused, []string{"dave@example.com"}) {
		t.Errorf("PendingUnused = %v, want [dave@example.com]", res.PendingUnused)
	}
	if !reflect.DeepEqual(f.Unsubs, []string{"bob@example.com"}) {
		t.Errorf("Unsubscribe calls for %v, want [bob@example.com]", f.Unsubs)
	}

	// The last watch naming alice ends.
	watches[1].Status = StatusMatched
	res, err = es.Reconcile(ctx, watches, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"alice@example.com"}) {
		t.Errorf("Removed = %v after the last watch ended, want [alice@example.com]", res.Removed)
	}
	var left []string
	for _, s := range f.Subs {
		left = append(left, s.Endpoint)
	}
	if want := []string{"dave@example.com", "carol@example.com", "erin@example.com"}; !reflect.DeepEqual(left, want) {
		t.Errorf("subscriptions left = %v, want %v (pending, hand-made and foreign-filter ones untouched)", left, want)
	}
}

// TestReconcile_OnlyScope: with only, the other unused addresses are left.
func TestReconcile_OnlyScope(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	for _, a := range []string{"alice@example.com", "bob@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
		f.Confirm(a)
	}
	res, err := es.Reconcile(ctx, nil, []string{"ALICE@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"alice@example.com"}) || len(f.Subs) != 1 {
		t.Errorf("Removed = %v, subscriptions left = %d; want only alice removed", res.Removed, len(f.Subs))
	}
}

// TestNoTopic: with no alerts topic the notifier refuses email and its
// cleanup is a no-op; Audit reports every active address as missing.
func TestNoTopic(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	n := &Notifier{snsClient: f, subs: f, topicArn: ""}
	w := emailWatch("w-a", StatusActive, "alice@example.com")
	if err := n.NotifyExpired(ctx, &w); !errors.Is(err, ErrNoAlertsTopic) {
		t.Errorf("NotifyExpired with no topic: err = %v, want ErrNoAlertsTopic", err)
	}
	if _, err := n.ReconcileEmailSubscriptions(ctx, []Watch{w}); err != nil {
		t.Errorf("ReconcileEmailSubscriptions with no topic: %v", err)
	}
	if len(f.Published)+len(f.Subscribes)+len(f.Unsubs) != 0 {
		t.Error("an SNS call was made with no alerts topic")
	}

	f.NoTopic = true
	es := NewEmailSubscriptions(f, alertsARN)
	if _, err := es.Ensure(ctx, "alice@example.com"); !IsTopicNotFound(err) {
		t.Errorf("Ensure on a missing topic: err = %v, want NotFound", err)
	}
	audit, err := es.Audit(ctx, []Watch{w})
	if err != nil {
		t.Fatal(err)
	}
	if !audit.NoTopic || !reflect.DeepEqual(audit.Missing, []string{"alice@example.com"}) {
		t.Errorf("audit = %+v, want NoTopic with alice missing", audit)
	}
}

// TestAudit_ReportsPendingMissingUnusedUnfiltered feeds doctor.
func TestAudit_ReportsPendingMissingUnusedUnfiltered(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	for _, a := range []string{"alice@example.com", "bob@example.com", "old@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	f.Confirm("alice@example.com")
	f.Confirm("old@example.com")
	f.AddSub("carol@example.com", nil, true)
	audit, err := es.Audit(ctx, []Watch{
		emailWatch("w1", StatusActive, "alice@example.com", "bob@example.com", "zed@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	check := func(name string, got, want []string) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	check("Confirmed", audit.Confirmed, []string{"alice@example.com"})
	check("Pending", audit.Pending, []string{"bob@example.com"})
	check("Missing", audit.Missing, []string{"zed@example.com"})
	check("Unused", audit.Unused, []string{"old@example.com"})
	if len(audit.Unfiltered) != 1 || audit.Unfiltered[0].Address != "carol@example.com" {
		t.Errorf("Unfiltered = %+v, want carol", audit.Unfiltered)
	}
}

// TestPollAll_UnsubscribesOnTheSweepAfterTheWatchEnds drives the poller's
// half of the deleter through a real sweep: the sweep that expires alice's
// watch still delivers her expiry email (her subscription is kept, since the
// cleanup runs on the list loaded before the transition), and the next sweep
// unsubscribes her.
func TestPollAll_UnsubscribesOnTheSweepAfterTheWatchEnds(t *testing.T) {
	ctx := context.Background()
	store := expiryTestStore(t)
	w := expiredWatch("w-alice")
	w.NotifyChannels = []NotifyChannel{{Type: "email", Target: "alice@example.com"}}
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	f := testutil.NewFakeSNS(alertsARN)
	if _, err := NewEmailSubscriptions(f, alertsARN).Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	f.Confirm("alice@example.com")

	p := NewPoller(nil, store, false, PollerOpts{Notifier: &Notifier{snsClient: f, subs: f, topicArn: alertsARN}})
	if _, err := p.PollAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.Published) != 1 {
		t.Fatalf("publishes = %d, want the one expiry email", len(f.Published))
	}
	if got := f.Deliveries(f.Published[0]); !reflect.DeepEqual(got, []string{"alice@example.com"}) {
		t.Errorf("expiry email delivered to %v, want [alice@example.com]", got)
	}
	if len(f.Unsubs) != 0 {
		t.Errorf("unsubscribed %v in the sweep that sent the last email", f.Unsubs)
	}

	if _, err := p.PollAll(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.Unsubs, []string{"alice@example.com"}) || len(f.Subs) != 0 {
		t.Errorf("after the next sweep: unsubscribed %v, %d subscription(s) left; want alice removed", f.Unsubs, len(f.Subs))
	}
}

func TestValidateEmail(t *testing.T) {
	for _, ok := range []string{"a@b.c", " Alice@Example.com "} {
		if err := ValidateEmail(ok); err != nil {
			t.Errorf("ValidateEmail(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "alice", "@b.c", "a@", "a@@b", "a b@c.d"} {
		if err := ValidateEmail(bad); err == nil || !strings.Contains(err.Error(), "invalid email") {
			t.Errorf("ValidateEmail(%q) = %v, want an error", bad, err)
		}
	}
}

// TestStatesAndMissing: per-address state, the best state winning for an
// address subscribed twice, and the active addresses with no subscription.
func TestStatesAndMissing(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	if es.TopicArn() != alertsARN {
		t.Errorf("TopicArn = %q", es.TopicArn())
	}
	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	f.AddSub("Bob@example.com", nil, false)
	f.AddSub("bob@example.com", map[string]string{"FilterPolicy": "{}"}, true)
	states, err := es.States(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]SubscriptionState{"alice@example.com": SubPending, "bob@example.com": SubConfirmed}; !reflect.DeepEqual(states, want) {
		t.Errorf("States = %v, want %v", states, want)
	}
	missing, err := es.MissingEmailSubscriptions(ctx, []Watch{
		emailWatch("w1", StatusActive, "alice@example.com", "zed@example.com"),
		emailWatch("w2", StatusExpired, "old@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(missing, []string{"zed@example.com"}) {
		t.Errorf("Missing = %v, want [zed@example.com]", missing)
	}
	f.NoTopic = true
	if _, err := es.States(ctx); !IsTopicNotFound(err) {
		t.Errorf("States on a missing topic: %v", err)
	}
	if _, err := es.MissingEmailSubscriptions(ctx, nil); err == nil {
		t.Error("MissingEmailSubscriptions on a missing topic: no error")
	}
}

func TestSubscriptionStateDescribe(t *testing.T) {
	for s, want := range map[SubscriptionState]string{
		SubConfirmed: "confirmed",
		SubPending:   "check your inbox",
		SubNone:      "notify-gc --resubscribe",
		SubNoTopic:   "lagotto deploy",
		SubUnknown:   "unknown",
	} {
		if got := s.Describe(); !strings.Contains(got, want) {
			t.Errorf("%s.Describe() = %q, want it to contain %q", s, got, want)
		}
	}
}

type codeErr string

func (c codeErr) Error() string     { return string(c) }
func (c codeErr) ErrorCode() string { return string(c) }

func TestIsTopicNotFoundAndManagedFilter(t *testing.T) {
	if !IsTopicNotFound(codeErr("NotFound")) || IsTopicNotFound(codeErr("AuthorizationError")) || IsTopicNotFound(errors.New("x")) {
		t.Error("IsTopicNotFound misclassifies")
	}
	for policy, want := range map[string]string{
		`{"lagotto_email":["a@b.c"]}`:           "a@b.c",
		"":                                      "",
		"not json":                              "",
		`{"lagotto_email":["a@b.c","d@e.f"]}`:   "",
		`{"lagotto_email":["a@b.c"],"x":["y"]}`: "",
		`{"team":["a@b.c"]}`:                    "",
	} {
		if got := managedFilterAddress(policy); got != want {
			t.Errorf("managedFilterAddress(%q) = %q, want %q", policy, got, want)
		}
	}
}

// TestReconcile_ErrorsAreReturnedAndOthersStillRun: a failed Unsubscribe is
// reported, and the other candidates are still processed.
func TestReconcile_ErrorsAreReturnedAndOthersStillRun(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN)
	es := NewEmailSubscriptions(f, alertsARN)
	for _, a := range []string{"alice@example.com", "bob@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
		f.Confirm(a)
	}
	f.UnsubscribeErr = map[string]error{"alice@example.com": errors.New("throttled")}
	res, err := es.Reconcile(ctx, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "alice@example.com") {
		t.Errorf("Reconcile err = %v, want alice's Unsubscribe failure", err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"bob@example.com"}) {
		t.Errorf("Removed = %v, want bob still removed", res.Removed)
	}
	if _, err := (&Notifier{snsClient: f, topicArn: alertsARN}).ReconcileEmailSubscriptions(ctx, nil); err != nil {
		t.Errorf("notifier without subs: %v", err)
	}
	if err := (&Notifier{snsClient: f, topicArn: alertsARN}).publishEmail(ctx, " ", "s", "b"); err == nil {
		t.Error("publishEmail with no address: no error")
	}
	f.NoTopic = true
	if _, err := es.Reconcile(ctx, nil, nil); err == nil {
		t.Error("Reconcile on a missing topic: no error")
	}
}

// TestPollAll_ReconcileFailureDoesNotStopTheSweep.
func TestPollAll_ReconcileFailureDoesNotStopTheSweep(t *testing.T) {
	ctx := context.Background()
	store := expiryTestStore(t)
	w := expiredWatch("w-x")
	if err := store.PutWatch(ctx, w); err != nil {
		t.Fatal(err)
	}
	p := NewPoller(nil, store, false)
	called := 0
	p.reconcileEmail = func(context.Context, []Watch) (ReconcileResult, error) {
		called++
		return ReconcileResult{Removed: []string{"a@b.c"}}, errors.New("AccessDenied")
	}
	summary, err := p.PollAll(ctx)
	if err != nil || called != 1 || summary.Expired != 1 {
		t.Errorf("PollAll: err = %v, reconcile calls = %d, expired = %d", err, called, summary.Expired)
	}
}

// TestCrossRegionTopic_EveryCallUsesTheARNRegion is the live-test bug on #248:
// with LAGOTTO_SNS_TOPIC_ARN in us-west-2 and the CLI in us-east-2, every SNS
// call ran in us-east-2 and SNS refused the topic ARN. Now every call on the
// alerts topic runs in the topic's region, and an sns: channel's publish runs
// in its own topic's region.
func TestCrossRegionTopic_EveryCallUsesTheARNRegion(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(alertsARN) // us-west-2
	f.ClientRegion = "us-east-2"        // the CLI / store region
	es := NewEmailSubscriptions(f, alertsARN)

	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatalf("Ensure across regions: %v", err)
	}
	f.Confirm("alice@example.com")
	if _, err := es.Ensure(ctx, "bob@example.com"); err != nil {
		t.Fatal(err)
	}
	f.Confirm("bob@example.com")
	if _, err := es.States(ctx); err != nil {
		t.Fatalf("States: %v", err)
	}
	if _, err := es.Audit(ctx, nil); err != nil {
		t.Fatalf("Audit: %v", err)
	}
	n := &Notifier{snsClient: f, subs: f, topicArn: alertsARN}
	w := emailWatch("w-a", StatusActive, "alice@example.com")
	if err := n.NotifyExpired(ctx, &w); err != nil {
		t.Fatalf("email publish across regions: %v", err)
	}
	res, err := n.ReconcileEmailSubscriptions(ctx, []Watch{w})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !reflect.DeepEqual(res.Removed, []string{"bob@example.com"}) {
		t.Errorf("Removed = %v", res.Removed)
	}
	for _, c := range f.Calls {
		if !strings.HasSuffix(c, " us-west-2") {
			t.Errorf("%s: not in the topic's region us-west-2", c)
		}
	}

	// An sns: channel's own topic in a third region.
	own := "arn:aws:sns:eu-west-1:111122223333:my-topic"
	f.Calls = nil
	raw := emailWatch("w-raw", StatusActive)
	raw.NotifyChannels = []NotifyChannel{{Type: "sns", Target: own}}
	m := &MatchResult{InstanceType: "p5.48xlarge", Region: "us-east-2", MatchedAt: time.Now()}
	sends := map[string]func() error{
		"match":       func() error { return n.Notify(ctx, &raw, m) },
		"quota cap":   func() error { return n.NotifyQuotaCap(ctx, &raw, 1, "capped") },
		"expired":     func() error { return n.NotifyExpired(ctx, &raw) },
		"unsupported": func() error { return n.NotifyUnsupported(ctx, &raw, "retired") },
	}
	for name, send := range sends {
		f.Calls = nil
		if err := send(); err != nil {
			t.Errorf("%s: sns channel in eu-west-1: %v", name, err)
		}
		if !reflect.DeepEqual(f.Calls, []string{"Publish eu-west-1"}) {
			t.Errorf("%s: calls = %v, want one Publish in eu-west-1", name, f.Calls)
		}
	}
}

func TestARNRegion(t *testing.T) {
	for arn, want := range map[string]string{
		alertsARN:             "us-west-2",
		alertsARN + ":0f1e":   "us-west-2",
		"PendingConfirmation": "",
		"":                    "",
	} {
		if got := ARNRegion(arn); got != want {
			t.Errorf("ARNRegion(%q) = %q, want %q", arn, got, want)
		}
	}
}

// hostCapture is an HTTP client that records each request and never dials.
type hostCapture struct{ hosts, auth []string }

func (h *hostCapture) Do(r *http.Request) (*http.Response, error) {
	h.hosts = append(h.hosts, r.URL.Host)
	h.auth = append(h.auth, r.Header.Get("Authorization"))
	return nil, errors.New("captured")
}

// TestInARNRegion_RealSDKSendsToTheTopicRegion: with the real SNS client built
// for us-east-2, a call carrying InARNRegion goes to us-west-2's endpoint and
// is signed for us-west-2, so the per-call option is honoured by the SDK, not
// only by the fake.
func TestInARNRegion_RealSDKSendsToTheTopicRegion(t *testing.T) {
	h := &hostCapture{}
	c := sns.New(sns.Options{
		Region:      "us-east-2",
		Credentials: credentials.NewStaticCredentialsProvider("AKIDTEST", "secret", ""),
		HTTPClient:  h,
		Retryer:     aws.NopRetryer{},
	})
	_, _ = NewEmailSubscriptions(c, alertsARN).List(context.Background(), false)
	if len(h.hosts) != 1 || h.hosts[0] != "sns.us-west-2.amazonaws.com" {
		t.Errorf("request went to %v, want sns.us-west-2.amazonaws.com", h.hosts)
	}
	if len(h.auth) != 1 || !strings.Contains(h.auth[0], "/us-west-2/sns/") {
		t.Errorf("signed as %v, want a us-west-2 SigV4 scope", h.auth)
	}
}
