package cmd

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Hermetic tests for the CLI side of #240, against testutil.FakeSNS through
// the real watcher.EmailSubscriptions.

const testAlerts = "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts"

func fakeSubs() (*testutil.FakeSNS, *watcher.EmailSubscriptions) {
	f := testutil.NewFakeSNS(testAlerts)
	return f, watcher.NewEmailSubscriptions(f, testAlerts)
}

func cmdEmailWatch(id string, status watcher.WatchStatus, addrs ...string) watcher.Watch {
	w := watcher.Watch{WatchID: id, Status: status}
	for _, a := range addrs {
		w.NotifyChannels = append(w.NotifyChannels, watcher.NotifyChannel{Type: "email", Target: a})
	}
	return w
}

// TestSubscribeWatchEmails_SubscribesAndSaysCheckInbox: watch subscribes each
// address with the filter policy and tells the user to confirm.
func TestSubscribeWatchEmails_SubscribesAndSaysCheckInbox(t *testing.T) {
	f, es := fakeSubs()
	w := cmdEmailWatch("w-1", watcher.StatusActive, "alice@example.com")
	w.NotifyChannels = append(w.NotifyChannels, watcher.NotifyChannel{Type: "webhook", Target: "https://192.0.2.10/h"})
	var errOut bytes.Buffer
	if err := subscribeWatchEmails(context.Background(), &errOut, alertsTopic{ARN: testAlerts}, es, &w); err != nil {
		t.Fatal(err)
	}
	if len(f.Subscribes) != 1 || f.Subscribes[0].Attributes["FilterPolicy"] != `{"lagotto_email":["alice@example.com"]}` {
		t.Fatalf("Subscribe calls = %+v, want one with alice's filter policy", f.Subscribes)
	}
	if got := w.NotifyChannels[0].Subscription; got != watcher.SubPending {
		t.Errorf("channel state = %q, want pending", got)
	}
	if w.NotifyChannels[1].Subscription != "" {
		t.Error("a webhook channel got a subscription state")
	}
	var out bytes.Buffer
	writeEmailStates(&out, "  Notify:   ", &w)
	if !strings.Contains(out.String(), "email:alice@example.com  pending confirmation: check your inbox") {
		t.Errorf("watch output = %q, want the check-your-inbox line", out.String())
	}

	// A second watch for the same address reuses the subscription.
	w2 := cmdEmailWatch("w-2", watcher.StatusActive, "alice@example.com")
	if err := subscribeWatchEmails(context.Background(), &errOut, alertsTopic{ARN: testAlerts}, es, &w2); err != nil {
		t.Fatal(err)
	}
	if len(f.Subs) != 1 {
		t.Errorf("subscriptions = %d after a second watch for the same address, want 1", len(f.Subs))
	}
}

// TestSubscribeWatchEmails_NoTopicRefuses: with no alerts topic, the watch is
// refused before anything is stored, naming `lagotto deploy`.
func TestSubscribeWatchEmails_NoTopicRefuses(t *testing.T) {
	w := cmdEmailWatch("w-1", watcher.StatusActive, "alice@example.com")
	err := subscribeWatchEmails(context.Background(), &bytes.Buffer{},
		alertsTopic{Why: "lagotto-capacity-alerts does not exist in account 1, region us-west-2 (`lagotto deploy` creates it)"}, nil, &w)
	if err == nil || !strings.Contains(err.Error(), "lagotto deploy") || !strings.Contains(err.Error(), "no watch was created") {
		t.Errorf("err = %v, want a refusal naming lagotto deploy", err)
	}
	// A watch with no email channel needs no topic.
	none := cmdEmailWatch("w-2", watcher.StatusActive)
	if err := subscribeWatchEmails(context.Background(), &bytes.Buffer{}, alertsTopic{}, nil, &none); err != nil {
		t.Errorf("no email channel: %v", err)
	}
	// A subscribe failure refuses too.
	f, es := fakeSubs()
	f.NoTopic = true
	if err := subscribeWatchEmails(context.Background(), &bytes.Buffer{}, alertsTopic{ARN: testAlerts}, es, &w); err == nil {
		t.Error("a failed Subscribe did not refuse the watch")
	}
}

// TestListAndStatus_ShowPendingState: list and status show each active
// watch's email channel state, and skip ended watches.
func TestListAndStatus_ShowPendingState(t *testing.T) {
	f, es := fakeSubs()
	ctx := context.Background()
	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := es.Ensure(ctx, "bob@example.com"); err != nil {
		t.Fatal(err)
	}
	f.Confirm("bob@example.com")
	watches := []watcher.Watch{
		cmdEmailWatch("w-alice", watcher.StatusActive, "alice@example.com"),
		cmdEmailWatch("w-bob", watcher.StatusActive, "bob@example.com"),
		cmdEmailWatch("w-zed", watcher.StatusActive, "zed@example.com"),
		cmdEmailWatch("w-old", watcher.StatusExpired, "old@example.com"),
	}
	ptrs := []*watcher.Watch{&watches[0], &watches[1], &watches[2], &watches[3]}
	annotateEmailStates(ctx, es, ptrs)

	var out bytes.Buffer
	writeListEmailStates(&out, watches)
	got := out.String()
	for _, want := range []string{
		"Email notifications:",
		"w-alice      alice@example.com  pending confirmation",
		"w-bob        bob@example.com  confirmed",
		"w-zed        zed@example.com  NOT subscribed",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("list output lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "old@example.com") {
		t.Errorf("list shows a state for an ended watch:\n%s", got)
	}

	out.Reset()
	writeNotifyChannels(&out, &watches[0])
	if !strings.Contains(out.String(), "Notify:   email:alice@example.com  pending confirmation: check your inbox") {
		t.Errorf("status output = %q", out.String())
	}

	// No topic: the channel says so instead.
	w := cmdEmailWatch("w-x", watcher.StatusActive, "alice@example.com")
	annotateEmailStates(ctx, nil, []*watcher.Watch{&w})
	if w.NotifyChannels[0].Subscription != watcher.SubNoTopic {
		t.Errorf("no topic: state = %q, want %q", w.NotifyChannels[0].Subscription, watcher.SubNoTopic)
	}
	f.NoTopic = true
	w = cmdEmailWatch("w-x", watcher.StatusActive, "alice@example.com")
	annotateEmailStates(ctx, es, []*watcher.Watch{&w})
	if w.NotifyChannels[0].Subscription != watcher.SubNoTopic {
		t.Errorf("topic NotFound: state = %q, want %q", w.NotifyChannels[0].Subscription, watcher.SubNoTopic)
	}
}

// TestCleanupWatchEmails_OnCancel: cancelling the last watch naming an address
// unsubscribes it; cancelling one while another active watch names it does
// not. The cancelled watch is excluded even if the eventually consistent
// status index still lists it as active.
func TestCleanupWatchEmails_OnCancel(t *testing.T) {
	ctx := context.Background()
	f, es := fakeSubs()
	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	f.Confirm("alice@example.com")

	w1 := cmdEmailWatch("w-1", watcher.StatusActive, "alice@example.com")
	w2 := cmdEmailWatch("w-2", watcher.StatusActive, "alice@example.com")
	var out, errOut bytes.Buffer

	// w1 cancelled; the index lags (w1 still listed) and w2 still uses alice.
	cleanupWatchEmails(ctx, &out, &errOut, es, func(context.Context) ([]watcher.Watch, error) {
		return []watcher.Watch{w1, w2}, nil
	}, &w1)
	if len(f.Unsubs) != 0 {
		t.Fatalf("unsubscribed %v while w-2 still names the address", f.Unsubs)
	}

	// w2 cancelled too; the lagging index still lists it.
	cleanupWatchEmails(ctx, &out, &errOut, es, func(context.Context) ([]watcher.Watch, error) {
		return []watcher.Watch{w2}, nil
	}, &w2)
	if !reflect.DeepEqual(f.Unsubs, []string{"alice@example.com"}) {
		t.Fatalf("unsubscribed %v after the last watch was cancelled, want alice", f.Unsubs)
	}
	if !strings.Contains(out.String(), "Unsubscribed alice@example.com") {
		t.Errorf("cancel output = %q", out.String())
	}

	// A list failure changes nothing and says so.
	_, es2 := fakeSubs()
	errOut.Reset()
	cleanupWatchEmails(ctx, &out, &errOut, es2, func(context.Context) ([]watcher.Watch, error) {
		return nil, errors.New("throttled")
	}, &w1)
	if !strings.Contains(errOut.String(), "notify-gc") {
		t.Errorf("list failure note = %q", errOut.String())
	}
}

// TestCleanupWatchEmails_PendingIsReported: a pending subscription cannot be
// unsubscribed; cancel says SNS deletes it.
func TestCleanupWatchEmails_PendingIsReported(t *testing.T) {
	ctx := context.Background()
	f, es := fakeSubs()
	if _, err := es.Ensure(ctx, "alice@example.com"); err != nil {
		t.Fatal(err)
	}
	w := cmdEmailWatch("w-1", watcher.StatusActive, "alice@example.com")
	var out bytes.Buffer
	cleanupWatchEmails(ctx, &out, &bytes.Buffer{}, es, func(context.Context) ([]watcher.Watch, error) { return nil, nil }, &w)
	if len(f.Unsubs) != 0 || !strings.Contains(out.String(), "within 3 days") {
		t.Errorf("unsubs = %v, output = %q; want none and the 3-day note", f.Unsubs, out.String())
	}
}

// TestNotifyGC: removes unused managed subscriptions, keeps used ones,
// changes nothing when the watch list cannot be read, and --resubscribe
// subscribes an active address with no subscription.
func TestNotifyGC(t *testing.T) {
	ctx := context.Background()
	f, es := fakeSubs()
	for _, a := range []string{"alice@example.com", "bob@example.com"} {
		if _, err := es.Ensure(ctx, a); err != nil {
			t.Fatal(err)
		}
		f.Confirm(a)
	}
	active := []watcher.Watch{cmdEmailWatch("w-a", watcher.StatusActive, "alice@example.com", "new@example.com")}

	if err := notifyGC(ctx, &bytes.Buffer{}, es, func(context.Context) ([]watcher.Watch, error) {
		return nil, errors.New("AccessDenied")
	}, true, ""); err == nil || len(f.Unsubs)+len(f.Subscribes) != 2 {
		t.Fatalf("list failure: err = %v, unsubs = %v, subscribes = %d; want an error and no change", err, f.Unsubs, len(f.Subscribes))
	}

	var out bytes.Buffer
	if err := notifyGC(ctx, &out, es, func(context.Context) ([]watcher.Watch, error) { return active, nil }, false, ""); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(f.Unsubs, []string{"bob@example.com"}) {
		t.Errorf("unsubscribed %v, want [bob@example.com]", f.Unsubs)
	}
	if len(f.Subscribes) != 2 {
		t.Errorf("notify-gc without --resubscribe subscribed something")
	}

	out.Reset()
	if err := notifyGC(ctx, &out, es, func(context.Context) ([]watcher.Watch, error) { return active, nil }, true, ""); err != nil {
		t.Fatal(err)
	}
	if len(f.Subscribes) != 3 || !strings.Contains(out.String(), "Subscribed new@example.com: pending confirmation") {
		t.Errorf("subscribes = %d, output = %q; want new@example.com subscribed", len(f.Subscribes), out.String())
	}
}

// TestParseNotifyChannels_EmailNormalizedAndValidated.
func TestParseNotifyChannels_EmailNormalizedAndValidated(t *testing.T) {
	ch, err := parseNotifyChannels([]string{"email: Alice@Example.COM "})
	if err != nil {
		t.Fatal(err)
	}
	if ch[0].Target != "alice@example.com" {
		t.Errorf("target = %q, want alice@example.com", ch[0].Target)
	}
	if _, err := parseNotifyChannels([]string{"email:not-an-address"}); err == nil {
		t.Error("an invalid address was accepted")
	}
}

// withFakeSubscriber points newEmailSubscriber at f for the test.
func withFakeSubscriber(t *testing.T, f *testutil.FakeSNS) *[]string {
	t.Helper()
	var built []string
	orig := newEmailSubscriber
	newEmailSubscriber = func(_ aws.Config, arn string) emailSubscriber {
		built = append(built, arn)
		f.TopicArn = arn
		return watcher.NewEmailSubscriptions(f, arn)
	}
	t.Cleanup(func() { newEmailSubscriber = orig })
	return &built
}

func TestEmailSubscriberFor(t *testing.T) {
	f := testutil.NewFakeSNS(testAlerts)
	built := withFakeSubscriber(t, f)
	cfg := aws.Config{Region: "us-west-2"}
	if s := emailSubscriberFor(cfg, envOf(nil), "111122223333"); s == nil || s.TopicArn() != testAlerts {
		t.Errorf("by name: %v", s)
	}
	override := "arn:aws:sns:eu-west-1:444455556666:my-alerts"
	if s := emailSubscriberFor(cfg, envOf(map[string]string{topicEnvVar: override}), ""); s == nil || s.TopicArn() != override {
		t.Errorf("env override: %v", s)
	}
	if s := emailSubscriberFor(cfg, envOf(nil), ""); s != nil {
		t.Errorf("no account: got %v, want nil", s)
	}
	if s := emailSubscriberFor(aws.Config{}, envOf(nil), "111122223333"); s != nil {
		t.Errorf("no region: got %v, want nil", s)
	}
	if len(*built) != 2 {
		t.Errorf("built %v", *built)
	}
}

// TestSubscribeForNewWatch: watch's subscribe step resolves the deployed
// topic, refuses with none, and probes nothing for a watch without email.
func TestSubscribeForNewWatch(t *testing.T) {
	ctx := context.Background()
	f := testutil.NewFakeSNS(testAlerts)
	withFakeSubscriber(t, f)
	cfg := aws.Config{Region: "us-west-2"}

	w := cmdEmailWatch("w-1", watcher.StatusActive, "alice@example.com")
	if err := subscribeForNewWatch(ctx, &bytes.Buffer{}, cfg, envOf(nil), "111122223333", &fakeProber{present: true}, &w); err != nil {
		t.Fatal(err)
	}
	if len(f.Subscribes) != 1 || w.NotifyChannels[0].Subscription != watcher.SubPending {
		t.Errorf("subscribes = %d, state = %q", len(f.Subscribes), w.NotifyChannels[0].Subscription)
	}

	w = cmdEmailWatch("w-2", watcher.StatusActive, "bob@example.com")
	err := subscribeForNewWatch(ctx, &bytes.Buffer{}, cfg, envOf(nil), "111122223333", &fakeProber{present: false}, &w)
	if err == nil || !strings.Contains(err.Error(), "lagotto deploy") {
		t.Errorf("no topic: err = %v, want a refusal naming lagotto deploy", err)
	}

	probe := &fakeProber{present: true}
	none := cmdEmailWatch("w-3", watcher.StatusActive)
	if err := subscribeForNewWatch(ctx, &bytes.Buffer{}, cfg, envOf(nil), "111122223333", probe, &none); err != nil || probe.calls != 0 {
		t.Errorf("no email channel: err = %v, probes = %d", err, probe.calls)
	}

	// A hand-made unfiltered subscription is reused, with a warning.
	f.AddSub("carol@example.com", nil, true)
	w = cmdEmailWatch("w-4", watcher.StatusActive, "carol@example.com")
	var errOut bytes.Buffer
	if err := subscribeForNewWatch(ctx, &errOut, cfg, envOf(nil), "111122223333", &fakeProber{present: true}, &w); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(errOut.String(), "NO filter policy") {
		t.Errorf("warning = %q", errOut.String())
	}
}

// TestResubscribeOnReactivate: extend re-subscribes a reactivated watch's
// address, and warns rather than fails without a topic.
func TestResubscribeOnReactivate(t *testing.T) {
	ctx := context.Background()
	f, es := fakeSubs()
	w := cmdEmailWatch("w-1", watcher.StatusExpired, "alice@example.com")
	var out, errOut bytes.Buffer
	resubscribeOnReactivate(ctx, &out, &errOut, es, &w)
	if len(f.Subscribes) != 1 || !strings.Contains(out.String(), "pending confirmation") {
		t.Errorf("subscribes = %d, out = %q", len(f.Subscribes), out.String())
	}
	errOut.Reset()
	resubscribeOnReactivate(ctx, &out, &errOut, nil, &w)
	if !strings.Contains(errOut.String(), "will not be delivered") || strings.Contains(errOut.String(), "no watch was created") {
		t.Errorf("no topic warning = %q", errOut.String())
	}
	none := cmdEmailWatch("w-2", watcher.StatusExpired)
	errOut.Reset()
	resubscribeOnReactivate(ctx, &out, &errOut, nil, &none)
	if errOut.Len() != 0 {
		t.Errorf("a watch with no email channel warned: %q", errOut.String())
	}
}

// TestNotifyGC_JSONAndErrors: the JSON form, and a failed Unsubscribe is
// returned (non-zero exit) while the rest still runs.
func TestNotifyGC_JSONAndErrors(t *testing.T) {
	ctx := context.Background()
	f, es := fakeSubs()
	if _, err := es.Ensure(ctx, "old@example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := es.Ensure(ctx, "pend@example.com"); err != nil {
		t.Fatal(err)
	}
	f.Confirm("old@example.com")
	var out bytes.Buffer
	if err := notifyGC(ctx, &out, es, func(context.Context) ([]watcher.Watch, error) { return nil, nil }, false, "json"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"topic_arn": "` + testAlerts, `"removed": [`, `"old@example.com"`, `"pending_unused": [`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("json lacks %s:\n%s", want, out.String())
		}
	}

	out.Reset()
	if err := notifyGC(ctx, &out, es, func(context.Context) ([]watcher.Watch, error) { return nil, nil }, false, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "within 3 days") {
		t.Errorf("text = %q", out.String())
	}

	f.NoTopic = true
	if err := notifyGC(ctx, &out, es, func(context.Context) ([]watcher.Watch, error) { return nil, nil }, true, ""); err == nil {
		t.Error("an SNS failure did not make notify-gc fail")
	}
}
