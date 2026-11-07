package arm

import (
	"context"
	"errors"
	"strings"
	"testing"

	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

const testTopic = "arn:aws:sns:us-west-2:123456789012:lagotto-capacity-alerts"

var (
	_ TopicProber     = (*deploy.Deployer)(nil)
	_ EmailSubscriber = (*watcher.EmailSubscriptions)(nil)
	_ EmailReconciler = (*watcher.EmailSubscriptions)(nil)
)

type fakeProber struct {
	present bool
	err     error
}

func (p fakeProber) AlertsTopicPresent(context.Context, string, string) (bool, error) {
	return p.present, p.err
}

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveAlertsTopic(t *testing.T) {
	for name, tc := range map[string]struct {
		env             map[string]string
		region, account string
		probe           fakeProber
		wantARN         string
		wantSource      string
		wantWhy         string
	}{
		"env override wins, trimmed": {
			env: map[string]string{TopicEnvVar: "  arn:aws:sns:eu-west-1:9:mine  "}, region: "us-west-2", account: "1",
			wantARN: "arn:aws:sns:eu-west-1:9:mine", wantSource: TopicEnvVar,
		},
		"no region": {wantWhy: "no AWS region is set"},
		"no account": {
			region: "us-west-2", wantWhy: "your AWS account could not be resolved",
		},
		"present": {
			region: "us-west-2", account: "123456789012", probe: fakeProber{present: true},
			wantARN: testTopic, wantSource: "created by `lagotto deploy`",
		},
		"absent": {
			region: "us-west-2", account: "123456789012",
			wantWhy: "lagotto-capacity-alerts does not exist in account 123456789012, region us-west-2",
		},
		"probe failed: used by name": {
			region: "us-west-2", account: "123456789012", probe: fakeProber{err: errors.New("AccessDenied")},
			wantARN: testTopic, wantSource: "could not confirm it exists: AccessDenied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := ResolveAlertsTopic(context.Background(), envOf(tc.env), tc.region, tc.account, tc.probe)
			if got.ARN != tc.wantARN {
				t.Errorf("ARN = %q, want %q", got.ARN, tc.wantARN)
			}
			if !strings.Contains(got.Source, tc.wantSource) || (tc.wantSource == "" && got.Source != "") {
				t.Errorf("Source = %q, want it to contain %q", got.Source, tc.wantSource)
			}
			if !strings.Contains(got.Why, tc.wantWhy) || (tc.wantWhy == "" && got.Why != "") {
				t.Errorf("Why = %q, want it to contain %q", got.Why, tc.wantWhy)
			}
		})
	}
}

// fakeSubs is an in-process EmailSubscriber and EmailReconciler.
type fakeSubs struct {
	topic      string
	ensure     map[string]watcher.EnsureResult
	ensureErr  map[string]error
	ensured    []string
	reconcile  watcher.ReconcileResult
	recErr     error
	recActive  []watcher.Watch
	recOnly    []string
	reconciled int
}

func (f *fakeSubs) TopicArn() string { return f.topic }

func (f *fakeSubs) Ensure(_ context.Context, addr string) (watcher.EnsureResult, error) {
	f.ensured = append(f.ensured, addr)
	if err := f.ensureErr[addr]; err != nil {
		return watcher.EnsureResult{}, err
	}
	return f.ensure[addr], nil
}

func (f *fakeSubs) Reconcile(_ context.Context, active []watcher.Watch, only []string) (watcher.ReconcileResult, error) {
	f.reconciled++
	f.recActive, f.recOnly = active, only
	return f.reconcile, f.recErr
}

func emailWatch(id string, targets ...string) *watcher.Watch {
	w := &watcher.Watch{WatchID: id, Status: watcher.StatusActive}
	for _, tg := range targets {
		typ := "email"
		if strings.HasPrefix(tg, "https://") {
			typ = "webhook"
		}
		w.NotifyChannels = append(w.NotifyChannels, watcher.NotifyChannel{Type: typ, Target: tg})
	}
	return w
}

func TestSubscribeWatchEmails(t *testing.T) {
	ctx := context.Background()

	// No email channel: nothing to subscribe, whatever the topic.
	if warns, err := SubscribeWatchEmails(ctx, AlertsTopic{}, nil, emailWatch("w-1", "https://hook")); err != nil || warns != nil {
		t.Errorf("no email channel: got (%v, %v)", warns, err)
	}

	// No topic: refused, and the refusal says why and that no watch was made.
	_, err := SubscribeWatchEmails(ctx, AlertsTopic{Why: "lagotto-capacity-alerts does not exist"}, nil, emailWatch("w-1", "a@example.com"))
	if err == nil {
		t.Fatal("no topic: not refused")
	}
	for _, want := range []string{"lagotto-capacity-alerts does not exist", TopicEnvVar, "no watch was created"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("no-topic refusal lacks %q: %v", want, err)
		}
	}

	// Subscribed: each channel gets its state; an unfiltered pre-existing
	// subscription is a warning, not a refusal.
	f := &fakeSubs{topic: testTopic, ensure: map[string]watcher.EnsureResult{
		"a@example.com": {Address: "a@example.com", State: watcher.SubPending, Created: true},
		"b@example.com": {Address: "b@example.com", State: watcher.SubConfirmed, Unfiltered: true},
	}}
	w := emailWatch("w-1", "A@Example.com", "https://hook", "b@example.com")
	warns, err := SubscribeWatchEmails(ctx, AlertsTopic{ARN: testTopic}, f, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 1 || !strings.HasPrefix(warns[0], "b@example.com already has a subscription on "+testTopic+" with NO filter policy") {
		t.Errorf("warnings = %q", warns)
	}
	if w.NotifyChannels[0].Subscription != watcher.SubPending || w.NotifyChannels[2].Subscription != watcher.SubConfirmed ||
		w.NotifyChannels[1].Subscription != "" {
		t.Errorf("channel states = %+v", w.NotifyChannels)
	}

	// A failing Ensure refuses, and keeps the warnings gathered before it.
	f = &fakeSubs{topic: testTopic,
		ensure:    map[string]watcher.EnsureResult{"a@example.com": {State: watcher.SubConfirmed, Unfiltered: true}},
		ensureErr: map[string]error{"b@example.com": errors.New("throttled")},
	}
	warns, err = SubscribeWatchEmails(ctx, AlertsTopic{ARN: testTopic}, f, emailWatch("w-1", "a@example.com", "b@example.com"))
	if err == nil || err.Error() != "subscribe b@example.com to email notifications: throttled; no watch was created" {
		t.Errorf("Ensure failure = %v", err)
	}
	if len(warns) != 1 {
		t.Errorf("warnings before the failure = %q, want 1", warns)
	}
}

func TestSetChannelStates(t *testing.T) {
	w := emailWatch("w-1", "a@example.com", "https://hook", "b@example.com")
	SetChannelStates(w, map[string]watcher.SubscriptionState{"a@example.com": watcher.SubConfirmed}, "")
	if w.NotifyChannels[0].Subscription != watcher.SubConfirmed || w.NotifyChannels[2].Subscription != watcher.SubNone || w.NotifyChannels[1].Subscription != "" {
		t.Errorf("states = %+v", w.NotifyChannels)
	}
	SetChannelStates(w, nil, watcher.SubNoTopic)
	if w.NotifyChannels[0].Subscription != watcher.SubNoTopic || w.NotifyChannels[2].Subscription != watcher.SubNoTopic || w.NotifyChannels[1].Subscription != "" {
		t.Errorf("fallback states = %+v", w.NotifyChannels)
	}
}

func TestCleanupWatchEmails(t *testing.T) {
	ctx := context.Background()
	w := emailWatch("w-1", "a@example.com")
	listed := func(ws ...watcher.Watch) func(context.Context) ([]watcher.Watch, error) {
		return func(context.Context) ([]watcher.Watch, error) { return ws, nil }
	}

	// The cancelled watch itself is excluded (the status index may lag), and
	// only its addresses are reconciled.
	other := *emailWatch("w-2", "b@example.com")
	f := &fakeSubs{topic: testTopic, reconcile: watcher.ReconcileResult{Removed: []string{"a@example.com"}}}
	res, err := CleanupWatchEmails(ctx, f, listed(*w, other), w)
	if err != nil || len(res.Removed) != 1 {
		t.Fatalf("got (%+v, %v)", res, err)
	}
	if len(f.recActive) != 1 || f.recActive[0].WatchID != "w-2" {
		t.Errorf("Reconcile active = %+v, want only w-2", f.recActive)
	}
	if len(f.recOnly) != 1 || f.recOnly[0] != "a@example.com" {
		t.Errorf("Reconcile only = %v", f.recOnly)
	}

	// No-ops: no email channel, no subscriber, no topic.
	for name, tc := range map[string]struct {
		subs EmailReconciler
		w    *watcher.Watch
	}{
		"no email": {&fakeSubs{topic: testTopic}, emailWatch("w-1", "https://hook")},
		"nil subs": {nil, w},
		"no topic": {&fakeSubs{}, w},
	} {
		res, err := CleanupWatchEmails(ctx, tc.subs, func(context.Context) ([]watcher.Watch, error) {
			t.Errorf("%s: listed the active watches", name)
			return nil, nil
		}, tc.w)
		if err != nil || len(res.Removed)+len(res.PendingUnused)+len(res.InUse) != 0 {
			t.Errorf("%s: got (%+v, %v)", name, res, err)
		}
	}

	// A failed listing is a *ListActiveError and nothing is reconciled.
	boom := errors.New("dynamodb down")
	f = &fakeSubs{topic: testTopic}
	_, err = CleanupWatchEmails(ctx, f, func(context.Context) ([]watcher.Watch, error) { return nil, boom }, w)
	var le *ListActiveError
	if !errors.As(err, &le) || !errors.Is(err, boom) || f.reconciled != 0 {
		t.Errorf("list failure: err = %v, reconciled = %d", err, f.reconciled)
	}

	// A missing topic is nothing to clean up; any other Reconcile error is returned.
	for _, nf := range []error{&snstypes.NotFoundException{}, &smithy.GenericAPIError{Code: "NotFound"}} {
		f = &fakeSubs{topic: testTopic, recErr: nf}
		if _, err := CleanupWatchEmails(ctx, f, listed(), w); err != nil {
			t.Errorf("topic not found (%T) returned %v", nf, err)
		}
	}
	f = &fakeSubs{topic: testTopic, recErr: errors.New("throttled"), reconcile: watcher.ReconcileResult{Removed: []string{"x"}}}
	res, err = CleanupWatchEmails(ctx, f, listed(), w)
	if err == nil || len(res.Removed) != 1 {
		t.Errorf("Reconcile failure: got (%+v, %v), want the partial result and the error", res, err)
	}
}
