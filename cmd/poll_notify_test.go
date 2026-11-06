package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// fakeProber is a topicProber with a canned answer.
type fakeProber struct {
	present bool
	err     error
	calls   int
}

func (f *fakeProber) AlertsTopicPresent(context.Context, string, string) (bool, error) {
	f.calls++
	return f.present, f.err
}

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// TestResolveAlertsTopic pins the #241 resolution order: the env override,
// then lagotto deploy's lagotto-capacity-alerts in the caller's account and
// region, else none with a reason.
func TestResolveAlertsTopic(t *testing.T) {
	const deployed = "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts"
	const override = "arn:aws:sns:eu-west-1:444455556666:my-alerts"
	for _, tc := range []struct {
		name       string
		env        map[string]string
		region     string
		account    string
		probe      *fakeProber
		wantARN    string
		wantProbe  int
		wantSource string
		wantWhy    string
	}{
		{name: "env override wins, no probe", env: map[string]string{topicEnvVar: " " + override + " "},
			region: "us-west-2", account: "111122223333", probe: &fakeProber{present: true},
			wantARN: override, wantSource: topicEnvVar},
		{name: "deployed topic exists", region: "us-west-2", account: "111122223333",
			probe: &fakeProber{present: true}, wantARN: deployed, wantProbe: 1, wantSource: "lagotto deploy"},
		{name: "deployed topic absent", region: "us-west-2", account: "111122223333",
			probe: &fakeProber{}, wantProbe: 1, wantWhy: "does not exist in account 111122223333, region us-west-2"},
		{name: "probe denied: use the name", region: "us-west-2", account: "111122223333",
			probe: &fakeProber{err: errors.New("AccessDenied")}, wantARN: deployed, wantProbe: 1, wantSource: "could not confirm"},
		{name: "no region", account: "111122223333", probe: &fakeProber{present: true}, wantWhy: "no AWS region"},
		{name: "no account", region: "us-west-2", probe: &fakeProber{present: true}, wantWhy: "account could not be resolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveAlertsTopic(context.Background(), envOf(tc.env), tc.region, tc.account, tc.probe)
			if got.ARN != tc.wantARN {
				t.Errorf("ARN = %q, want %q", got.ARN, tc.wantARN)
			}
			if tc.probe.calls != tc.wantProbe {
				t.Errorf("probe calls = %d, want %d", tc.probe.calls, tc.wantProbe)
			}
			if !strings.Contains(got.Source, tc.wantSource) {
				t.Errorf("Source = %q, want it to contain %q", got.Source, tc.wantSource)
			}
			if !strings.Contains(got.Why, tc.wantWhy) || (tc.wantWhy == "" && got.Why != "") {
				t.Errorf("Why = %q, want %q", got.Why, tc.wantWhy)
			}
		})
	}
}

func TestAccountFromARN(t *testing.T) {
	for in, want := range map[string]string{
		"arn:aws:iam::111122223333:user/alice":                  "111122223333",
		"arn:aws:sts::111122223333:assumed-role/r/session":      "111122223333",
		"arn:aws-us-gov:sts::999988887777:assumed-role/r/s/x:y": "999988887777",
		"":           "",
		"not-an-arn": "",
	} {
		if got := accountFromARN(in); got != want {
			t.Errorf("accountFromARN(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTopicWarnings: with no topic, a general warning, then one per in-scope
// watch that has an email channel. sns and webhook channels need no topic,
// so their watches are not named. A resolved topic warns about nothing.
func TestTopicWarnings(t *testing.T) {
	none := alertsTopic{Why: "lagotto-capacity-alerts does not exist"}
	watches := []watcher.Watch{
		{WatchID: "w-mail", Project: "p1", NotifyChannels: []watcher.NotifyChannel{{Type: "email", Target: "a@example.com"}, {Type: "webhook", Target: "https://x"}}},
		{WatchID: "w-sns", Project: "p1", NotifyChannels: []watcher.NotifyChannel{{Type: "sns", Target: "arn:aws:sns:us-east-1:1:t"}}},
		{WatchID: "w-hook", Project: "p1", NotifyChannels: []watcher.NotifyChannel{{Type: "webhook", Target: "https://x"}}},
		{WatchID: "w-other", Project: "p2", NotifyChannels: []watcher.NotifyChannel{{Type: "email", Target: "b@example.com"}}},
	}

	got := topicWarnings(none, watches, &watcher.WatchFilter{Project: "p1"})
	if len(got) != 2 {
		t.Fatalf("warnings = %d, want 2 (general + w-mail):\n%s", len(got), strings.Join(got, "\n"))
	}
	for _, want := range []string{"email notifications will NOT be delivered", "does not exist", topicEnvVar, "lagotto deploy"} {
		if !strings.Contains(got[0], want) {
			t.Errorf("general warning lacks %q: %s", want, got[0])
		}
	}
	if !strings.Contains(got[1], "w-mail") || !strings.Contains(got[1], "email:a@example.com") {
		t.Errorf("per-watch warning = %q, want it to name w-mail and its email target", got[1])
	}

	if all := topicWarnings(none, watches, nil); len(all) != 3 {
		t.Errorf("unscoped warnings = %d, want 3 (general + w-mail + w-other)", len(all))
	}
	if ok := topicWarnings(alertsTopic{ARN: "arn:aws:sns:us-east-1:1:t"}, watches, nil); len(ok) != 0 {
		t.Errorf("warnings with a topic = %v, want none", ok)
	}
}

// fakePollAWS serves the calls buildPoller makes: sts:GetCallerIdentity,
// sns:GetTopicAttributes, and the active-watch DynamoDB Query. Everything
// else is a test failure.
type fakePollAWS struct {
	t            *testing.T
	topicExists  bool
	activeWatchS string // DynamoDB JSON items for the Query
	mu           sync.Mutex
	actions      []string
}

func (f *fakePollAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		f.record(target)
		if target == "DynamoDB_20120810.Query" {
			w.Header().Set("Content-Type", "application/x-amz-json-1.0")
			_, _ = io.WriteString(w, `{"Items":[`+f.activeWatchS+`]}`)
			return
		}
		f.t.Errorf("unexpected JSON call %s", target)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	form, _ := url.ParseQuery(string(body))
	action := form.Get("Action")
	f.record(action)
	w.Header().Set("Content-Type", "text/xml")
	switch action {
	case "GetCallerIdentity":
		_, _ = io.WriteString(w, `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult>`+
			`<Arn>arn:aws:iam::111122223333:user/alice</Arn><UserId>AIDA</UserId><Account>111122223333</Account>`+
			`</GetCallerIdentityResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>`)
	case "GetTopicAttributes":
		if form.Get("TopicArn") != "arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts" {
			f.t.Errorf("probed topic %q, want lagotto deploy's in us-west-2/111122223333", form.Get("TopicArn"))
		}
		if !f.topicExists {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<ErrorResponse><Error><Type>Sender</Type><Code>NotFound</Code><Message>Topic does not exist</Message></Error><RequestId>r</RequestId></ErrorResponse>`)
			return
		}
		_, _ = io.WriteString(w, `<GetTopicAttributesResponse><GetTopicAttributesResult><Attributes><entry><key>TopicArn</key>`+
			`<value>arn:aws:sns:us-west-2:111122223333:lagotto-capacity-alerts</value></entry></Attributes></GetTopicAttributesResult>`+
			`<ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetTopicAttributesResponse>`)
	default:
		f.t.Errorf("unexpected query call %q", action)
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *fakePollAWS) record(a string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.actions = append(f.actions, a)
}

const emailWatchItem = `{"watch_id":{"S":"w-mail"},"status":{"S":"active"},` +
	`"notify_channels":{"L":[{"M":{"type":{"S":"email"},"target":{"S":"alice@example.com"}}}]}}`

// runBuildPoller runs the real buildPoller against f and returns the poller's
// notifier field and what it printed.
func runBuildPoller(t *testing.T, f *fakePollAWS) (*watcher.Notifier, string) {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{
			Region:       "us-west-2",
			Credentials:  credentials.NewStaticCredentialsProvider("fake", "fake", ""),
			BaseEndpoint: aws.String(srv.URL),
			HTTPClient:   srv.Client(),
			Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	})
	var out bytes.Buffer
	old := pollStderr
	pollStderr = &out
	t.Cleanup(func() { pollStderr = old })

	p, _, err := buildPoller(context.Background())
	if err != nil {
		t.Fatalf("buildPoller: %v", err)
	}
	n := reflect.ValueOf(p).Elem().FieldByName("notifier")
	if n.IsNil() {
		return nil, out.String()
	}
	return (*watcher.Notifier)(n.UnsafePointer()), out.String()
}

// TestBuildPoller_NotifierWithoutTopic is the #241 wiring gate. With
// LAGOTTO_SNS_TOPIC_ARN unset and no lagotto-capacity-alerts topic, the poller
// `lagotto poll` builds must still have a notifier (webhook and sns channels
// need none), and poll start must warn, naming the email watch.
func TestBuildPoller_NotifierWithoutTopic(t *testing.T) {
	t.Setenv(topicEnvVar, "")
	n, out := runBuildPoller(t, &fakePollAWS{activeWatchS: emailWatchItem})
	if n == nil {
		t.Fatal("buildPoller built no notifier without a topic: webhook and sns channels would never fire (#241)")
	}
	if n.HasAlertsTopic() {
		t.Error("HasAlertsTopic() = true, but no topic exists")
	}
	for _, want := range []string{"email notifications will NOT be delivered", "w-mail", "email:alice@example.com"} {
		if !strings.Contains(out, want) {
			t.Errorf("poll start output lacks %q:\n%s", want, out)
		}
	}
}

// TestBuildPoller_DeployedTopicFound: with no env override, buildPoller uses
// lagotto deploy's topic in the caller's account and region, and warns about
// nothing.
func TestBuildPoller_DeployedTopicFound(t *testing.T) {
	t.Setenv(topicEnvVar, "")
	f := &fakePollAWS{topicExists: true, activeWatchS: emailWatchItem}
	n, out := runBuildPoller(t, f)
	if n == nil || !n.HasAlertsTopic() {
		t.Fatalf("notifier = %v, want one with the deployed alerts topic", n)
	}
	if strings.Contains(out, "Warning") {
		t.Errorf("warned with a topic resolved:\n%s", out)
	}
	if !strings.Contains(strings.Join(f.actions, ","), "GetTopicAttributes") {
		t.Errorf("never probed the deployed topic; calls: %v", f.actions)
	}
}

// TestBuildPoller_EnvOverride: LAGOTTO_SNS_TOPIC_ARN wins and nothing is probed.
func TestBuildPoller_EnvOverride(t *testing.T) {
	t.Setenv(topicEnvVar, "arn:aws:sns:us-west-2:111122223333:custom")
	f := &fakePollAWS{activeWatchS: emailWatchItem}
	n, out := runBuildPoller(t, f)
	if n == nil || !n.HasAlertsTopic() {
		t.Fatalf("notifier = %v, want one with the override topic", n)
	}
	if strings.Contains(out, "Warning") {
		t.Errorf("warned with an override set:\n%s", out)
	}
	if strings.Contains(strings.Join(f.actions, ","), "GetTopicAttributes") {
		t.Errorf("probed a topic despite the override; calls: %v", f.actions)
	}
}
