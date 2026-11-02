package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// fakeAWS is a loopback AWS for the #231 gates: just enough STS, DynamoDB and
// EventBridge Scheduler for runWatch / runExtend to run end to end through the
// real SDK clients, so the errors they classify are the SDK's own parse of a
// real wire response (an AccessDeniedException is NOT a modelled Scheduler
// error type) rather than a hand-built Go value.
type fakeAWS struct {
	t *testing.T

	// scheduleState is the poller schedule's State; "" means it does not exist
	// (GetSchedule answers 404 ResourceNotFoundException).
	scheduleState string
	// getErr / updateErr, when set, make GetSchedule / UpdateSchedule fail with
	// that error code (403 for AccessDeniedException, 500 otherwise).
	getErr, updateErr string
	// item is the stored watch GetItem returns (DynamoDB JSON), for extend.
	item string

	mu        sync.Mutex
	calls     []string
	puts      []map[string]map[string]any // PutItem Items
	updates   []string                    // UpdateItem UpdateExpression + values, raw
	schedPuts []map[string]any            // UpdateSchedule bodies
}

const fakeCallerARN = "arn:aws:iam::123456789012:user/alice"

func (f *fakeAWS) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeAWS) called(call string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == call {
			n++
		}
	}
	return n
}

func schedError(w http.ResponseWriter, code string) {
	w.Header().Set("X-Amzn-ErrorType", code)
	w.Header().Set("Content-Type", "application/json")
	switch code {
	case "AccessDeniedException":
		w.WriteHeader(http.StatusForbidden)
	case "ResourceNotFoundException":
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, _ = io.WriteString(w, `{"Message":"fake `+code+`"}`)
}

func (f *fakeAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	target := r.Header.Get("X-Amz-Target")
	switch {
	case strings.HasPrefix(target, "DynamoDB_20120810."):
		op := strings.TrimPrefix(target, "DynamoDB_20120810.")
		f.record("dynamodb:" + op)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		switch op {
		case "DescribeTable":
			_, _ = io.WriteString(w, `{"Table":{"TableName":"t","TableStatus":"ACTIVE"}}`)
		case "PutItem":
			var in struct {
				Item map[string]map[string]any
			}
			if err := json.Unmarshal(body, &in); err != nil {
				f.t.Errorf("PutItem body: %v", err)
			}
			f.mu.Lock()
			f.puts = append(f.puts, in.Item)
			f.mu.Unlock()
			_, _ = io.WriteString(w, `{}`)
		case "UpdateItem":
			f.mu.Lock()
			f.updates = append(f.updates, string(body))
			f.mu.Unlock()
			_, _ = io.WriteString(w, `{}`)
		case "GetItem":
			_, _ = io.WriteString(w, `{"Item":`+f.item+`}`)
		default:
			f.t.Errorf("unexpected DynamoDB call %s", op)
			w.WriteHeader(http.StatusBadRequest)
		}
	case strings.Contains(string(body), "Action=GetCallerIdentity"):
		f.record("sts:GetCallerIdentity")
		w.Header().Set("Content-Type", "text/xml")
		_, _ = io.WriteString(w, `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Arn>`+
			fakeCallerARN+`</Arn><UserId>AIDFAKE</UserId><Account>123456789012</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata></GetCallerIdentityResponse>`)
	case strings.HasPrefix(r.URL.Path, "/schedules/"):
		name := strings.TrimPrefix(r.URL.Path, "/schedules/")
		if name != "lagotto-capacity-poller" {
			f.t.Errorf("Scheduler call for schedule %q, want the poller's", name)
		}
		switch r.Method {
		case http.MethodGet:
			f.record("scheduler:GetSchedule")
			if f.getErr != "" {
				schedError(w, f.getErr)
				return
			}
			if f.scheduleState == "" {
				schedError(w, "ResourceNotFoundException")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Name":               name,
				"Arn":                "arn:aws:scheduler:us-east-1:123456789012:schedule/default/" + name,
				"GroupName":          "default",
				"Description":        "lagotto capacity poller",
				"State":              f.scheduleState,
				"ScheduleExpression": "rate(1 minute)",
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				"Target": map[string]any{
					"Arn":     "arn:aws:lambda:us-east-1:123456789012:function:lagotto-capacity-poller",
					"RoleArn": runtimeiam.SchedulerInvokeRoleARN("123456789012"),
				},
			})
		case http.MethodPut:
			f.record("scheduler:UpdateSchedule")
			var in map[string]any
			_ = json.Unmarshal(body, &in)
			f.mu.Lock()
			f.schedPuts = append(f.schedPuts, in)
			f.mu.Unlock()
			if f.updateErr != "" {
				schedError(w, f.updateErr)
				return
			}
			if s, ok := in["State"].(string); ok {
				f.scheduleState = s
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ScheduleArn":"arn:aws:scheduler:us-east-1:123456789012:schedule/default/`+name+`"}`)
		default:
			f.t.Errorf("unexpected Scheduler %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	default:
		f.t.Errorf("unexpected request %s %s target=%q body=%.200s", r.Method, r.URL, target, body)
		w.WriteHeader(http.StatusBadRequest)
	}
}

// installFakeAWS points awscfg.Load at f. Retries are off so an injected 500
// is one request, not a backoff loop.
func installFakeAWS(t *testing.T, f *fakeAWS) {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{
			Region:       "us-east-1",
			Credentials:  credentials.NewStaticCredentialsProvider("fake", "fake", ""),
			BaseEndpoint: aws.String(srv.URL),
			HTTPClient:   srv.Client(),
			Retryer:      func() aws.Retryer { return aws.NopRetryer{} },
		}, nil
	})
}

// runWatchWithFake runs `lagotto watch g5.xlarge` (defaults: notify, 24h)
// against f and returns its error.
func runWatchWithFake(t *testing.T, f *fakeAWS) error {
	t.Helper()
	installFakeAWS(t, f)
	var out bytes.Buffer
	watchCmd.SetOut(&out)
	t.Cleanup(func() { watchCmd.SetOut(nil) })
	return runWatch(watchCmd, []string{"g5.xlarge"})
}

func putStatuses(f *fakeAWS) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var s []string
	for _, item := range f.puts {
		st, _ := item["status"]["S"].(string)
		s = append(s, st)
	}
	return s
}

// assertNamesPermissions checks the refusal names everything the caller lacks.
func assertNamesPermissions(t *testing.T, err error) {
	t.Helper()
	for _, want := range []string{
		"scheduler:GetSchedule", "scheduler:UpdateSchedule", "iam:PassRole",
		runtimeiam.SchedulerInvokeRoleName, "docs/reference/cli-iam-actions.md", "lagotto#232",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not name %q:\n%v", want, err)
		}
	}
}

// TestWatchRefusesWhenGetScheduleDenied is the #231 bug: an AccessDenied from
// GetSchedule was read as "poller not deployed", the watch was stored active,
// the self-disabled poller stayed off, and nothing was printed.
func TestWatchRefusesWhenGetScheduleDenied(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", getErr: "AccessDeniedException"}
	err := runWatchWithFake(t, f)
	if err == nil {
		t.Fatal("watch succeeded although GetSchedule was denied: the watch would never be polled (#231)")
	}
	assertNamesPermissions(t, err)
	if !strings.Contains(err.Error(), "nothing was changed") {
		t.Errorf("refusal does not say the watch was not stored:\n%v", err)
	}
	if n := f.called("dynamodb:PutItem"); n != 0 {
		t.Errorf("watch wrote %d PutItem(s) after GetSchedule was denied; no watch may be stored (statuses %v)", n, putStatuses(f))
	}
}

// TestWatchRefusesWhenGetScheduleFails: any error that is not a definite
// ResourceNotFound leaves the watch's polling unknown, so it is refused too.
func TestWatchRefusesWhenGetScheduleFails(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", getErr: "InternalServerException"}
	err := runWatchWithFake(t, f)
	if err == nil {
		t.Fatal("watch succeeded although GetSchedule failed")
	}
	if !strings.Contains(err.Error(), "InternalServerException") {
		t.Errorf("refusal hides the cause:\n%v", err)
	}
	if n := f.called("dynamodb:PutItem"); n != 0 {
		t.Errorf("watch stored a watch (%d PutItem) after GetSchedule failed", n)
	}
}

// TestWatchNotDeployedStillCreates: ResourceNotFound really does mean the
// hosted poller is not deployed (the watch is for 'lagotto poll --daemon'), and
// that keeps working exactly as before — stored, no UpdateSchedule, no error.
func TestWatchNotDeployedStillCreates(t *testing.T) {
	f := &fakeAWS{scheduleState: ""}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch failed with no poller deployed: %v", err)
	}
	if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
		t.Errorf("PutItem statuses = %v, want one active watch", got)
	}
	if n := f.called("scheduler:GetSchedule"); n != 1 {
		t.Errorf("GetSchedule called %d times, want 1", n)
	}
	if n := f.called("scheduler:UpdateSchedule"); n != 0 {
		t.Errorf("UpdateSchedule called %d times with no schedule deployed", n)
	}
}

// TestWatchEnablesDisabledSchedule: the normal path switches the poller back on,
// AFTER the watch is stored, round-tripping the schedule's other fields.
func TestWatchEnablesDisabledSchedule(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if f.scheduleState != "ENABLED" {
		t.Errorf("schedule state = %q, want ENABLED", f.scheduleState)
	}
	f.mu.Lock()
	order := strings.Join(f.calls, ",")
	f.mu.Unlock()
	if p, u := strings.Index(order, "dynamodb:PutItem"), strings.Index(order, "scheduler:UpdateSchedule"); p < 0 || u < 0 || p > u {
		t.Errorf("calls %s: the watch must be stored before the schedule is enabled", order)
	}
	if len(f.schedPuts) == 1 {
		put := f.schedPuts[0]
		if put["Description"] != "lagotto capacity poller" || put["GroupName"] != "default" {
			t.Errorf("UpdateSchedule dropped fields it must round-trip: %v", put)
		}
	}
}

// TestWatchEnabledScheduleNoUpdate: an already-enabled schedule is left alone.
func TestWatchEnabledScheduleNoUpdate(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if n := f.called("scheduler:UpdateSchedule"); n != 0 {
		t.Errorf("UpdateSchedule called %d times on an enabled schedule", n)
	}
}

// TestWatchRollsBackWhenUpdateScheduleDenied: GetSchedule allowed but
// UpdateSchedule (or the iam:PassRole it needs) denied. The watch was already
// stored, so it is rewritten as cancelled — never left active and unpolled.
func TestWatchRollsBackWhenUpdateScheduleDenied(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException"}
	err := runWatchWithFake(t, f)
	if err == nil {
		t.Fatal("watch succeeded although UpdateSchedule was denied (#231)")
	}
	assertNamesPermissions(t, err)
	if got := putStatuses(f); len(got) != 2 || got[0] != "active" || got[1] != "cancelled" {
		t.Errorf("PutItem statuses = %v, want [active cancelled]: the unpolled watch must be rolled back", got)
	}
	if !strings.Contains(err.Error(), "no watch was created") {
		t.Errorf("refusal does not say the watch was rolled back:\n%v", err)
	}
	if f.scheduleState != "DISABLED" {
		t.Errorf("schedule state = %q", f.scheduleState)
	}
}

// storedWatchItem is a minimal stored watch in DynamoDB JSON.
func storedWatchItem(status string) string {
	exp := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	return `{"watch_id":{"S":"w-1"},"user_id":{"S":"` + fakeCallerARN + `"},"status":{"S":"` + status +
		`"},"instance_type_pattern":{"S":"g5.xlarge"},"expires_at":{"S":"` + exp + `"}}`
}

func runExtendWithFake(t *testing.T, f *fakeAWS) error {
	t.Helper()
	installFakeAWS(t, f)
	var out bytes.Buffer
	extendCmd.SetOut(&out)
	t.Cleanup(func() { extendCmd.SetOut(nil) })
	return runExtend(extendCmd, []string{"w-1"})
}

func (f *fakeAWS) updateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.updates)
}

// TestExtendReactivateRefusesWhenGetScheduleDenied: the same bug through
// 'lagotto extend' on an expired watch.
func TestExtendReactivateRefusesWhenGetScheduleDenied(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", getErr: "AccessDeniedException", item: storedWatchItem("expired")}
	err := runExtendWithFake(t, f)
	if err == nil {
		t.Fatal("extend reactivated a watch although GetSchedule was denied (#231)")
	}
	assertNamesPermissions(t, err)
	if n := f.updateCount(); n != 0 {
		t.Errorf("extend wrote %d UpdateItem(s) after GetSchedule was denied", n)
	}
}

// TestExtendReactivateRollsBackWhenUpdateScheduleDenied: the reactivation is
// undone (status back to expired) when the schedule cannot be switched on.
func TestExtendReactivateRollsBackWhenUpdateScheduleDenied(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException", item: storedWatchItem("expired")}
	err := runExtendWithFake(t, f)
	if err == nil {
		t.Fatal("extend succeeded although UpdateSchedule was denied (#231)")
	}
	assertNamesPermissions(t, err)
	f.mu.Lock()
	updates := append([]string(nil), f.updates...)
	f.mu.Unlock()
	if len(updates) != 2 || !strings.Contains(updates[0], `"active"`) || !strings.Contains(updates[1], `"expired"`) {
		t.Errorf("UpdateItems = %v, want reactivate then roll back to expired", updates)
	}
}

// TestExtendReactivateNotDeployed: no poller deployed is still fine.
func TestExtendReactivateNotDeployed(t *testing.T) {
	f := &fakeAWS{item: storedWatchItem("expired")}
	if err := runExtendWithFake(t, f); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if n := f.updateCount(); n != 1 {
		t.Errorf("UpdateItem count = %d, want 1", n)
	}
}

// TestExtendActiveSkipsSchedule: extending a still-active watch does not touch
// the schedule (unchanged behaviour; it was never part of that path).
func TestExtendActiveSkipsSchedule(t *testing.T) {
	f := &fakeAWS{getErr: "AccessDeniedException", item: storedWatchItem("active")}
	if err := runExtendWithFake(t, f); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if n := f.called("scheduler:GetSchedule"); n != 0 {
		t.Errorf("GetSchedule called %d times extending an active watch", n)
	}
}

// TestArmWatchPollingRollbackFailure: if even the rollback fails the message
// says the watch is active and unpolled, and how to cancel it.
func TestArmWatchPollingRollbackFailure(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException"}
	installFakeAWS(t, f)
	cfg, _ := awscfg.Load(context.Background(), "")
	err := armWatchPolling(context.Background(), scheduler.NewFromConfig(cfg), io.Discard, "create watch w-1", "no watch was created",
		func(context.Context) error { return nil },
		func(context.Context) error { return io.ErrUnexpectedEOF })
	if err == nil || !strings.Contains(err.Error(), "lagotto cancel") || !strings.Contains(err.Error(), "will not be polled") {
		t.Errorf("rollback failure not reported: %v", err)
	}
}
