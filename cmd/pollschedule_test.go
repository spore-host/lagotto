package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"
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
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
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
	// scheduleExpr is its ScheduleExpression; "" means rate(5 minutes), the
	// normal cadence (#323).
	scheduleExpr string
	// bumpErr, when set, makes the poller-epoch UpdateItem (#323) fail with
	// that error code.
	bumpErr string
	// getErr / updateErr, when set, make GetSchedule / UpdateSchedule fail with
	// that error code (403 for AccessDeniedException, 500 otherwise).
	getErr, updateErr string
	// item is the stored watch GetItem returns (DynamoDB JSON), for extend.
	item string
	// conditionFails makes every UpdateItem fail its condition (#257).
	conditionFails bool
	// pollerVersion is the deployed poller Lambda's Version tag (#239 phase
	// 4); "" means no poller is deployed (GetFunction answers 404), and
	// pollerUntagged means one is deployed with no Version tag.
	pollerVersion string
	// tablesAbsent makes lagotto's tables not exist (DescribeTable answers
	// ResourceNotFoundException) until a CreateTable creates one (#325).
	tablesAbsent bool

	mu        sync.Mutex
	created   map[string]bool // tables CreateTable made
	calls     []string
	puts      []map[string]map[string]any // PutItem Items
	updates   []string                    // UpdateItem UpdateExpression + values, raw (not the epoch's)
	bumps     int                         // poller-epoch UpdateItems (#323)
	schedPuts []map[string]any            // UpdateSchedule bodies

	// leases is the spore-host-lifecycle table the commands mark lagotto
	// active in (lagotto#326); nil means a fresh one per installFakeAWS.
	leases *lifecycletest.Fake
	// leaseOpts adjust the commands' lifecycle client (a clock, say).
	leaseOpts []func(*lifecycle.Options)
	// region is the config's region; "" is us-east-1.
	region string
	// requests counts every request the fake served, of any service.
	requests int
}

// leaseTable is f's lifecycle table, made on first use.
func (f *fakeAWS) leaseTable() *lifecycletest.Fake {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leases == nil {
		f.leases = lifecycletest.New()
	}
	return f.leases
}

const fakeCallerARN = "arn:aws:iam::123456789012:user/alice"

// pollerUntagged is the fakeAWS.pollerVersion of a poller deployed with no
// Version tag.
const pollerUntagged = "untagged"

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
	f.mu.Lock()
	f.requests++
	f.mu.Unlock()
	body, _ := io.ReadAll(r.Body)
	target := r.Header.Get("X-Amz-Target")
	switch {
	case strings.HasPrefix(target, "DynamoDB_20120810."):
		op := strings.TrimPrefix(target, "DynamoDB_20120810.")
		if op != "UpdateItem" || !strings.Contains(string(body), watcher.PollerControlKey) {
			// The poller-epoch bump is recorded on its own, below, so the
			// UpdateItem counts stay the watch's.
			f.record("dynamodb:" + op)
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		var named struct{ TableName string }
		_ = json.Unmarshal(body, &named)
		switch op {
		case "DescribeTable":
			f.mu.Lock()
			absent := f.tablesAbsent && !f.created[named.TableName]
			f.mu.Unlock()
			if absent {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"Requested resource not found"}`)
				return
			}
			_, _ = io.WriteString(w, `{"Table":{"TableName":"t","TableStatus":"ACTIVE"}}`)
		case "CreateTable":
			f.mu.Lock()
			if f.created == nil {
				f.created = map[string]bool{}
			}
			f.created[named.TableName] = true
			f.mu.Unlock()
			_, _ = io.WriteString(w, `{"TableDescription":{"TableName":"`+named.TableName+`","TableStatus":"ACTIVE"}}`)
		case "DescribeTimeToLive":
			_, _ = io.WriteString(w, `{"TimeToLiveDescription":{"TimeToLiveStatus":"DISABLED"}}`)
		case "UpdateTimeToLive":
			_, _ = io.WriteString(w, `{"TimeToLiveSpecification":{"AttributeName":"ttl_timestamp","Enabled":true}}`)
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
			if strings.Contains(string(body), watcher.PollerControlKey) {
				f.record("dynamodb:UpdateItem(poller-epoch)")
				if f.bumpErr != "" {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#`+f.bumpErr+`","message":"fake"}`)
					return
				}
				f.mu.Lock()
				f.bumps++
				f.mu.Unlock()
				_, _ = io.WriteString(w, `{}`)
				return
			}
			f.mu.Lock()
			f.updates = append(f.updates, string(body))
			f.mu.Unlock()
			if f.conditionFails {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}`)
				return
			}
			_, _ = io.WriteString(w, `{}`)
		case "Query":
			_, _ = io.WriteString(w, `{"Items":[],"Count":0}`)
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
			expr := f.scheduleExpr
			if expr == "" {
				expr = "rate(5 minutes)"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Name":               name,
				"Arn":                "arn:aws:scheduler:us-east-1:123456789012:schedule/default/" + name,
				"GroupName":          "default",
				"Description":        "lagotto capacity poller",
				"State":              f.scheduleState,
				"ScheduleExpression": expr,
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				"Target": map[string]any{
					"Arn":     "arn:aws:lambda:us-east-1:123456789012:function:lagotto-capacity-poller",
					"RoleArn": runtimeiam.SchedulerInvokeRoleARN("aws", "123456789012"),
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
			if e, ok := in["ScheduleExpression"].(string); ok {
				f.scheduleExpr = e
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ScheduleArn":"arn:aws:scheduler:us-east-1:123456789012:schedule/default/`+name+`"}`)
		default:
			f.t.Errorf("unexpected Scheduler %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	case r.Method == http.MethodGet && r.URL.Path == "/2015-03-31/functions/lagotto-capacity-poller":
		f.record("lambda:GetFunction")
		if f.pollerVersion == "" {
			schedError(w, "ResourceNotFoundException")
			return
		}
		tags := map[string]string{}
		if f.pollerVersion != pollerUntagged {
			tags["Version"] = f.pollerVersion
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Configuration": map[string]any{"FunctionName": "lagotto-capacity-poller"},
			"Tags":          tags,
		})
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
	installFakeLeases(t, f.leaseTable(), f.leaseOpts...)
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	region := f.region
	if region == "" {
		region = "us-east-1"
	}
	awscfg.SetLoaderForTest(t, func(context.Context, ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{
			Region:       region,
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
	// The fake serves no EC2: give the #256 subnet check a stub that can't
	// resolve regions, which it treats as a warning, never a refusal.
	prev := newSubnetCoverageChecker
	newSubnetCoverageChecker = func(aws.Config) *watcher.SubnetCoverageChecker {
		return watcher.NewSubnetCoverageCheckerWith(
			func(string) watcher.SubnetCoverageEC2 { return subnetEC2{} },
			func(context.Context) ([]string, error) { return nil, errors.New("no EC2 in the fake") })
	}
	t.Cleanup(func() { newSubnetCoverageChecker = prev })
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
// with --local that keeps working as before: stored, no UpdateSchedule, no
// error.
func TestWatchNotDeployedStillCreates(t *testing.T) {
	// With --local since #325: without it, no poller is a refusal
	// (TestWatchRefusesWithNoPoller).
	setFlagsForTest(t, watchCmd, map[string]string{"local": "true"})
	f := &fakeAWS{scheduleState: ""}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch failed with no poller deployed: %v", err)
	}
	if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
		t.Errorf("PutItem statuses = %v, want one active watch", got)
	}
	// Once in lagotto's schedule group, once in the default one (#324).
	if n := f.called("scheduler:GetSchedule"); n != 2 {
		t.Errorf("GetSchedule called %d times, want 2", n)
	}
	if n := f.called("scheduler:UpdateSchedule"); n != 0 {
		t.Errorf("UpdateSchedule called %d times with no schedule deployed", n)
	}
}

// TestWatchEnablesDisabledSchedule: a schedule a poller older than #323
// disabled is switched back on at the normal cadence, AFTER the watch is
// stored, round-tripping the schedule's other fields.
func TestWatchEnablesDisabledSchedule(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if f.scheduleState != "ENABLED" {
		t.Errorf("schedule state = %q, want ENABLED", f.scheduleState)
	}
	if f.scheduleExpr != "rate(5 minutes)" {
		t.Errorf("schedule expression = %q, want rate(5 minutes)", f.scheduleExpr)
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

// TestWatchRestoresIdleCadence (#323): an idle poller slowed the schedule to
// hourly; arming a watch bumps the poller epoch AFTER storing the watch, then
// re-reads the schedule and sets it back to every 5 minutes, ENABLED,
// round-tripping the rest.
func TestWatchRestoresIdleCadence(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", scheduleExpr: "rate(1 hour)"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if f.scheduleState != "ENABLED" || f.scheduleExpr != "rate(5 minutes)" {
		t.Errorf("schedule = %s %s, want ENABLED rate(5 minutes)", f.scheduleState, f.scheduleExpr)
	}
	if f.bumps != 1 {
		t.Errorf("poller epoch bumped %d times, want 1", f.bumps)
	}
	f.mu.Lock()
	order := strings.Join(f.calls, ",")
	f.mu.Unlock()
	put := strings.Index(order, "dynamodb:PutItem")
	bump := strings.Index(order, "dynamodb:UpdateItem(poller-epoch)")
	reread := strings.LastIndex(order, "scheduler:GetSchedule")
	update := strings.Index(order, "scheduler:UpdateSchedule")
	if put < 0 || bump < put || reread < bump || update < reread {
		t.Errorf("calls %s: want store, then bump the epoch, then re-read and update the schedule", order)
	}
	if n := f.called("scheduler:GetSchedule"); n != 2 {
		t.Errorf("GetSchedule called %d times, want 2 (preflight, and the re-read after the bump)", n)
	}
	if len(f.schedPuts) == 1 && f.schedPuts[0]["Description"] != "lagotto capacity poller" {
		t.Errorf("UpdateSchedule dropped the Description: %v", f.schedPuts[0])
	}
}

// TestWatchEpochDeniedStillArms: a lagotto-cli-user policy written before
// #323 cannot bump the epoch. That is a warning, not a refusal: the schedule
// is still set to every 5 minutes and the watch kept.
func TestWatchEpochDeniedStillArms(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", scheduleExpr: "rate(1 hour)", bumpErr: "AccessDeniedException"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch refused over the poller epoch: %v", err)
	}
	if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
		t.Errorf("PutItem statuses = %v, want one active watch", got)
	}
	if f.scheduleExpr != "rate(5 minutes)" {
		t.Errorf("schedule expression = %s, want rate(5 minutes)", f.scheduleExpr)
	}
}

// TestWatchNotDeployedNoEpoch: with no hosted poller there is no one to tell,
// so no control item is written.
func TestWatchNotDeployedNoEpoch(t *testing.T) {
	setFlagsForTest(t, watchCmd, map[string]string{"local": "true"})
	f := &fakeAWS{scheduleState: ""}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if n := f.called("dynamodb:UpdateItem(poller-epoch)"); n != 0 {
		t.Errorf("poller epoch written %d times with no poller deployed", n)
	}
}

// TestWatchEnabledScheduleNoUpdate: a schedule already ENABLED at the normal
// cadence is left alone (idempotent), though the epoch is still bumped.
func TestWatchEnabledScheduleNoUpdate(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED"}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if n := f.called("scheduler:UpdateSchedule"); n != 0 {
		t.Errorf("UpdateSchedule called %d times on an enabled schedule", n)
	}
	if f.bumps != 1 {
		t.Errorf("poller epoch bumped %d times, want 1", f.bumps)
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

// TestPollScheduleUpdateRefusalRollbackFailure: if even the rollback fails the
// message says the watch is active and unpolled, and how to cancel it.
func TestPollScheduleUpdateRefusalRollbackFailure(t *testing.T) {
	err := pollScheduleUpdateRefusal("create watch w-1", "no watch was created", io.ErrUnexpectedEOF, io.EOF)
	if err == nil || !strings.Contains(err.Error(), "lagotto cancel") || !strings.Contains(err.Error(), "will not be polled") {
		t.Errorf("rollback failure not reported: %v", err)
	}
}
