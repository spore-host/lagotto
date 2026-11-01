package doctor

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// singleRegionInvokePolicy is the pre-v0.65.1 shape of the shared invoke policy:
// scoped to the function in ONE region — the region whose deploy ran last.
func singleRegionInvokePolicy(region string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:InvokeFunction",` +
		`"Resource":"` + deploy.PollerFunctionARN(region, testAccount) + `"}]}`
}

// TestEvaluate_InvokePolicyCoverage is the #218 doctor gate: the iam-roles check
// passed in an account whose us-east-1 schedule had failed 2,015 times in a week,
// because the shared invoke role's policy named only us-west-2's poller.
// invoke-policy evaluates the policy against THIS region's function ARN.
func TestEvaluate_InvokePolicyCoverage(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want Status
	}{
		// testRegion is us-west-2.
		{"scoped to another region (the #218 shape)", singleRegionInvokePolicy("us-east-1"), StatusFail},
		{"scoped to this region only", singleRegionInvokePolicy(testRegion), StatusWarn},
		{"the policy this lagotto writes", expectedInvokePolicy(t), StatusPass},
		{"the same, URL-encoded as IAM returns it", url.PathEscape(expectedInvokePolicy(t)), StatusPass},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := healthyState(t)
			st.InvokePolicyDoc = tc.doc
			r := Evaluate(st)
			c := find(t, r, CheckInvokePolicy)
			if c.Status != tc.want {
				t.Fatalf("invoke-policy = %s (%s), want %s", c.Status, c.Summary, tc.want)
			}
			if tc.want == StatusPass {
				if len(c.Fix) != 0 {
					t.Errorf("a passing check offered a fix: %v", c.Fix)
				}
				return
			}
			if len(c.Fix) == 0 || !strings.HasPrefix(c.Fix[0], "lagotto deploy --version "+testVersion) {
				t.Errorf("fix = %v, want a 'lagotto deploy --version %s' (one deploy rewrites the shared policy)", c.Fix, testVersion)
			}
			if tc.want == StatusFail {
				if !strings.Contains(c.Summary, deploy.PollerFunctionARN(testRegion, testAccount)) {
					t.Errorf("summary %q should name the function that cannot be invoked", c.Summary)
				}
				if !r.Failed() {
					t.Error("a schedule that cannot invoke its poller must fail the report")
				}
			}
		})
	}
}

func TestEvaluate_InvokePolicyAbsenceAndReadFailure(t *testing.T) {
	// Role absent: iam-roles already FAILs for it; a second line would be noise.
	st := healthyState(t)
	st.Roles[runtimeiam.SchedulerInvokeRoleName] = false
	st.InvokePolicyDoc, st.InvokePolicyErr = "", runtimeiam.ErrInvokeRoleNotFound
	absent(t, Evaluate(st), CheckInvokePolicy)

	// Role present, policy absent: Scheduler can invoke nothing.
	st = healthyState(t)
	st.InvokePolicyDoc, st.InvokePolicyErr = "", runtimeiam.ErrInvokePolicyNotFound
	if c := find(t, Evaluate(st), CheckInvokePolicy); c.Status != StatusFail {
		t.Errorf("policy absent: invoke-policy = %s, want FAIL", c.Status)
	}

	// Unreadable: we cannot tell, so WARN, never FAIL.
	st = healthyState(t)
	st.InvokePolicyDoc, st.InvokePolicyErr = "", errors.New("AccessDenied")
	if c := find(t, Evaluate(st), CheckInvokePolicy); c.Status != StatusWarn {
		t.Errorf("unreadable: invoke-policy = %s, want WARN", c.Status)
	}

	// Unparseable.
	st = healthyState(t)
	st.InvokePolicyDoc = "{not json"
	if c := find(t, Evaluate(st), CheckInvokePolicy); c.Status != StatusWarn {
		t.Errorf("unparseable: invoke-policy = %s, want WARN", c.Status)
	}
}

func TestEvaluate_SchedulerErrors(t *testing.T) {
	st := healthyState(t)
	if c := find(t, Evaluate(st), CheckSchedulerErrs); c.Status != StatusPass {
		t.Errorf("zero errors: scheduler-errors = %s, want PASS", c.Status)
	}

	st.SchedulerErrors.Count = 2015
	c := find(t, Evaluate(st), CheckSchedulerErrs)
	if c.Status != StatusWarn || !strings.Contains(c.Summary, "2015") {
		t.Errorf("errors present: scheduler-errors = %s %q, want WARN naming the count", c.Status, c.Summary)
	}
	// Per-group, not per-schedule: the detail must not let the count be read as
	// the poller's alone.
	if !strings.Contains(strings.Join(c.Detail, " "), "per schedule group") {
		t.Errorf("detail %v should say the metric is per schedule group", c.Detail)
	}

	st = healthyState(t)
	st.SchedulerErrors.Err = errors.New("AccessDenied: cloudwatch:GetMetricStatistics")
	r := Evaluate(st)
	c = find(t, r, CheckSchedulerErrs)
	if c.Status != StatusWarn || !strings.Contains(c.Summary, "best effort") {
		t.Errorf("unreadable: scheduler-errors = %s %q, want a best-effort WARN", c.Status, c.Summary)
	}
	if r.Failed() {
		t.Error("missing CloudWatch permission must not fail doctor")
	}

	// Not gathered, or no schedule to talk about → omitted.
	st = healthyState(t)
	st.SchedulerErrors = SchedulerErrors{}
	absent(t, Evaluate(st), CheckSchedulerErrs)
	st = healthyState(t)
	st.Schedule.Exists = false
	absent(t, Evaluate(st), CheckSchedulerErrs)
}

// TestGather_InvokePolicyAndSchedulerErrors drives the #218 account's shape
// through Gather against fakes: the shared role scoped to us-west-2, doctor run
// in us-east-1, and Scheduler reporting target errors.
func TestGather_InvokePolicyAndSchedulerErrors(t *testing.T) {
	const region = "us-east-1"
	f := &fakeIAM{
		t:     t,
		roles: map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true},
		policies: map[string]string{
			runtimeiam.SchedulerInvokeRoleName + "/" + runtimeiam.SchedulerInvokePolicyName: url.PathEscape(singleRegionInvokePolicy("us-west-2")),
		},
	}
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	metrics := &fakeMetrics{out: &cloudwatch.GetMetricStatisticsOutput{Datapoints: []cwtypes.Datapoint{
		{Sum: aws.Float64(12)}, {Sum: aws.Float64(276)},
	}}}
	st := Gather(context.Background(), Options{
		Region: region, AccountID: testAccount, CLIVersion: testVersion,
		IAM:     f,
		Infra:   &fakeInfra{poller: deploy.PollerInfo{Exists: true}, schedule: deploy.ScheduleInfo{Exists: true, State: "ENABLED"}, topic: true},
		Metrics: metrics,
		Now:     func() time.Time { return now },
	})
	if st.InvokePolicyErr != nil {
		t.Fatalf("InvokePolicyErr = %v", st.InvokePolicyErr)
	}
	if st.SchedulerErrors.Count != 288 || st.SchedulerErrors.Err != nil {
		t.Errorf("SchedulerErrors = %+v, want the datapoints summed to 288", st.SchedulerErrors)
	}
	if f.writes != 0 {
		t.Errorf("Gather made %d IAM write(s)", f.writes)
	}

	// The exact metric asked for.
	q := metrics.in
	if q == nil {
		t.Fatal("GetMetricStatistics was not called")
	}
	if aws.ToString(q.Namespace) != "AWS/Scheduler" || aws.ToString(q.MetricName) != "TargetErrorCount" {
		t.Errorf("queried %s/%s, want AWS/Scheduler/TargetErrorCount", aws.ToString(q.Namespace), aws.ToString(q.MetricName))
	}
	if len(q.Dimensions) != 1 || aws.ToString(q.Dimensions[0].Name) != "ScheduleGroup" || aws.ToString(q.Dimensions[0].Value) != "default" {
		t.Errorf("dimensions = %+v, want ScheduleGroup=default (the poller schedule's group)", q.Dimensions)
	}
	if len(q.Statistics) != 1 || q.Statistics[0] != cwtypes.StatisticSum {
		t.Errorf("statistics = %v, want [Sum]", q.Statistics)
	}
	if !aws.ToTime(q.EndTime).Equal(now) || !aws.ToTime(q.StartTime).Equal(now.Add(-SchedulerErrorsWindow)) {
		t.Errorf("window = %v..%v, want the %v before %v", aws.ToTime(q.StartTime), aws.ToTime(q.EndTime), SchedulerErrorsWindow, now)
	}

	r := Evaluate(st)
	if c := find(t, r, CheckInvokePolicy); c.Status != StatusFail {
		t.Errorf("invoke-policy = %s (%s), want FAIL: us-east-1's poller is not in the policy", c.Status, c.Summary)
	}
	if c := find(t, r, CheckSchedulerErrs); c.Status != StatusWarn {
		t.Errorf("scheduler-errors = %s, want WARN", c.Status)
	}

	// A metrics read failure is recorded, not fatal.
	st = Gather(context.Background(), Options{
		Region: region, AccountID: testAccount,
		Infra:   &fakeInfra{schedule: deploy.ScheduleInfo{Exists: true}},
		Metrics: &fakeMetrics{err: errors.New("AccessDenied")},
	})
	if !st.SchedulerErrors.Checked || st.SchedulerErrors.Err == nil {
		t.Errorf("SchedulerErrors = %+v, want the read error recorded", st.SchedulerErrors)
	}
}

type fakeMetrics struct {
	in  *cloudwatch.GetMetricStatisticsInput
	out *cloudwatch.GetMetricStatisticsOutput
	err error
}

func (f *fakeMetrics) GetMetricStatistics(_ context.Context, in *cloudwatch.GetMetricStatisticsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error) {
	f.in = in
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}
