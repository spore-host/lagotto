package deploy

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// TestPollerTags_StampsTheVersion: the Version tag is the ONLY signal that lets
// `lagotto doctor` tell a stale poller from a current one, so it has to be there,
// normalized, and absent rather than empty when there's no version to stamp.
func TestPollerTags_StampsTheVersion(t *testing.T) {
	tags := PollerTags("production", "0.60.0")
	if tags[VersionTag] != "0.60.0" {
		t.Errorf("tags[%s] = %q, want 0.60.0", VersionTag, tags[VersionTag])
	}
	// A leading "v" is stripped so the tag always holds the bare version, however
	// the caller spelled --version.
	if got := PollerTags("production", "v0.60.0")[VersionTag]; got != "0.60.0" {
		t.Errorf("tags[%s] = %q for 'v0.60.0', want 0.60.0", VersionTag, got)
	}
	// No version → no tag at all, so doctor can distinguish "deployed by a lagotto
	// that didn't stamp versions" from "stamped with nothing".
	if _, ok := PollerTags("production", "")[VersionTag]; ok {
		t.Error("an empty version must omit the tag rather than write an empty one")
	}
	// The pre-existing tags must survive.
	for k, want := range map[string]string{"Environment": "staging", "Application": "lagotto", "Component": "capacity-poller"} {
		if got := PollerTags("staging", "0.60.0")[k]; got != want {
			t.Errorf("tags[%q] = %q, want %q", k, got, want)
		}
	}
}

// TestInspectPoller_ReportsWhatIsDeployed covers the read doctor's version and
// poller-existence checks are built on.
func TestInspectPoller_ReportsWhatIsDeployed(t *testing.T) {
	l := &fakeLambda{getOut: &lambda.GetFunctionOutput{
		Configuration: liveConfig(testPollerInput()),
		Tags:          PollerTags("production", "0.60.0"),
	}}
	d, _ := testDeployer(l, &fakeSNS{}, &fakeScheduler{})

	got, err := d.InspectPoller(context.Background())
	if err != nil {
		t.Fatalf("InspectPoller: %v", err)
	}
	if !got.Exists {
		t.Fatal("Exists = false for a deployed function")
	}
	if got.Version != "0.60.0" {
		t.Errorf("Version = %q, want 0.60.0 (from the %s tag)", got.Version, VersionTag)
	}
	if got.ARN != PollerFunctionARN(testRegion, testAccount) {
		t.Errorf("ARN = %q", got.ARN)
	}
	if got.RoleARN != runtimeRoleARN(testAccount) {
		t.Errorf("RoleARN = %q", got.RoleARN)
	}
}

// TestInspectPoller_NoVersionTagIsEmptyNotAGuess: a poller deployed before the tag
// existed must report an empty version. Doctor turns that into "unknown" — a wrong
// version claim is worse than no claim.
func TestInspectPoller_NoVersionTagIsEmptyNotAGuess(t *testing.T) {
	l := &fakeLambda{getOut: &lambda.GetFunctionOutput{
		Configuration: liveConfig(testPollerInput()),
		Tags:          map[string]string{"Application": "lagotto"},
	}}
	d, _ := testDeployer(l, &fakeSNS{}, &fakeScheduler{})
	got, err := d.InspectPoller(context.Background())
	if err != nil {
		t.Fatalf("InspectPoller: %v", err)
	}
	if got.Version != "" {
		t.Errorf("Version = %q, want empty for an unstamped poller", got.Version)
	}
}

// TestInspectPoller_AbsenceVsFailure: an absent function is (Exists=false, nil), but
// an AccessDenied must be returned as an error — reporting it as "not deployed"
// would send the user off re-running `deploy` to fix a permissions problem.
func TestInspectPoller_AbsenceVsFailure(t *testing.T) {
	absent, _ := testDeployer(&fakeLambda{getErr: &lambdatypes.ResourceNotFoundException{}}, &fakeSNS{}, &fakeScheduler{})
	got, err := absent.InspectPoller(context.Background())
	if err != nil || got.Exists {
		t.Errorf("absent function → (%+v, %v), want (Exists:false, nil)", got, err)
	}

	denied := errors.New("AccessDenied")
	broken, _ := testDeployer(&fakeLambda{getErr: denied}, &fakeSNS{}, &fakeScheduler{})
	if _, err := broken.InspectPoller(context.Background()); !errors.Is(err, denied) {
		t.Errorf("err = %v, want the AccessDenied surfaced", err)
	}
}

// TestInspectPollerSchedule covers the read behind the schedule-state check — the
// one that catches "watches are armed but nothing is polling them".
func TestInspectPollerSchedule(t *testing.T) {
	sc := &fakeScheduler{getOut: &scheduler.GetScheduleOutput{
		Name:               strptr(PollerScheduleName),
		State:              schedulertypes.ScheduleStateEnabled,
		ScheduleExpression: strptr(pollerScheduleExpression),
		Target:             &schedulertypes.Target{Arn: strptr(PollerFunctionARN(testRegion, testAccount))},
	}}
	d, _ := testDeployer(&fakeLambda{}, &fakeSNS{}, sc)

	got, err := d.InspectPollerSchedule(context.Background())
	if err != nil {
		t.Fatalf("InspectPollerSchedule: %v", err)
	}
	if !got.Exists || !got.Enabled() {
		t.Errorf("got %+v, want an existing ENABLED schedule", got)
	}
	if got.Expression != pollerScheduleExpression {
		t.Errorf("Expression = %q, want %q", got.Expression, pollerScheduleExpression)
	}

	// DISABLED must not read as enabled — the whole silent-death check hinges on it.
	sc.getOut.State = schedulertypes.ScheduleStateDisabled
	got, err = d.InspectPollerSchedule(context.Background())
	if err != nil {
		t.Fatalf("InspectPollerSchedule: %v", err)
	}
	if got.Enabled() {
		t.Error("a DISABLED schedule reported as enabled")
	}

	// Absent vs unreadable, same rule as the function.
	absent, _ := testDeployer(&fakeLambda{}, &fakeSNS{}, &fakeScheduler{getErr: &schedulertypes.ResourceNotFoundException{}})
	if info, err := absent.InspectPollerSchedule(context.Background()); err != nil || info.Exists {
		t.Errorf("absent schedule → (%+v, %v), want (Exists:false, nil)", info, err)
	}
	denied := errors.New("AccessDenied")
	broken, _ := testDeployer(&fakeLambda{}, &fakeSNS{}, &fakeScheduler{getErr: denied})
	if _, err := broken.InspectPollerSchedule(context.Background()); !errors.Is(err, denied) {
		t.Errorf("err = %v, want the AccessDenied surfaced", err)
	}
}

func TestAlertsTopicPresent(t *testing.T) {
	d, _ := testDeployer(&fakeLambda{}, &fakeSNS{attributes: map[string]string{"DisplayName": alertsTopicDisplayName}}, &fakeScheduler{})
	present, err := d.AlertsTopicPresent(context.Background(), testRegion, testAccount)
	if err != nil || !present {
		t.Errorf("got (%v, %v), want (true, nil)", present, err)
	}

	missing, _ := testDeployer(&fakeLambda{}, &fakeSNS{getErr: &snstypes.NotFoundException{}}, &fakeScheduler{})
	present, err = missing.AlertsTopicPresent(context.Background(), testRegion, testAccount)
	if err != nil || present {
		t.Errorf("got (%v, %v), want (false, nil) for an absent topic", present, err)
	}
}
