package schedgroup

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/smithy-go"
)

var errDenied = &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "denied"}

// fake answers per group: a schedule, or an error.
type fake struct {
	get     map[string]error // group -> error (nil = found)
	create  map[string]error
	created []string // groups CreateSchedule was called with
}

func (f *fake) GetSchedule(_ context.Context, in *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	g := aws.ToString(in.GroupName)
	err, ok := f.get[g]
	if !ok {
		return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("no " + g)}
	}
	if err != nil {
		return nil, err
	}
	return &scheduler.GetScheduleOutput{Name: in.Name}, nil
}

func (f *fake) CreateSchedule(_ context.Context, in *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	g := aws.ToString(in.GroupName)
	f.created = append(f.created, g)
	return &scheduler.CreateScheduleOutput{}, f.create[g]
}

func TestFindPrefersLagottoGroupAndSetsGroupName(t *testing.T) {
	f := &fake{get: map[string]error{Name: nil, Default: nil}}
	out, err := Find(context.Background(), f, "s")
	if err != nil || aws.ToString(out.GroupName) != Name {
		t.Fatalf("Find = %v, %v; want group %s", out, err, Name)
	}
}

func TestFindFallsBackToDefault(t *testing.T) {
	for name, lagottoErr := range map[string]error{
		"absent": &schedulertypes.ResourceNotFoundException{},
		"denied": errDenied,
	} {
		t.Run(name, func(t *testing.T) {
			get := map[string]error{Default: nil}
			if name == "denied" {
				get[Name] = lagottoErr
			}
			out, err := Find(context.Background(), &fake{get: get}, "s")
			if err != nil || aws.ToString(out.GroupName) != Default {
				t.Fatalf("Find = %v, %v; want the default group's schedule", out, err)
			}
		})
	}
}

func TestFindNotFoundInBoth(t *testing.T) {
	_, err := Find(context.Background(), &fake{}, "s")
	if !IsNotFound(err) {
		t.Fatalf("Find = %v, want ResourceNotFoundException", err)
	}
}

func TestFindDeniedAndAbsentIsTheDenial(t *testing.T) {
	// lagotto's group cannot be read and default does not have it: whether the
	// schedule exists is unknown, which must not read as "not deployed".
	_, err := Find(context.Background(), &fake{get: map[string]error{Name: errDenied}}, "s")
	if !IsAccessDenied(err) {
		t.Fatalf("Find = %v, want the AccessDenied", err)
	}
}

func TestCreate(t *testing.T) {
	in := &scheduler.CreateScheduleInput{Name: aws.String("lagotto-launch-sl-1")}
	cases := []struct {
		name      string
		create    map[string]error
		wantGroup string
		wantCalls []string
		wantErr   bool
	}{
		{"in lagotto's group", nil, Name, []string{Name}, false},
		{"group absent", map[string]error{Name: &schedulertypes.ResourceNotFoundException{}}, Default, []string{Name, Default}, false},
		{"old policy", map[string]error{Name: errDenied}, Default, []string{Name, Default}, false},
		{"conflict is not a fallback", map[string]error{Name: &schedulertypes.ConflictException{}}, "", []string{Name}, true},
		{"both refuse", map[string]error{Name: errDenied, Default: errors.New("boom")}, "", []string{Name, Default}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fake{create: c.create}
			g, err := Create(context.Background(), f, in)
			if (err != nil) != c.wantErr || g != c.wantGroup {
				t.Fatalf("Create = %q, %v; want %q (err %v)", g, err, c.wantGroup, c.wantErr)
			}
			if len(f.created) != len(c.wantCalls) {
				t.Fatalf("CreateSchedule groups = %v, want %v", f.created, c.wantCalls)
			}
			for i := range f.created {
				if f.created[i] != c.wantCalls[i] {
					t.Fatalf("CreateSchedule groups = %v, want %v", f.created, c.wantCalls)
				}
			}
		})
	}
	if in.GroupName != nil {
		t.Error("Create modified its input")
	}
}

// TestARNInEachPartition: lagotto's group's ARN is in its region's partition
// (lagotto#347).
func TestARNInEachPartition(t *testing.T) {
	for region, want := range map[string]string{
		"us-east-1":      "arn:aws:scheduler:us-east-1:123456789012:schedule-group/lagotto",
		"us-gov-west-1":  "arn:aws-us-gov:scheduler:us-gov-west-1:123456789012:schedule-group/lagotto",
		"cn-northwest-1": "arn:aws-cn:scheduler:cn-northwest-1:123456789012:schedule-group/lagotto",
	} {
		if got := ARN(region, "123456789012"); got != want {
			t.Errorf("ARN(%s) = %q, want %q", region, got, want)
		}
	}
}
