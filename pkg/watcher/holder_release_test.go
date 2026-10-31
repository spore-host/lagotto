package watcher

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// fakeReleaseEC2 counts calls through the Release seam.
type fakeReleaseEC2 struct {
	cancelErr   error
	describeOut *ec2.DescribeCapacityReservationsOutput
	describeErr error
	cancels     int
	describes   int
	gotRegion   string
}

func (f *fakeReleaseEC2) CancelCapacityReservation(_ context.Context, _ *ec2.CancelCapacityReservationInput, _ ...func(*ec2.Options)) (*ec2.CancelCapacityReservationOutput, error) {
	f.cancels++
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}
	return &ec2.CancelCapacityReservationOutput{Return: aws.Bool(true)}, nil
}

func (f *fakeReleaseEC2) DescribeCapacityReservations(_ context.Context, _ *ec2.DescribeCapacityReservationsInput, _ ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error) {
	f.describes++
	return f.describeOut, f.describeErr
}

func holderWith(f *fakeReleaseEC2) *Holder {
	return &Holder{releaseClient: func(region string) holderReleaseAPI { f.gotRegion = region; return f }}
}

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: code}
}

func stateOut(st ec2types.CapacityReservationState) *ec2.DescribeCapacityReservationsOutput {
	return &ec2.DescribeCapacityReservationsOutput{CapacityReservations: []ec2types.CapacityReservation{{State: st}}}
}

// TestRelease_Idempotent is #203's idempotency contract: an already-ended
// reservation is success, not an error, and a still-active one that EC2
// refuses to cancel is an error.
func TestRelease_Idempotent(t *testing.T) {
	cases := []struct {
		name          string
		f             fakeReleaseEC2
		wantReleased  bool
		wantEnded     bool
		wantErr       bool
		wantNotPermit bool
		wantDescribes int
	}{
		{name: "active, cancelled now", wantReleased: true},
		{name: "not found", f: fakeReleaseEC2{cancelErr: apiErr("InvalidCapacityReservationId.NotFound")}, wantEnded: true},
		{name: "already cancelled", f: fakeReleaseEC2{cancelErr: apiErr("IncorrectState"), describeOut: stateOut(ec2types.CapacityReservationStateCancelled)}, wantEnded: true, wantDescribes: 1},
		{name: "already expired", f: fakeReleaseEC2{cancelErr: apiErr("IncorrectState"), describeOut: stateOut(ec2types.CapacityReservationStateExpired)}, wantEnded: true, wantDescribes: 1},
		{name: "gone by describe", f: fakeReleaseEC2{cancelErr: apiErr("IncorrectState"), describeErr: apiErr("InvalidCapacityReservationId.NotFound")}, wantEnded: true, wantDescribes: 1},
		{name: "still active, cancel refused", f: fakeReleaseEC2{cancelErr: apiErr("InternalError"), describeOut: stateOut(ec2types.CapacityReservationStateActive)}, wantErr: true, wantDescribes: 1},
		{name: "access denied", f: fakeReleaseEC2{cancelErr: apiErr("UnauthorizedOperation")}, wantErr: true, wantNotPermit: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.f
			got, err := holderWith(&f).Release(context.Background(), "us-west-2", "cr-0123")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if errors.Is(err, ErrReleaseNotPermitted) != tc.wantNotPermit {
				t.Errorf("errors.Is(err, ErrReleaseNotPermitted) = %v, want %v (err %v)", !tc.wantNotPermit, tc.wantNotPermit, err)
			}
			if got.Released != tc.wantReleased || got.AlreadyEnded != tc.wantEnded {
				t.Errorf("outcome = %+v, want Released=%v AlreadyEnded=%v", got, tc.wantReleased, tc.wantEnded)
			}
			if f.cancels != 1 {
				t.Errorf("CancelCapacityReservation called %d times, want 1", f.cancels)
			}
			if f.describes != tc.wantDescribes {
				t.Errorf("DescribeCapacityReservations called %d times, want %d", f.describes, tc.wantDescribes)
			}
			if f.gotRegion != "us-west-2" {
				t.Errorf("release client region = %q, want us-west-2", f.gotRegion)
			}
		})
	}
}

func TestRelease_EmptyIDNoCalls(t *testing.T) {
	f := &fakeReleaseEC2{}
	if _, err := holderWith(f).Release(context.Background(), "us-east-1", ""); err != nil {
		t.Fatal(err)
	}
	if f.cancels+f.describes != 0 {
		t.Errorf("empty id made %d EC2 calls", f.cancels+f.describes)
	}
}
