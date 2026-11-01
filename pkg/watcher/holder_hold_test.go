package watcher

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// fakeCreateEC2 records the reservations Hold asks EC2 to create.
type fakeCreateEC2 struct {
	createdAZs []string
}

func (f *fakeCreateEC2) CreateCapacityReservation(_ context.Context, in *ec2.CreateCapacityReservationInput, _ ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error) {
	f.createdAZs = append(f.createdAZs, aws.ToString(in.AvailabilityZone))
	return &ec2.CreateCapacityReservationOutput{CapacityReservation: &ec2types.CapacityReservation{
		CapacityReservationId: aws.String("cr-0fake"),
	}}, nil
}

func holderForHold(f *fakeCreateEC2, usable map[string]bool) *Holder {
	return &Holder{
		createClient:  func(string) holderCreateAPI { return f },
		launchableAZs: func(context.Context, string) map[string]bool { return usable },
	}
}

// TestHold_SkipsAZsWithoutDefaultSubnet is #209: a hold must not reserve in
// an AZ the caller can't launch into. us-west-2-lax-1b (an opted-in Local
// Zone, offered by truffle, first in the candidate order) has no default
// subnet, so the reservation goes to the first candidate that has one, and
// the match reports that AZ.
func TestHold_SkipsAZsWithoutDefaultSubnet(t *testing.T) {
	f := &fakeCreateEC2{}
	h := holderForHold(f, map[string]bool{"us-west-2a": true, "us-west-2b": true})
	m := &MatchResult{
		InstanceType:     "g7e.8xlarge",
		Region:           "us-west-2",
		AvailabilityZone: "us-west-2-lax-1b",
		CandidateAZs:     []string{"us-west-2-lax-1b", "us-west-2b", "us-west-2a"},
	}
	if err := h.Hold(context.Background(), &Watch{WatchID: "w-209"}, m); err != nil {
		t.Fatalf("Hold: %v", err)
	}
	if len(f.createdAZs) != 1 || f.createdAZs[0] != "us-west-2b" {
		t.Fatalf("reservations created in %v, want exactly one in us-west-2b", f.createdAZs)
	}
	if m.AvailabilityZone != "us-west-2b" {
		t.Errorf("m.AvailabilityZone = %q, want the reserved AZ us-west-2b", m.AvailabilityZone)
	}
	if m.ReservationID != "cr-0fake" || m.ActionTaken != "held" {
		t.Errorf("match = %+v, want held cr-0fake", m)
	}
}

// TestHold_NoUsableAZRefuses is #209's failure case: when the lookup worked
// and no candidate AZ has a default subnet, Hold creates no reservation and
// says why.
func TestHold_NoUsableAZRefuses(t *testing.T) {
	f := &fakeCreateEC2{}
	h := holderForHold(f, map[string]bool{"us-west-2a": true})
	m := &MatchResult{
		InstanceType:     "g7e.8xlarge",
		Region:           "us-west-2",
		AvailabilityZone: "us-west-2-lax-1b",
		CandidateAZs:     []string{"us-west-2-lax-1b"},
	}
	err := h.Hold(context.Background(), &Watch{WatchID: "w-209"}, m)
	if err == nil {
		t.Fatal("Hold succeeded with no usable AZ, want an error")
	}
	if !strings.Contains(err.Error(), "default subnet") || !strings.Contains(err.Error(), "us-west-2-lax-1b") {
		t.Errorf("error %q should name the AZ and the missing default subnet", err)
	}
	if len(f.createdAZs) != 0 {
		t.Errorf("reservations created in %v, want none", f.createdAZs)
	}
	if m.ActionTaken != "hold_failed" || m.ReservationID != "" {
		t.Errorf("match = %+v, want hold_failed with no reservation", m)
	}
}

// TestHold_SubnetLookupUnavailableFailsOpen: if the default-subnet set can't
// be determined, Hold keeps the old behaviour (first candidate) rather than
// blocking every hold.
func TestHold_SubnetLookupUnavailableFailsOpen(t *testing.T) {
	f := &fakeCreateEC2{}
	h := holderForHold(f, nil)
	m := &MatchResult{InstanceType: "g7e.8xlarge", Region: "us-west-2", AvailabilityZone: "us-west-2c", CandidateAZs: []string{"us-west-2c", "us-west-2a"}}
	if err := h.Hold(context.Background(), &Watch{WatchID: "w-209"}, m); err != nil {
		t.Fatalf("Hold: %v", err)
	}
	if len(f.createdAZs) != 1 || f.createdAZs[0] != "us-west-2c" {
		t.Errorf("reservations created in %v, want exactly one in us-west-2c", f.createdAZs)
	}
}

// TestNewHolder_WiresSubnetLookup: the production Holder (CLI poll and the
// hosted poller Lambda) must carry the default-subnet lookup, or the #209
// filter is skipped. Constructing it makes no AWS call.
func TestNewHolder_WiresSubnetLookup(t *testing.T) {
	h := NewHolder(aws.Config{})
	if h.launchableAZs == nil {
		t.Error("NewHolder left launchableAZs nil; holds would reserve in AZs with no default subnet")
	}
	if h.createClient == nil {
		t.Error("NewHolder left createClient nil")
	}
}
