package inventorytest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fp "github.com/spore-host/libs/footprint"

	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/testutil/hermetic"
)

func TestMain(m *testing.M) { hermetic.Main(m) }

const acct = "123456789012"

// TestSourceAnswersGather: the fake answers inventory.Gather the way the AWS
// source does, so the footprint of a Deployed region and Account classifies
// as a real one would: everything tagged and deletable, the poller's own
// machinery residue or litter, nothing active.
func TestSourceAnswersGather(t *testing.T) {
	src := New("us-east-1", "us-west-2").SetRegion(Deployed("us-east-1", acct)).
		SetAccount(Account(acct), runtimeiam.InstanceRoleName)
	f, err := inventory.Gather(context.Background(), src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
	if err != nil || len(f.Warnings) != 0 {
		t.Fatalf("gather: %v %v", err, f.Warnings)
	}
	here := f.RegionFacts("us-east-1")
	if here == nil || len(here.Tables) != 3 || here.Bucket == nil || here.Function == nil || here.Active() {
		t.Fatalf("us-east-1 facts %+v", here)
	}
	rep := inventory.Classify(f, inventory.Options{}, time.Now())
	n := 0
	for _, r := range rep.Resources {
		if r.Class == fp.ClassActive {
			t.Errorf("%s %s is active", r.Type, r.Name)
		}
		if r.Ownership != fp.OwnershipTagged {
			t.Errorf("%s %s is %s", r.Type, r.Name, r.Ownership)
		}
		n++
	}
	if n != 14 { // 9 in us-east-1, 5 account-wide
		t.Errorf("%d resources", n)
	}
	if src.Reads("us-east-1 Tables") != 1 || src.Reads("us-west-2 Tables") != 1 || src.Reads("global CLIPolicy") != 1 {
		t.Errorf("reads: %d %d %d", src.Reads("us-east-1 Tables"), src.Reads("us-west-2 Tables"), src.Reads("global CLIPolicy"))
	}
}

// TestSourceFailAndUpdate: Fail makes one read fail until cleared, and
// Update changes what a region answers between reads.
func TestSourceFailAndUpdate(t *testing.T) {
	ctx := context.Background()
	src := New("us-east-1").SetRegion(Deployed("us-east-1", acct)).SetAccount(Account(acct))
	boom := errors.New("AccessDenied")
	for _, key := range []string{"us-east-1 Tables", "us-east-1 Activity", "us-east-1 Poller", "us-east-1 Topic", "us-east-1 LogGroup",
		"us-east-1 ScheduleGroups", "us-east-1 Schedules", "us-east-1 Holds", "us-east-1 Jobs", "us-east-1 LegacyStack",
		"us-east-1 BucketTags", "global Role " + runtimeiam.RoleName, "global InstanceProfilesForRole", "global AuthorizedRoles",
		"global CLIPolicy", "global ArtifactBucketRegions"} {
		src.Fail(key, boom)
		f, err := inventory.Gather(ctx, src, acct, inventory.Scope{AllRegions: true}, inventory.Options{})
		if err != nil || len(f.Warnings) == 0 || !strings.Contains(strings.Join(f.Warnings, "\n"), "AccessDenied") {
			t.Errorf("%s failing: err %v, warnings %v", key, err, f.Warnings)
		}
		src.Fail(key, nil)
	}
	src.Fail("global EnabledRegions", boom)
	if _, err := inventory.Gather(ctx, src, acct, inventory.Scope{AllRegions: true}, inventory.Options{}); !errors.Is(err, boom) {
		t.Errorf("EnabledRegions failing: %v", err)
	}
	src.Fail("global EnabledRegions", nil)

	src.Update("us-east-1", func(r *inventory.Region) { r.ActiveWatches = 1; r.Bucket = nil })
	r := src.Region("us-east-1")
	if a, _ := r.Activity(ctx, "", ""); a.ActiveWatches != 1 {
		t.Errorf("activity %+v", a)
	}
	if _, err := r.BucketTags(ctx, "us-east-1", acct); err == nil {
		t.Error("a region with no bucket answered its tags")
	}
	if p, err := src.Account().InstanceProfile(ctx, "absent"); p != nil || err != nil {
		t.Errorf("absent profile %+v %v", p, err)
	}
	src.SetAccount(inventory.Account{AuthorizedProfiles: []inventory.Profile{{Item: inventory.Item{Name: "team-gpu"}}}})
	if p, err := src.Account().InstanceProfile(ctx, "team-gpu"); p == nil || err != nil {
		t.Errorf("authorized profile %+v %v", p, err)
	}
}
