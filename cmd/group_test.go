package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

func TestValidateGroupFlags(t *testing.T) {
	spawn := func() *watcher.Watch { return &watcher.Watch{Action: watcher.ActionSpawn} }
	if p, err := validateGroupFlags("", "", false, spawn()); err != nil || p != "" {
		t.Errorf("no group: %q %v", p, err)
	}
	if _, err := validateGroupFlags("", "keep", true, spawn()); err == nil {
		t.Error("--group-on-extra without --group accepted")
	}
	if p, err := validateGroupFlags("gpu", "", false, spawn()); err != nil || p != watcher.GroupOnExtraTerminate {
		t.Errorf("default policy: %q %v", p, err)
	}
	if p, err := validateGroupFlags("gpu", "keep", true, spawn()); err != nil || p != watcher.GroupOnExtraKeep {
		t.Errorf("keep: %q %v", p, err)
	}
	for name, w := range map[string]*watcher.Watch{
		"notify":       {Action: watcher.ActionNotify},
		"until-landed": {Action: watcher.ActionHold, UntilLanded: true},
		"fleet":        {Action: watcher.ActionSpawn, DesiredCount: 3},
	} {
		if _, err := validateGroupFlags("gpu", "", false, w); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := validateGroupFlags("gpu", "hold", true, spawn()); err == nil || !strings.Contains(err.Error(), "not supported yet") {
		t.Errorf("hold policy: %v", err)
	}
}

func TestIsGroupArg(t *testing.T) {
	for arg, want := range map[string]bool{"w-1234abcd": false, "sl-1234": false, "gpu-hunt": true, "g-gpu": true} {
		if got := isGroupArg(arg); got != want {
			t.Errorf("isGroupArg(%q) = %v", arg, got)
		}
	}
	if groupName("g-gpu") != "gpu" || groupName("gpu") != "gpu" {
		t.Error("groupName")
	}
}

func TestWriteGroupStatus(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	ended := now.Add(-time.Hour)
	g := &watcher.WatchGroup{Name: "gpu", State: watcher.GroupLanded, Owner: "arn:alice", CreatedAt: now.Add(-48 * time.Hour),
		WinnerWatch: "w-a", WinnerInstanceID: "i-1", WinnerType: "g7e.2xlarge", WinnerAZ: "us-east-2b", EndedAt: &ended}
	members := []*watcher.Watch{
		{WatchID: "w-a", Status: watcher.StatusMatched, Action: watcher.ActionSpawn, InstanceTypePattern: "g7e.2xlarge", Regions: []string{"us-east-2"}, Attempts: 3},
		{WatchID: "w-b", Status: watcher.StatusCancelled, Action: watcher.ActionHold, InstanceTypePattern: "g6e.xlarge", Regions: []string{"us-west-2"},
			GroupOnExtra: watcher.GroupOnExtraKeep, CancelledReason: "group gpu landed by w-a (i-1, g7e.2xlarge, us-east-2b)"},
	}
	var out bytes.Buffer
	writeGroupStatus(&out, groupStatus{Group: g, Members: members}, now)
	got := out.String()
	for _, want := range []string{
		"Group:    gpu\n",
		"State:    landed by w-a (i-1, g7e.2xlarge, us-east-2b) at 2026-11-08T11:00:00Z\n",
		"Members (2):",
		"w-a          matched    spawn  g7e.2xlarge          [us-east-2]               terminate     3 -         winner",
		"w-b          cancelled  hold   g6e.xlarge           [us-west-2]               keep          0 -         group gpu landed by w-a",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status lacks %q:\n%s", want, got)
		}
	}

	hunting := &watcher.WatchGroup{Name: "gpu", State: watcher.GroupHunting, SlotWatch: "w-a", SlotExpiresAt: "2026-11-08T12:40:00Z"}
	members[0].Status = watcher.StatusActive
	out.Reset()
	writeGroupStatus(&out, groupStatus{Group: hunting, Members: members}, now)
	if got := out.String(); !strings.Contains(got, "State:    hunting (1 of 2 member(s) active)") ||
		!strings.Contains(got, "Slot:     held by w-a until 2026-11-08T12:40:00Z") {
		t.Errorf("hunting status:\n%s", got)
	}
}

// `lagotto cancel <group>` against the emulator: the group ends, every active
// member is cancelled with the group's tombstone, a member's pending
// reservation is released, an already-ended member is left alone, and another
// caller's group reads as not found.
func TestRunCancelGroup_Substrate(t *testing.T) {
	env := testutil.SubstrateServer(t)
	env.CreateWatchesTable(t, watchesTable)
	env.CreateHistoryTable(t, historyTable)
	ctx := context.Background()
	cfg := env.AWSConfig
	stsClient := sts.NewFromConfig(cfg)
	id, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatal(err)
	}
	me := aws.ToString(id.Arn)
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	now := time.Now().UTC()
	put := func(w *watcher.Watch) {
		t.Helper()
		if _, err := store.JoinGroup(ctx, w.Group, w.UserID, w.WatchID, watcher.RetentionTTL(now)); err != nil {
			t.Fatal(err)
		}
		w.CreatedAt, w.ExpiresAt = now, now.Add(24*time.Hour)
		if err := store.PutWatch(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	since := now
	put(&watcher.Watch{WatchID: "w-spawn", UserID: me, Group: "gpu", Status: watcher.StatusActive, Action: watcher.ActionSpawn, InstanceTypePattern: "g6e.xlarge"})
	put(&watcher.Watch{WatchID: "w-hold", UserID: me, Group: "gpu", Status: watcher.StatusActive, Action: watcher.ActionHold, InstanceTypePattern: "g6e.xlarge",
		PendingReservationID: "cr-1", PendingRegion: "us-west-2", PendingSince: &since})
	put(&watcher.Watch{WatchID: "w-done", UserID: me, Group: "gpu", Status: watcher.StatusFailed, Action: watcher.ActionSpawn, InstanceTypePattern: "g6e.xlarge"})
	if _, err := store.ReserveGroupSlot(ctx, "gpu", "w-spawn", now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.JoinGroup(ctx, "theirs", "arn:aws:iam::000000000000:user/bob", "w-bob", 0); err != nil {
		t.Fatal(err)
	}

	prev := cancelYes
	cancelYes = true
	t.Cleanup(func() { cancelYes = prev })
	rel := &fakeReleaser{}
	var out, errOut bytes.Buffer
	var cleaned []string
	cleanup := func(w *watcher.Watch) { cleaned = append(cleaned, w.WatchID) }

	if err := runCancelGroup(ctx, &out, &errOut, store, stsClient, rel, "theirs", cleanup); err == nil || !strings.Contains(err.Error(), `no watch or group "theirs" found`) {
		t.Errorf("another caller's group: %v, want not found", err)
	}
	if err := runCancelGroup(ctx, &out, &errOut, store, stsClient, rel, "gpu", cleanup); err != nil {
		t.Fatalf("cancel group: %v\n%s", err, errOut.String())
	}
	g, _ := store.GetGroup(ctx, "gpu")
	if g.State != watcher.GroupCancelled || g.SlotWatch != "" {
		t.Errorf("group = %+v, want cancelled with the slot cleared", g)
	}
	want := "group gpu cancelled: cancelled by its owner (lagotto cancel gpu)"
	for _, id := range []string{"w-spawn", "w-hold"} {
		w, _ := store.GetWatch(ctx, id)
		if w.Status != watcher.StatusCancelled || w.CancelledReason != want || w.Landing() {
			t.Errorf("%s = %s %q landing=%v", id, w.Status, w.CancelledReason, w.Landing())
		}
	}
	if w, _ := store.GetWatch(ctx, "w-done"); w.Status != watcher.StatusFailed {
		t.Errorf("w-done = %s, want left failed", w.Status)
	}
	if rel.calls != 1 || rel.id != "cr-1" || rel.region != "us-west-2" {
		t.Errorf("released %d (%s in %s), want cr-1 in us-west-2", rel.calls, rel.id, rel.region)
	}
	if strings.Join(cleaned, ",") != "w-hold,w-spawn" {
		t.Errorf("email cleanup ran for %v", cleaned)
	}
	if !strings.Contains(out.String(), "Cancelled group gpu") {
		t.Errorf("output:\n%s", out.String())
	}
	// Again: nothing active, the group ended.
	if err := runCancelGroup(ctx, &out, &errOut, store, stsClient, rel, "gpu", cleanup); err == nil {
		t.Error("cancelled an ended group with no active member")
	}
}
