package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// No tables in a region without a poller (lagotto#325). Before, `watch`,
// `launch` and `setup` created lagotto's tables in whatever region the CLI
// resolved: one account had tables in 8 regions and pollers in 2, and nothing
// polled, or ever removed, the other 6.

// dynamoCalls counts the DynamoDB calls f saw.
func dynamoCalls(f *fakeAWS) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, "dynamodb:") {
			n++
		}
	}
	return n
}

// TestWatchRefusesWithNoPoller: no poller schedule in the region, no --local.
// Refused, naming both ways forward, before any DynamoDB call: no table is
// created and no watch is stored.
func TestWatchRefusesWithNoPoller(t *testing.T) {
	f := &fakeAWS{tablesAbsent: true}
	err := runWatchWithFake(t, f)
	if err == nil {
		t.Fatal("watch succeeded in a region with no poller and no --local")
	}
	for _, want := range []string{"no hosted poller is deployed in us-east-1", "nothing was created",
		"lagotto deploy --region us-east-1", "--local", "lagotto poll --daemon"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not say %q:\n%v", want, err)
		}
	}
	if n := f.called("dynamodb:CreateTable"); n != 0 {
		t.Errorf("watch created %d table(s) in a region with no poller", n)
	}
	if n := dynamoCalls(f); n != 0 {
		t.Errorf("watch made %d DynamoDB call(s) before refusing (%v); it must touch no table", n, f.calls)
	}
}

// TestWatchLocalCreatesTables: --local is the explicit local-daemon mode. It
// creates the tables where they are absent and stores the watch.
func TestWatchLocalCreatesTables(t *testing.T) {
	setFlagsForTest(t, watchCmd, map[string]string{"local": "true"})
	f := &fakeAWS{tablesAbsent: true}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch --local: %v", err)
	}
	if n := f.called("dynamodb:CreateTable"); n != 2 {
		t.Errorf("CreateTable called %d times, want 2 (watches and match history)", n)
	}
	if got := putStatuses(f); len(got) != 1 || got[0] != "active" {
		t.Errorf("PutItem statuses = %v, want one active watch", got)
	}
}

// TestWatchWithPollerCreatesMissingTables: where a poller IS deployed, a
// missing table (deleted by hand) is still created, as before.
func TestWatchWithPollerCreatesMissingTables(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", tablesAbsent: true}
	if err := runWatchWithFake(t, f); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if n := f.called("dynamodb:CreateTable"); n != 2 {
		t.Errorf("CreateTable called %d times, want 2", n)
	}
}

func TestRequireHostedPoller(t *testing.T) {
	if requireHostedPoller("eu-west-1", true, false) != nil || requireHostedPoller("eu-west-1", false, true) != nil ||
		requireHostedPoller("eu-west-1", true, true) != nil {
		t.Error("a deployed poller or --local was refused")
	}
	err := requireHostedPoller("eu-west-1", false, false)
	if err == nil || !strings.Contains(err.Error(), "lagotto deploy --region eu-west-1") {
		t.Errorf("err = %v, want the refusal naming 'lagotto deploy --region eu-west-1'", err)
	}
}

// TestLaunchRefusesWithNoPoller: `lagotto launch` checks for the poller
// function before it ensures the scheduled-launches table, so with none it
// creates nothing and says where to deploy.
func TestLaunchRefusesWithNoPoller(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "launch.yaml")
	if err := os.WriteFile(cfgPath, []byte("instance_type: g5.xlarge\nttl: 2h\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	setFlagsForTest(t, launchCmd, map[string]string{"spawn-config": cfgPath, "after": "6h"})
	f := &fakeAWS{tablesAbsent: true}
	installFakeAWS(t, f)
	var out bytes.Buffer
	launchCmd.SetOut(&out)
	t.Cleanup(func() { launchCmd.SetOut(nil) })

	err := runLaunch(launchCmd, nil)
	if err == nil {
		t.Fatal("launch succeeded with no poller deployed")
	}
	if !strings.Contains(err.Error(), "lagotto deploy --region us-east-1") || !strings.Contains(err.Error(), "nothing was created") {
		t.Errorf("refusal does not name 'lagotto deploy --region us-east-1':\n%v", err)
	}
	if n := f.called("lambda:GetFunction"); n != 1 {
		t.Errorf("GetFunction called %d times, want 1: the refusal must come from the poller check", n)
	}
	if n := dynamoCalls(f); n != 0 {
		t.Errorf("launch made %d DynamoDB call(s) with no poller (%v); it must create no table", n, f.calls)
	}
}

// TestSetupCreatesNoTables: setup's table step, against a region where none of
// lagotto's tables exist, creates none and says where they come from. The
// control half shows the fake would have recorded a create.
func TestSetupCreatesNoTables(t *testing.T) {
	f := &fakeAWS{tablesAbsent: true}
	installFakeAWS(t, f)
	cfg, err := awscfg.Load(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	store := watcher.NewStore(cfg, "lagotto-watches", "lagotto-match-history")
	var out bytes.Buffer
	if err := setupTables(context.Background(), store, "us-east-1", &out); err != nil {
		t.Fatalf("setupTables: %v", err)
	}
	if n := f.called("dynamodb:CreateTable"); n != 0 {
		t.Errorf("setup created %d table(s)", n)
	}
	if n := f.called("dynamodb:DescribeTable"); n == 0 {
		t.Error("setup did not look at the tables at all; the tag step is not running")
	}
	if !strings.Contains(out.String(), "lagotto deploy --region us-east-1") {
		t.Errorf("setup does not say where the tables come from:\n%s", out.String())
	}

	// Control: the same store and fake do record a create.
	if _, err := store.EnsureTables(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := f.called("dynamodb:CreateTable"); n == 0 {
		t.Error("the fake records no CreateTable even for EnsureTables; the assertion above is vacuous")
	}
}
