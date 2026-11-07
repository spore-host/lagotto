package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/pflag"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// --- pure parts ---------------------------------------------------------

func baseUpdateWatch() *watcher.Watch {
	return &watcher.Watch{
		WatchID:             "w-1",
		UserID:              fakeCallerARN,
		Status:              watcher.StatusActive,
		Service:             watcher.ServiceEC2,
		InstanceTypePattern: "g5.xlarge",
		Regions:             []string{"us-east-1"},
		Action:              watcher.ActionNotify,
		NotifyChannels:      []watcher.NotifyChannel{{Type: "email", Target: "a@example.com"}},
		ExpiresAt:           time.Now().UTC().Add(time.Hour),
	}
}

func noSpawn(string) ([]byte, error) { return nil, errors.New("no spawn config in this test") }

func TestApplyUpdate(t *testing.T) {
	cases := []struct {
		name    string
		o       updateOpts
		check   func(t *testing.T, nw *watcher.Watch, ch []fieldChange)
		wantErr string
	}{
		{name: "add-type dedupes", o: updateOpts{addTypes: []string{"g6.xlarge,g5.xlarge", "g6e.xlarge"}},
			check: func(t *testing.T, nw *watcher.Watch, ch []fieldChange) {
				if nw.InstanceTypePattern != "g5.xlarge,g6.xlarge,g6e.xlarge" || len(ch) != 1 || ch[0].Field != watcher.FieldInstanceTypePattern {
					t.Errorf("pattern %q changes %v", nw.InstanceTypePattern, ch)
				}
			}},
		{name: "instance-type replaces, then add-type appends", o: updateOpts{instanceType: "p5.*", instanceTypeSet: true, addTypes: []string{"p4d.24xlarge"}},
			check: func(t *testing.T, nw *watcher.Watch, _ []fieldChange) {
				if nw.InstanceTypePattern != "p5.*,p4d.24xlarge" {
					t.Errorf("pattern %q", nw.InstanceTypePattern)
				}
			}},
		{name: "add and remove regions", o: updateOpts{addRegions: []string{"us-west-2", "eu-west-1"}, removeRegions: []string{"us-east-1"}},
			check: func(t *testing.T, nw *watcher.Watch, _ []fieldChange) {
				if strings.Join(nw.Regions, ",") != "us-west-2,eu-west-1" {
					t.Errorf("regions %v", nw.Regions)
				}
			}},
		{name: "remove the last region", o: updateOpts{removeRegions: []string{"us-east-1"}}, wantErr: "every enabled region"},
		{name: "remove a region it lacks", o: updateOpts{removeRegions: []string{"ap-south-1"}}, wantErr: "does not search that region"},
		{name: "regions '' means all", o: updateOpts{regions: []string{""}, regionsSet: true},
			check: func(t *testing.T, nw *watcher.Watch, ch []fieldChange) {
				if len(nw.Regions) != 0 || len(ch) != 1 {
					t.Errorf("regions %v changes %v", nw.Regions, ch)
				}
			}},
		{name: "azs, spot, price, project", o: updateOpts{azs: []string{"us-east-1a"}, azsSet: true, spot: true, maxPrice: 2.5, maxPriceSet: true, project: "p", projectSet: true},
			check: func(t *testing.T, nw *watcher.Watch, ch []fieldChange) {
				if len(ch) != 4 || !nw.Spot || nw.MaxPrice != 2.5 || nw.Project != "p" || nw.AvailabilityZones[0] != "us-east-1a" {
					t.Errorf("watch %+v changes %v", nw, ch)
				}
			}},
		{name: "unchanged values are not changes", o: updateOpts{regions: []string{"us-east-1"}, regionsSet: true, noSpot: true},
			check: func(t *testing.T, _ *watcher.Watch, ch []fieldChange) {
				if len(ch) != 0 {
					t.Errorf("changes %v, want none", ch)
				}
			}},
		{name: "add and remove notify", o: updateOpts{addNotify: []string{"email:B@Example.com", "email:a@example.com"}, removeNotify: []string{"email:a@example.com"}},
			check: func(t *testing.T, nw *watcher.Watch, _ []fieldChange) {
				if got := strings.Join(channelStrings(nw.NotifyChannels), ","); got != "email:b@example.com" {
					t.Errorf("channels %s", got)
				}
			}},
		{name: "replace notify", o: updateOpts{notify: []string{"webhook:https://example.com/h"}, notifySet: true},
			check: func(t *testing.T, nw *watcher.Watch, _ []fieldChange) {
				if got := strings.Join(channelStrings(nw.NotifyChannels), ","); got != "webhook:https://example.com/h" {
					t.Errorf("channels %s", got)
				}
			}},
		{name: "remove a channel it lacks", o: updateOpts{removeNotify: []string{"email:z@example.com"}}, wantErr: "does not notify it"},
		{name: "bad notify", o: updateOpts{addNotify: []string{"pager:x"}}, wantErr: "invalid notify type"},
		{name: "spawn config on a notify watch", o: updateOpts{spawnConfig: "x.yaml"}, wantErr: "applies only to an EC2 --action spawn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := baseUpdateWatch()
			nw, ch, err := applyUpdate(w, tc.o, noSpawn)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, nw, ch)
			if w.InstanceTypePattern != "g5.xlarge" || strings.Join(w.Regions, ",") != "us-east-1" || len(w.NotifyChannels) != 1 {
				t.Errorf("applyUpdate modified the original watch: %+v", w)
			}
		})
	}
}

func TestApplyUpdate_RegionsOnAllRegionsWatch(t *testing.T) {
	w := baseUpdateWatch()
	w.Regions = nil
	if _, _, err := applyUpdate(w, updateOpts{addRegions: []string{"us-west-2"}}, noSpawn); err == nil || !strings.Contains(err.Error(), "already searches every enabled region") {
		t.Errorf("add-region to an all-regions watch: err = %v", err)
	}
}

func TestApplyUpdate_SpawnConfig(t *testing.T) {
	w := baseUpdateWatch()
	w.Action = watcher.ActionSpawn
	w.LaunchConfigJSON = []byte(`{"ami":"old"}`)
	nw, ch, err := applyUpdate(w, updateOpts{spawnConfig: "new.yaml"}, func(src string) ([]byte, error) {
		if src != "new.yaml" {
			t.Errorf("loaded %q", src)
		}
		return []byte(`{"ami":"new"}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(nw.LaunchConfigJSON) != `{"ami":"new"}` || len(ch) != 1 || !strings.HasPrefix(ch[0].After.(string), "sha256:") {
		t.Errorf("config %s changes %v", nw.LaunchConfigJSON, ch)
	}
	if _, _, err := applyUpdate(w, updateOpts{spawnConfig: "bad.yaml"}, noSpawn); err == nil {
		t.Error("a spawn config that fails to load was accepted")
	}
}

func TestUpdateOptsValidate(t *testing.T) {
	if err := (updateOpts{}).validate(); err == nil || !strings.Contains(err.Error(), "nothing to update") {
		t.Errorf("no flags: %v", err)
	}
	if err := (updateOpts{spot: true, noSpot: true}).validate(); err == nil {
		t.Error("--spot --no-spot accepted")
	}
	if err := (updateOpts{instanceTypeSet: true, instanceType: " "}).validate(); err == nil {
		t.Error("empty --instance-type accepted")
	}
	if err := (updateOpts{maxPriceSet: true, maxPrice: -1}).validate(); err == nil {
		t.Error("negative --max-price accepted")
	}
	if err := (updateOpts{projectSet: true}).validate(); err != nil {
		t.Errorf("--project '': %v", err)
	}
}

func TestCheckUpdatable(t *testing.T) {
	now := time.Now().UTC()
	mk := func(mod func(w *watcher.Watch)) *watcher.Watch {
		w := baseUpdateWatch()
		mod(w)
		return w
	}
	cases := []struct {
		name       string
		w          *watcher.Watch
		reactivate bool
		wantErr    string
	}{
		{"active", mk(func(*watcher.Watch) {}), false, ""},
		{"expired needs --reactivate", mk(func(w *watcher.Watch) { w.Status = watcher.StatusExpired }), false, "--reactivate"},
		{"expired, reactivate", mk(func(w *watcher.Watch) { w.Status = watcher.StatusExpired }), true, ""},
		{"failed, reactivate", mk(func(w *watcher.Watch) { w.Status = watcher.StatusFailed }), true, ""},
		{"expired past deadline", mk(func(w *watcher.Watch) { w.Status = watcher.StatusExpired; w.ExpiresAt = now.Add(-time.Hour) }), true, "lagotto extend"},
		{"failed past deadline", mk(func(w *watcher.Watch) { w.Status = watcher.StatusFailed; w.ExpiresAt = now.Add(-time.Hour) }), true, "new watch"},
		{"matched", mk(func(w *watcher.Watch) { w.Status = watcher.StatusMatched }), true, "only an active watch"},
		{"cancelled", mk(func(w *watcher.Watch) { w.Status = watcher.StatusCancelled }), false, "only an active watch"},
		{"landing instance", mk(func(w *watcher.Watch) { w.PendingInstanceID = "i-1" }), false, "is landing: instance i-1"},
		{"landing reservation", mk(func(w *watcher.Watch) { w.PendingReservationID = "cr-1" }), false, "capacity reservation cr-1"},
		{"sagemaker job in flight", mk(func(w *watcher.Watch) { w.SageMakerJobName = "job-1" }), false, "SageMaker job job-1"},
		{"live lease", mk(func(w *watcher.Watch) { w.LeaseOwner, w.LeaseExpiresAt = "hosted-lambda", now.Add(time.Minute) }), false, "a poller is acting"},
		{"stale lease", mk(func(w *watcher.Watch) { w.LeaseOwner, w.LeaseExpiresAt = "hosted-lambda", now.Add(-time.Minute) }), false, ""},
		{"update fence", mk(func(w *watcher.Watch) {
			w.LeaseOwner, w.LeaseExpiresAt = watcher.UpdateLeasePrefix+"x", now.Add(time.Minute)
		}), false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkUpdatable(tc.w, tc.reactivate, now)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestUpdateConflictError(t *testing.T) {
	now := time.Now().UTC()
	if err := updateConflictError("w-1", nil, errors.New("boom"), now); !strings.Contains(err.Error(), "run the update again") {
		t.Errorf("read failure: %v", err)
	}
	landing := baseUpdateWatch()
	landing.PendingInstanceID = "i-9"
	if err := updateConflictError("w-1", landing, nil, now); !strings.Contains(err.Error(), "i-9") || !strings.Contains(err.Error(), "nothing was changed") {
		t.Errorf("landing: %v", err)
	}
	leased := baseUpdateWatch()
	leased.LeaseOwner, leased.LeaseExpiresAt = "p", now.Add(time.Minute)
	if err := updateConflictError("w-1", leased, nil, now); !strings.Contains(err.Error(), "a poller is acting") {
		t.Errorf("lease: %v", err)
	}
	if err := updateConflictError("w-1", baseUpdateWatch(), nil, now); !strings.Contains(err.Error(), "it is now active") {
		t.Errorf("changed: %v", err)
	}
}

func TestValidateWatchSearch(t *testing.T) {
	ul := baseUpdateWatch()
	ul.Action = watcher.ActionHold
	ul.UntilLanded = true
	ul.RemindEverySeconds = int64((7 * 24 * time.Hour) / time.Second)
	ul.RemindPauseAfter = watcher.DefaultRemindPauseAfter
	if err := validateWatchSearch(ul); err != nil {
		t.Fatalf("valid until-landed watch: %v", err)
	}
	ul.NotifyChannels = nil
	if err := validateWatchSearch(ul); err == nil || !strings.Contains(err.Error(), "needs a --notify channel") {
		t.Errorf("until-landed with no channels: %v", err)
	}
	sp := baseUpdateWatch()
	sp.Action = watcher.ActionSpawn
	if err := validateWatchSearch(sp); !errors.Is(err, errSpawnConfigRequired) {
		t.Errorf("spawn with no config: %v", err)
	}
	ml := baseUpdateWatch()
	ml.InstanceTypePattern = "ml.g5.xlarge"
	if err := validateWatchSearch(ml); err == nil {
		t.Error("ml.* pattern on an EC2 watch accepted")
	}
	neg := baseUpdateWatch()
	neg.MaxPrice = -1
	if err := validateWatchSearch(neg); err == nil {
		t.Error("negative max price accepted")
	}
}

func TestEmailDiffAndHelpers(t *testing.T) {
	w := baseUpdateWatch()
	w.NotifyChannels = append(w.NotifyChannels, watcher.NotifyChannel{Type: "email", Target: "b@example.com"})
	nw := baseUpdateWatch()
	nw.NotifyChannels = []watcher.NotifyChannel{{Type: "email", Target: "b@example.com"}, {Type: "email", Target: "c@example.com"}, {Type: "webhook", Target: "https://x"}}
	added, removed := emailDiff(w, nw)
	if strings.Join(added, ",") != "c@example.com" || strings.Join(removed, ",") != "a@example.com" {
		t.Errorf("added %v removed %v", added, removed)
	}
	sw := withEmails(nw, added)
	if len(sw.NotifyChannels) != 1 || sw.WatchID != nw.WatchID {
		t.Errorf("withEmails: %+v", sw)
	}
	sw.NotifyChannels[0].Subscription = watcher.SubPending
	copyEmailStates(nw, sw)
	if nw.NotifyChannels[1].Subscription != watcher.SubPending || nw.NotifyChannels[0].Subscription != "" {
		t.Errorf("copyEmailStates: %+v", nw.NotifyChannels)
	}
}

func TestWriteUpdate(t *testing.T) {
	w := baseUpdateWatch()
	nw, ch, err := applyUpdate(w, updateOpts{regions: nil, regionsSet: true, maxPrice: 1.25, maxPriceSet: true, notifySet: true}, noSpawn)
	if err != nil {
		t.Fatal(err)
	}
	r := updateResult{WatchID: "w-1", DryRun: true, Changes: ch, ResetState: searchChanged(ch), Watch: nw, OldStatus: watcher.StatusActive}
	var out bytes.Buffer
	if err := writeUpdate(&out, "table", r); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Dry run: would update watch w-1:",
		"regions:", "us-east-1 -> (all enabled)",
		"max_price:", "any -> $1.2500/hr",
		"notify_channels:", "email:a@example.com -> (none)",
		"were reset", "Nothing was written.",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}

	r.DryRun, r.Reactivated, r.OldStatus = false, true, watcher.StatusExpired
	out.Reset()
	_ = writeUpdate(&out, "table", r)
	if !strings.Contains(out.String(), "Updated watch w-1:") || !strings.Contains(out.String(), "expired -> active") {
		t.Errorf("applied output:\n%s", out.String())
	}

	out.Reset()
	if err := writeUpdate(&out, "json", r); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("json: %v\n%s", err, out.String())
	}
	if got["watch_id"] != "w-1" || got["reactivated"] != true || len(got["changes"].([]any)) != 3 {
		t.Errorf("json: %v", got)
	}
}

func TestDisplayValue(t *testing.T) {
	for _, tc := range []struct {
		field string
		v     any
		want  string
	}{
		{watcher.FieldAvailabilityZones, []string{}, "(all in region)"},
		{watcher.FieldNotifyChannels, []string{}, "(none)"},
		{watcher.FieldProject, "", "(none)"},
		{watcher.FieldSpot, true, "true"},
		{"other", 1.5, "1.5"},
	} {
		if got := displayValue(tc.field, tc.v); got != tc.want {
			t.Errorf("displayValue(%s, %v) = %q, want %q", tc.field, tc.v, got, tc.want)
		}
	}
}

func TestRenderHistoryUpdatedRow(t *testing.T) {
	wait := 60.0
	rows := []watcher.MatchResult{{WatchID: "w-1", ActionTaken: watcher.ActionUpdated, ChangedFields: []string{"regions", "max_price"},
		MatchedAt: time.Now().UTC(), WaitToAcquireSeconds: &wait}}
	var out bytes.Buffer
	if err := renderHistory(&out, rows, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "changed: regions, max_price") || strings.Contains(out.String(), "1m") {
		t.Errorf("history:\n%s", out.String())
	}
	e := historyEntries(rows, nil)
	if e[0].(matchEntry).Event != "updated" {
		t.Errorf("event = %q, want updated", e[0].(matchEntry).Event)
	}
}

// --- end to end, against the loopback fake ------------------------------

const updateTopic = "arn:aws:sns:us-east-1:123456789012:lagotto-capacity-alerts"

// updateWatchItem is a stored active notify watch w-1 in DynamoDB JSON, with
// an email channel and the derived state an update resets.
func updateWatchItem(status string) string {
	exp := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	return `{"watch_id":{"S":"w-1"},"user_id":{"S":"` + fakeCallerARN + `"},"status":{"S":"` + status +
		`"},"service":{"S":"ec2"},"action":{"S":"notify"},"instance_type_pattern":{"S":"g5.xlarge"},` +
		`"regions":{"L":[{"S":"us-east-1"}]},"spot":{"BOOL":false},` +
		`"notify_channels":{"L":[{"M":{"type":{"S":"email"},"target":{"S":"a@example.com"}}}]},` +
		`"consecutive_failures":{"N":"3"},"attempts":{"N":"7"},` +
		`"updated_at":{"S":"2026-11-06T10:00:00.123456789Z"},"expires_at":{"S":"` + exp + `"}}`
}

// runUpdateWithFake runs `lagotto update w-1` with flags against f.
func runUpdateWithFake(t *testing.T, f *fakeAWS, flags map[string]string) (string, error) {
	t.Helper()
	installFakeAWS(t, f)
	t.Setenv(topicEnvVar, updateTopic)
	prev := newSubnetCoverageChecker
	newSubnetCoverageChecker = func(aws.Config) *watcher.SubnetCoverageChecker {
		return watcher.NewSubnetCoverageCheckerWith(
			func(string) watcher.SubnetCoverageEC2 { return subnetEC2{} },
			func(context.Context) ([]string, error) { return nil, errors.New("no EC2 in the fake") })
	}
	t.Cleanup(func() { newSubnetCoverageChecker = prev })
	t.Cleanup(resetUpdateFlags)
	for k, v := range flags {
		if err := updateCmd.Flags().Set(k, v); err != nil {
			t.Fatalf("set --%s: %v", k, err)
		}
	}
	var out bytes.Buffer
	updateCmd.SetOut(&out)
	t.Cleanup(func() { updateCmd.SetOut(nil) })
	err := runUpdate(updateCmd, []string{"w-1"})
	return out.String(), err
}

func resetUpdateFlags() {
	updateCmd.Flags().VisitAll(func(fl *pflag.Flag) {
		if sv, ok := fl.Value.(pflag.SliceValue); ok {
			_ = sv.Replace(nil)
		} else {
			_ = fl.Value.Set(fl.DefValue)
		}
		fl.Changed = false
	})
	updateFlags = updateOpts{}
}

func (f *fakeAWS) lastUpdate(t *testing.T) map[string]any {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.updates) == 0 {
		t.Fatal("no UpdateItem")
	}
	var in map[string]any
	if err := json.Unmarshal([]byte(f.updates[len(f.updates)-1]), &in); err != nil {
		t.Fatal(err)
	}
	return in
}

func TestUpdateEndToEnd(t *testing.T) {
	sns := testutil.NewFakeSNS(updateTopic)
	sns.AddSub("a@example.com", map[string]string{"FilterPolicy": watcher.EmailFilterPolicy("a@example.com")}, true)
	withFakeSubscriber(t, sns)
	f := &fakeAWS{scheduleState: "DISABLED", item: updateWatchItem("active")}
	out, err := runUpdateWithFake(t, f, map[string]string{"add-region": "us-west-2", "add-notify": "email:b@example.com"})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !strings.Contains(out, "Updated watch w-1:") || !strings.Contains(out, "us-east-1 -> us-east-1,us-west-2") {
		t.Errorf("output:\n%s", out)
	}

	in := f.lastUpdate(t)
	cond, _ := in["ConditionExpression"].(string)
	expr, _ := in["UpdateExpression"].(string)
	vals, _ := in["ExpressionAttributeValues"].(map[string]any)
	if !strings.Contains(cond, "updated_at = :ver") || vals[":ver"].(map[string]any)["S"] != "2026-11-06T10:00:00.123456789Z" {
		t.Errorf("condition does not compare the raw updated_at read: %s %v", cond, vals[":ver"])
	}
	for _, want := range []string{"lease_expires_at < :now", "attribute_not_exists(pending_instance_id)"} {
		if !strings.Contains(cond, want) {
			t.Errorf("condition lacks %q: %s", want, cond)
		}
	}
	if !strings.Contains(expr, "consecutive_failures") || strings.Contains(expr, "attempts") {
		t.Errorf("update expression %s: must reset consecutive_failures and keep attempts", expr)
	}
	if f.called("dynamodb:PutItem") != 1 {
		t.Errorf("PutItem calls = %d, want only the history row", f.called("dynamodb:PutItem"))
	} else if f.puts[0]["action_taken"]["S"] != watcher.ActionUpdated {
		t.Errorf("history row %v", f.puts[0])
	}
	if f.scheduleState != "ENABLED" {
		t.Errorf("schedule state %q: the active watch's poller was not re-armed", f.scheduleState)
	}
	// Only the added address was subscribed; a's confirmed subscription is untouched.
	if len(sns.Subscribes) != 1 || aws.ToString(sns.Subscribes[0].Endpoint) != "b@example.com" || len(sns.Unsubs) != 0 {
		t.Errorf("subscribes %d unsubs %v", len(sns.Subscribes), sns.Unsubs)
	}
}

func TestUpdateRemovesEmailAndUnsubscribes(t *testing.T) {
	sns := testutil.NewFakeSNS(updateTopic)
	sns.AddSub("a@example.com", map[string]string{"FilterPolicy": watcher.EmailFilterPolicy("a@example.com")}, true)
	withFakeSubscriber(t, sns)
	f := &fakeAWS{scheduleState: "ENABLED", item: updateWatchItem("active")}
	if _, err := runUpdateWithFake(t, f, map[string]string{"notify": "webhook:https://example.com/h"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(sns.Unsubs) != 1 || len(sns.Subscribes) != 0 {
		t.Errorf("unsubs %v subscribes %d: the removed address should be unsubscribed and nothing subscribed", sns.Unsubs, len(sns.Subscribes))
	}
	// A notify-only change keeps the derived search state.
	if expr, _ := f.lastUpdate(t)["UpdateExpression"].(string); strings.Contains(expr, "consecutive_failures") {
		t.Errorf("a notify-only update reset the search state: %s", expr)
	}
	if f.called("scheduler:UpdateSchedule") != 0 {
		t.Error("an enabled schedule was updated")
	}
}

func TestUpdateDryRunWritesNothing(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", item: updateWatchItem("active")}
	out, err := runUpdateWithFake(t, f, map[string]string{"spot": "true", "dry-run": "true"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Dry run") || f.called("dynamodb:UpdateItem")+f.called("dynamodb:PutItem")+f.called("scheduler:GetSchedule") != 0 {
		t.Errorf("dry run wrote or armed something (calls %v):\n%s", f.calls, out)
	}
}

func TestUpdateNoChange(t *testing.T) {
	f := &fakeAWS{item: updateWatchItem("active")}
	out, err := runUpdateWithFake(t, f, map[string]string{"regions": "us-east-1"})
	if err != nil || !strings.Contains(out, "nothing to update") || f.called("dynamodb:UpdateItem") != 0 {
		t.Errorf("err %v, out %q, updates %d", err, out, f.called("dynamodb:UpdateItem"))
	}
}

func TestUpdateConflictWritesNoHistory(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", item: updateWatchItem("active"), conditionFails: true}
	_, err := runUpdateWithFake(t, f, map[string]string{"max-price": "3"})
	if err == nil || !strings.Contains(err.Error(), "nothing was changed") {
		t.Fatalf("err = %v", err)
	}
	if f.called("dynamodb:PutItem") != 0 {
		t.Error("a refused update wrote a history row")
	}
}

func TestUpdateExpiredNeedsReactivate(t *testing.T) {
	f := &fakeAWS{item: updateWatchItem("expired")}
	if _, err := runUpdateWithFake(t, f, map[string]string{"spot": "true"}); err == nil || !strings.Contains(err.Error(), "--reactivate") {
		t.Fatalf("err = %v", err)
	}
	if f.called("dynamodb:UpdateItem") != 0 {
		t.Error("refused update wrote")
	}
}

func TestUpdateReactivateRollsBackWhenScheduleDenied(t *testing.T) {
	sns := testutil.NewFakeSNS(updateTopic)
	withFakeSubscriber(t, sns)
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException", item: updateWatchItem("expired")}
	_, err := runUpdateWithFake(t, f, map[string]string{"spot": "true", "reactivate": "true"})
	if err == nil || !strings.Contains(err.Error(), "put back to expired") {
		t.Fatalf("err = %v", err)
	}
	if n := f.called("dynamodb:UpdateItem"); n != 2 {
		t.Errorf("UpdateItem calls = %d, want the update and the rollback", n)
	}
	if !strings.Contains(f.lastUpdate(t)["UpdateExpression"].(string), "#st = :status") {
		t.Errorf("rollback did not restore the status: %v", f.lastUpdate(t)["UpdateExpression"])
	}
	// A reactivated watch's addresses are all (re)subscribed (#240).
	if len(sns.Subscribes) != 1 || aws.ToString(sns.Subscribes[0].Endpoint) != "a@example.com" {
		t.Errorf("subscribes %d: the reactivated watch's address was not resubscribed", len(sns.Subscribes))
	}
}

func TestUpdateActiveScheduleDeniedKeepsUpdate(t *testing.T) {
	f := &fakeAWS{scheduleState: "DISABLED", updateErr: "AccessDeniedException", item: updateWatchItem("active")}
	_, err := runUpdateWithFake(t, f, map[string]string{"spot": "true"})
	if err == nil || !strings.Contains(err.Error(), "the update was stored") {
		t.Fatalf("err = %v", err)
	}
	if n := f.called("dynamodb:UpdateItem"); n != 1 {
		t.Errorf("UpdateItem calls = %d, want the update only", n)
	}
}

func TestUpdateRefusesWhenGetScheduleDenied(t *testing.T) {
	f := &fakeAWS{getErr: "AccessDeniedException", item: updateWatchItem("active")}
	_, err := runUpdateWithFake(t, f, map[string]string{"spot": "true"})
	if err == nil {
		t.Fatal("update succeeded although GetSchedule was denied")
	}
	if f.called("dynamodb:UpdateItem") != 0 {
		t.Error("update wrote although the schedule could not be read")
	}
}

func TestUpdateJSONOutput(t *testing.T) {
	f := &fakeAWS{scheduleState: "ENABLED", item: updateWatchItem("active")}
	prev := outputFormat
	outputFormat = "json"
	t.Cleanup(func() { outputFormat = prev })
	out, err := runUpdateWithFake(t, f, map[string]string{"azs": "us-east-1b"})
	if err != nil {
		t.Fatal(err)
	}
	var got updateResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json: %v\n%s", err, out)
	}
	if len(got.Changes) != 1 || got.Changes[0].Field != watcher.FieldAvailabilityZones || got.Watch.AvailabilityZones[0] != "us-east-1b" {
		t.Errorf("json result %+v", got)
	}
}
