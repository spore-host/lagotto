package cmd

// Parity vectors for lagotto-ts (#289), the CLI half: the flag and watch
// validation `lagotto watch`/`update`/`ack` run before any AWS call. See
// internal/parity; pkg/watcher/parity_vectors_test.go has the library half.

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/spore-host/lagotto/internal/parity"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var cliParityNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestParityVectorsCLIValidation(t *testing.T) {
	ttl := parity.Vectors{Go: "cmd.parseTTLFlag", Note: "output is seconds; tries time.ParseDuration, then lagotto's d/w form"}
	for _, in := range []string{"24h", "30m", "7d", "1.5d", "2d12h", "0", "-1h", "0s", "abc", ""} {
		var out any
		d, err := parseTTLFlag(in)
		if err == nil {
			out = d.Seconds()
		}
		ttl.Cases = append(ttl.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: out, Error: parity.ErrText(err)})
	}

	ul := parity.Vectors{Go: "cmd.parseUntilLandedFlags", Note: "the *_set inputs say whether the flag was given; output remind_every in seconds"}
	type ulIn struct {
		UntilLanded bool   `json:"until_landed"`
		Remind      string `json:"remind"`
		TTLSet      bool   `json:"ttl_set"`
		RemindSet   bool   `json:"remind_set"`
		MaintainSet bool   `json:"maintain_set"`
		UntilSet    bool   `json:"until_set"`
	}
	for _, c := range []struct {
		name string
		in   ulIn
	}{
		{"off", ulIn{Remind: "7d"}},
		{"remind without until-landed", ulIn{Remind: "1d", RemindSet: true}},
		{"default remind", ulIn{UntilLanded: true, Remind: "7d"}},
		{"remind 36h", ulIn{UntilLanded: true, Remind: "36h", RemindSet: true}},
		{"remind 1.5d", ulIn{UntilLanded: true, Remind: "1.5d", RemindSet: true}},
		{"with ttl", ulIn{UntilLanded: true, Remind: "7d", TTLSet: true}},
		{"with maintain", ulIn{UntilLanded: true, Remind: "7d", MaintainSet: true}},
		{"with until", ulIn{UntilLanded: true, Remind: "7d", UntilSet: true}},
		{"remind too short", ulIn{UntilLanded: true, Remind: "5m", RemindSet: true}},
		{"remind too long", ulIn{UntilLanded: true, Remind: "31d", RemindSet: true}},
		{"remind invalid", ulIn{UntilLanded: true, Remind: "soon", RemindSet: true}},
	} {
		f, err := parseUntilLandedFlags(c.in.UntilLanded, c.in.Remind, c.in.TTLSet, c.in.RemindSet, c.in.MaintainSet, c.in.UntilSet)
		var out any
		if err == nil {
			out = map[string]any{"enabled": f.enabled, "remind_every": f.remindEvery.Seconds(), "remind_pause_after": f.pauseAfter, "ack_window": f.window().Seconds()}
		}
		ul.Cases = append(ul.Cases, parity.Case{Name: c.name, Input: c.in, Output: out, Error: parity.ErrText(err)})
	}

	notify := parity.Vectors{Go: "cmd.parseNotifyChannels", Note: "an email is stored normalized. A webhook host is resolved (DNS) and must not be private, so a resolvable public host is not a fixed vector"}
	for _, in := range [][]string{
		{"email:Alice@Example.COM"}, {"webhook:https://127.0.0.1/x"}, {"webhook:http://hooks.example.com/x"},
		{"sns:arn:aws:sns:us-east-1:123456789012:t"}, {"slack:#ops"}, {"alice@example.com"}, {"email:not-an-address"},
		{"email:a@example.com", "sns:arn:aws:sns:us-west-2:123456789012:t"}, {},
	} {
		chs, err := parseNotifyChannels(in)
		var out any
		if err == nil {
			if chs == nil {
				chs = []watcher.NotifyChannel{}
			}
			out = chs
		}
		notify.Cases = append(notify.Cases, parity.Case{Name: fmt.Sprint(in), Input: in, Output: out, Error: parity.ErrText(err)})
	}

	fleet := parity.Vectors{Go: "cmd.validateFleetFlags"}
	for _, c := range []struct {
		action   watcher.ActionMode
		maintain int
		until    string
	}{
		{watcher.ActionSpawn, 0, ""}, {watcher.ActionSpawn, 3, ""}, {watcher.ActionHold, 3, ""}, {watcher.ActionSpawn, -1, ""},
		{watcher.ActionSpawn, 0, "s3-empty:s3://b/k/"}, {watcher.ActionSpawn, 3, "s3-empty:s3://b/k/"}, {watcher.ActionSpawn, 3, "bogus:x"},
	} {
		fleet.Cases = append(fleet.Cases, parity.Case{Name: fmt.Sprintf("%s maintain=%d until=%q", c.action, c.maintain, c.until),
			Input: map[string]any{"action": c.action, "maintain": c.maintain, "until": c.until},
			Error: parity.ErrText(validateFleetFlags(c.action, c.maintain, c.until))})
	}

	regions := parity.Vectors{Go: "cmd.validateFleetRegions"}
	for _, c := range []struct {
		maintain int
		regions  []string
	}{{0, nil}, {3, nil}, {3, []string{" "}}, {3, []string{"us-east-1"}}} {
		regions.Cases = append(regions.Cases, parity.Case{Name: fmt.Sprintf("maintain=%d regions=%q", c.maintain, c.regions),
			Input: map[string]any{"maintain": c.maintain, "regions": c.regions}, Error: parity.ErrText(validateFleetRegions(c.maintain, c.regions))})
	}

	svc := parity.Vectors{Go: "cmd.validateServiceFlags", Note: "action_given is whether --action was passed (notify is the default)"}
	for _, c := range []struct {
		service watcher.Service
		action  watcher.ActionMode
		given   bool
		config  string
	}{
		{watcher.ServiceEC2, watcher.ActionSpawn, true, ""}, {watcher.ServiceSageMaker, watcher.ActionSpawn, true, "job.yaml"},
		{watcher.ServiceSageMaker, watcher.ActionSpawn, true, ""}, {watcher.ServiceSageMaker, watcher.ActionNotify, false, "job.yaml"},
		{watcher.ServiceSageMaker, watcher.ActionNotify, true, "job.yaml"}, {watcher.ServiceSageMaker, watcher.ActionHold, true, "job.yaml"},
	} {
		svc.Cases = append(svc.Cases, parity.Case{Name: fmt.Sprintf("%s %s given=%v config=%q", c.service, c.action, c.given, c.config),
			Input: map[string]any{"service": c.service, "action": c.action, "action_given": c.given, "sagemaker_config": c.config},
			Error: parity.ErrText(validateServiceFlags(c.service, c.action, c.given, c.config))})
	}

	email := []watcher.NotifyChannel{{Type: "email", Target: "a@example.com"}}
	cfg := []byte(`{"instance_type":"p5.48xlarge"}`)
	search := parity.Vectors{Go: "cmd.validateWatchSearch", Note: "input is a stored watch (lagotto's JSON); the check every create and update runs before any AWS call"}
	for _, c := range []struct {
		name string
		w    watcher.Watch
	}{
		{"hold", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1"}}},
		{"spawn with config", watcher.Watch{Action: watcher.ActionSpawn, InstanceTypePattern: "p5.*", LaunchConfigJSON: cfg}},
		{"spawn without config", watcher.Watch{Action: watcher.ActionSpawn, InstanceTypePattern: "p5.*"}},
		{"sagemaker spawn without spawn config", watcher.Watch{Service: watcher.ServiceSageMaker, Action: watcher.ActionSpawn, InstanceTypePattern: "ml.g5.2xlarge"}},
		{"ec2 ml pattern", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "ml.g5.2xlarge"}},
		{"sagemaker hold", watcher.Watch{Service: watcher.ServiceSageMaker, Action: watcher.ActionHold, InstanceTypePattern: "ml.g5.2xlarge"}},
		{"negative max price", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "p5.48xlarge", MaxPrice: -1}},
		{"fleet without regions", watcher.Watch{Action: watcher.ActionSpawn, InstanceTypePattern: "g6.4xlarge", LaunchConfigJSON: cfg, DesiredCount: 3}},
		{"fleet with regions", watcher.Watch{Action: watcher.ActionSpawn, InstanceTypePattern: "g6.4xlarge", LaunchConfigJSON: cfg, DesiredCount: 3, Regions: []string{"us-east-1"}}},
		{"hold fleet", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "g6.4xlarge", DesiredCount: 3, Regions: []string{"us-east-1"}}},
		{"until-landed hold with email", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "p5.48xlarge", UntilLanded: true,
			RemindEverySeconds: int64(watcher.DefaultRemindEvery / time.Second), RemindPauseAfter: watcher.DefaultRemindPauseAfter, NotifyChannels: email}},
		{"until-landed hold without a channel", watcher.Watch{Action: watcher.ActionHold, InstanceTypePattern: "p5.48xlarge", UntilLanded: true,
			RemindEverySeconds: int64(watcher.DefaultRemindEvery / time.Second)}},
		{"until-landed notify", watcher.Watch{Action: watcher.ActionNotify, InstanceTypePattern: "p5.48xlarge", UntilLanded: true,
			RemindEverySeconds: int64(watcher.DefaultRemindEvery / time.Second), NotifyChannels: email}},
		{"empty pattern", watcher.Watch{Action: watcher.ActionHold}},
	} {
		w := c.w
		search.Cases = append(search.Cases, parity.Case{Name: c.name, Input: w, Error: parity.ErrText(validateWatchSearch(&w))})
	}

	ack := parity.Vectors{Go: "cmd.ackExpiry", Note: "now is fixed; output is the new expires_at"}
	for _, c := range []struct {
		name string
		w    watcher.Watch
	}{
		{"until-landed 7d pause 2", watcher.Watch{WatchID: "w-1a2b3c4d", UntilLanded: true, RemindEverySeconds: 7 * 86400, RemindPauseAfter: 2}},
		{"until-landed 1d pause 0", watcher.Watch{WatchID: "w-1a2b3c4d", UntilLanded: true, RemindEverySeconds: 86400}},
		{"ttl watch", watcher.Watch{WatchID: "w-1a2b3c4d", ExpiresAt: cliParityNow.Add(time.Hour)}},
	} {
		w := c.w
		var out any
		d, err := ackExpiry(&w, cliParityNow)
		if err == nil {
			out = d
		}
		ack.Cases = append(ack.Cases, parity.Case{Name: c.name, Input: map[string]any{"now": cliParityNow, "watch": w}, Output: out, Error: parity.ErrText(err)})
	}

	launch := parity.Vectors{Go: "watcher.ParseSpawnConfigYAML + SpawnConfigFile.ValidateLaunchPrivileges + InstanceProfileName",
		Note: "the spawn-config rules a stored watch or scheduled launch must pass, at creation and again by the poller before every launch"}
	for _, c := range []struct{ name, yaml string }{
		{"plain", "instance_type: p5.48xlarge\nttl: 8h\n"},
		{"authorized role", "iam_role: research-role\n"},
		{"inline policy document", "iam_policy_document: '{\"Version\":\"2012-10-17\"}'\n"},
		{"policy file", "iam_policy_file: ./policy.json\n"},
		{"managed policies", "iam_policy: [AmazonS3ReadOnlyAccess]\n"},
		{"spored role", "iam_role: spored-instance-role\n"},
		{"poller role", "iam_role: lagotto-capacity-poller-role\n"},
		{"role ARN", "iam_role: arn:aws:iam::123456789012:role/x\n"},
		{"wildcard role", "iam_role: research-*\n"},
		{"user data file", "user_data_file: ./boot.sh\n"},
		{"user data @path", "user_data: '@/etc/passwd'\n"},
		{"bad key name", "key_name: \"bad\\nname\"\n"},
		{"unknown key", "instance_typo: p5.48xlarge\n"},
	} {
		var out any
		sc, err := watcher.ParseSpawnConfigYAML([]byte(c.yaml))
		if err == nil {
			err = sc.ValidateLaunchPrivileges()
			if err == nil {
				out = map[string]any{"instance_profile": sc.InstanceProfileName()}
			}
		}
		launch.Cases = append(launch.Cases, parity.Case{Name: c.name, Input: c.yaml, Output: out, Error: parity.ErrText(err)})
	}

	parity.Check(t, "cli-validation.json", parity.File{Area: "CLI flag and watch validation", Vectors: map[string]parity.Vectors{
		"parseTTLFlag": ttl, "parseUntilLandedFlags": ul, "parseNotifyChannels": notify, "validateFleetFlags": fleet,
		"validateFleetRegions": regions, "validateServiceFlags": svc, "validateWatchSearch": search, "ackExpiry": ack,
		"spawnConfigLaunchPrivileges": launch,
	}})
}
