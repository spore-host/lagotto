package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/watcher"
)

func spawnWatch(id, config string) watcher.Watch {
	return watcher.Watch{WatchID: id, Status: watcher.StatusActive, Action: watcher.ActionSpawn, LaunchConfigJSON: []byte(config)}
}

// #309: every armed watch and launch the poller will refuse, and nothing else.
func TestLaunchConfigFindings(t *testing.T) {
	inline := `{"iampolicydocument":"{}"}`
	watches := []watcher.Watch{
		spawnWatch("o/w-inline", inline),
		spawnWatch("o/w-ok", `{"instancetype":"p5.48xlarge"}`),
		spawnWatch("o/w-role", `{"iamrole":"unauthorized-role"}`),
		spawnWatch("o/w-goodrole", `{"iamrole":"authorized-role"}`),
		{WatchID: "o/w-expired", Status: watcher.StatusExpired, Action: watcher.ActionSpawn, LaunchConfigJSON: []byte(inline)},
		{WatchID: "o/w-hold", Status: watcher.StatusActive, Action: watcher.ActionHold},
	}
	launches := []watcher.ScheduledLaunch{
		{ScheduleID: "o/sl-inline", Status: watcher.ScheduledPending, LaunchConfigJSON: []byte(inline)},
		{ScheduleID: "o/sl-fired", Status: watcher.ScheduledLaunched, LaunchConfigJSON: []byte(inline)},
		{ScheduleID: "o/sl-cron", Status: watcher.ScheduledLaunched, CronExpr: "cron(0 9 * * ? *)", LaunchConfigJSON: []byte(inline)},
	}
	roles := func(role string) ([]string, bool) {
		if role == "unauthorized-role" {
			return []string{"iam:PassRole"}, true
		}
		return nil, true
	}
	var got []string
	for _, f := range launchConfigFindings(watches, launches, roles) {
		got = append(got, watcher.ShortID(f.ID)+" "+f.Kind)
	}
	want := "w-inline inline-iam, w-role role-unauthorized, sl-inline inline-iam, sl-cron inline-iam"
	if strings.Join(got, ", ") != want {
		t.Errorf("findings = %s, want %s", strings.Join(got, ", "), want)
	}

	// An unknown authorization is never reported as a problem.
	unknown := func(string) ([]string, bool) { return nil, false }
	if fs := launchConfigFindings(watches[2:3], nil, unknown); len(fs) != 0 {
		t.Errorf("unknown authorization reported: %+v", fs)
	}
}

func TestListFlagsRefusedWatches(t *testing.T) {
	watches := []watcher.Watch{spawnWatch("o/w-inline", `{"iampolicyfile":"./p.json"}`), spawnWatch("o/w-ok", `{}`)}
	annotateLaunchConfigProblems(watches, nil)
	if watches[0].LaunchConfigProblem == nil || watches[1].LaunchConfigProblem != nil {
		t.Fatalf("problems = %+v, %+v", watches[0].LaunchConfigProblem, watches[1].LaunchConfigProblem)
	}
	var out strings.Builder
	writeLaunchConfigProblems(&out, watches)
	for _, want := range []string{"REFUSE to launch", "w-inline: spawn config sets iam_policy_file", "1. an administrator", "lagotto setup --instance-role <role>", "3. lagotto update w-inline --spawn-config <file>"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("list output lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	writeLaunchConfigProblems(&out, watches[1:])
	if out.Len() != 0 {
		t.Errorf("printed %q with no problem", out.String())
	}
}

func TestDoctorLaunchConfigCheck(t *testing.T) {
	watches := func(context.Context) ([]watcher.Watch, error) {
		return []watcher.Watch{spawnWatch("o/w-inline", `{"iampolicydocument":"{}"}`)}, nil
	}
	noLaunches := func(context.Context) ([]watcher.ScheduledLaunch, error) { return nil, nil }
	denied := func(context.Context) ([]watcher.ScheduledLaunch, error) {
		return nil, errors.New("AccessDeniedException: dynamodb:Scan")
	}
	none := func(context.Context) ([]watcher.Watch, error) { return nil, nil }

	for _, c := range []struct {
		name     string
		w        func(context.Context) ([]watcher.Watch, error)
		l        func(context.Context) ([]watcher.ScheduledLaunch, error)
		want     doctor.Status
		contains string
	}{
		{"refused watch", watches, noLaunches, doctor.StatusFail, "will refuse"},
		{"refused watch, launches unreadable", watches, denied, doctor.StatusFail, "will refuse"},
		{"launches unreadable", none, denied, doctor.StatusWarn, "dynamodb:Scan"},
		{"clean", none, noLaunches, doctor.StatusPass, "pass the poller's launch rules"},
	} {
		r := &doctor.Report{}
		appendLaunchConfigCheck(context.Background(), r, c.w, c.l, nil)
		if len(r.Checks) != 1 || r.Checks[0].Name != doctor.CheckLaunchConfigs {
			t.Fatalf("%s: checks = %+v", c.name, r.Checks)
		}
		got := r.Checks[0]
		if got.Status != c.want || !strings.Contains(got.Summary, c.contains) {
			t.Errorf("%s: %s %q, want %s containing %q", c.name, got.Status, got.Summary, c.want, c.contains)
		}
	}
}
