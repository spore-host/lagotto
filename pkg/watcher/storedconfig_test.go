package watcher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/failure"
)

func TestCheckStoredLaunchConfig(t *testing.T) {
	for _, c := range []struct {
		name, config, kind, role string
		fixHas                   string
	}{
		{"plain", `{"instancetype":"p5.48xlarge","ttl":"8h"}`, "", "", ""},
		{"authorized-looking role", `{"iamrole":"research-role"}`, "", "", ""},
		{"inline document", `{"iampolicydocument":"{\"Version\":\"2012-10-17\"}"}`, FindingInlineIAM, "", "lagotto setup --instance-role <role>"},
		{"inline document with a role", `{"iamrole":"research-role","iampolicydocument":"{}"}`, FindingInlineIAM, "research-role", "lagotto setup --instance-role research-role"},
		{"policy file", `{"iampolicyfile":"./p.json"}`, FindingInlineIAM, "", "remove iam_policy, iam_policy_file and iam_policy_document"},
		{"managed policies", `{"iampolicy":["AmazonS3ReadOnlyAccess"]}`, FindingInlineIAM, "", "lagotto update w-1a2b3c4d --spawn-config <file>"},
		{"spored role", `{"iamrole":"spored-instance-role"}`, FindingRoleUnusable, "spored-instance-role", "plain role name"},
		{"role ARN", `{"iamrole":"arn:aws:iam::123456789012:role/x"}`, FindingRoleUnusable, "arn:aws:iam::123456789012:role/x", "lagotto update"},
		{"user data file", `{"userdatafile":"./boot.sh"}`, FindingOtherRefusal, "", "lagotto update w-1a2b3c4d"},
		{"unreadable", `{`, FindingOtherRefusal, "", "lagotto update"},
		{"empty", ``, "", "", ""},
	} {
		f := CheckStoredLaunchConfig("AROAX:alice/w-1a2b3c4d", []byte(c.config))
		if c.kind == "" {
			if f != nil {
				t.Errorf("%s: finding %+v, want none", c.name, f)
			}
			continue
		}
		if f == nil {
			t.Errorf("%s: no finding, want %s", c.name, c.kind)
			continue
		}
		if f.Kind != c.kind || f.Role != c.role {
			t.Errorf("%s: kind %q role %q, want %q %q", c.name, f.Kind, f.Role, c.kind, c.role)
		}
		if !strings.Contains(strings.Join(f.Fix, "\n"), c.fixHas) {
			t.Errorf("%s: fix %q lacks %q", c.name, f.Fix, c.fixHas)
		}
		// The reason is the poller's own refusal, word for word.
		if f.Kind == FindingInlineIAM && !strings.Contains(f.Reason, launchPrivilegeHint) {
			t.Errorf("%s: reason %q is not the poller's refusal", c.name, f.Reason)
		}
	}
}

// The finding is exactly what the poller refuses: everything it flags, the
// spawner refuses with ErrConfigRefused, and vice versa.
func TestCheckStoredLaunchConfigAgreesWithTheSpawner(t *testing.T) {
	for _, cfg := range []string{
		`{"iampolicydocument":"{}"}`, `{"iamrole":"spored-x"}`, `{"userdata":"@/etc/passwd"}`, `{"iamrole":"ok-role"}`, `{"instancetype":"p5.48xlarge"}`,
	} {
		sl := &ScheduledLaunch{ScheduleID: "sl-1", LaunchConfigJSON: []byte(cfg)}
		_, err := (&Spawner{}).LaunchScheduled(context.Background(), sl)
		refused := err != nil && IsConfigRefused(err)
		if flagged := CheckStoredLaunchConfig("sl-1", []byte(cfg)) != nil; flagged != refused {
			t.Errorf("%s: flagged=%v but the spawner refused=%v (%v)", cfg, flagged, refused, err)
		}
	}
}

func TestRoleUnauthorizedFinding(t *testing.T) {
	f := RoleUnauthorizedFinding("sl-0011aabb", "research-role", []string{"iam:PassRole"})
	if f.Kind != FindingRoleUnauthorized || !strings.Contains(f.Reason, "iam:PassRole") ||
		f.Fix[0] != "an administrator authorizes it: lagotto setup --instance-role research-role" {
		t.Errorf("finding = %+v", f)
	}
	lines := f.Lines()
	if !strings.HasPrefix(lines[0], "sl-0011aabb: ") || !strings.HasPrefix(lines[1], "  1. ") {
		t.Errorf("lines = %q", lines)
	}
}

func TestScheduledLaunchArmed(t *testing.T) {
	for _, c := range []struct {
		sl   ScheduledLaunch
		want bool
	}{
		{ScheduledLaunch{Status: ScheduledPending}, true},
		{ScheduledLaunch{Status: ScheduledRetrying}, true},
		{ScheduledLaunch{Status: ScheduledLaunched}, false},
		{ScheduledLaunch{Status: ScheduledFailed}, false},
		{ScheduledLaunch{Status: ScheduledCancelled}, false},
		{ScheduledLaunch{Status: ScheduledLaunched, CronExpr: "cron(0 9 * * ? *)"}, true},
		{ScheduledLaunch{Status: ScheduledFailed, CronExpr: "cron(0 9 * * ? *)"}, true},
		{ScheduledLaunch{Status: ScheduledCancelled, CronExpr: "cron(0 9 * * ? *)"}, false},
		{ScheduledLaunch{Status: ScheduledPending, MovedTo: "AROAX:a/sl-1"}, false},
	} {
		if got := c.sl.Armed(); got != c.want {
			t.Errorf("%+v Armed = %v, want %v", c.sl, got, c.want)
		}
	}
}

// #309: a watch the poller refuses at land time tells its owner why, with the
// fix, instead of a match notice that says capacity was found.
func TestNotifyConfigRefused(t *testing.T) {
	w := &Watch{WatchID: "AROAX:alice/w-1a2b3c4d", InstanceTypePattern: "p5.48xlarge", Action: ActionSpawn,
		LaunchConfigJSON: []byte(`{"iampolicyfile":"./p.json"}`),
		NotifyChannels:   []NotifyChannel{{Type: "email", Target: "alice@example.com"}, {Type: "sns", Target: "arn:aws:sns:us-east-1:111122223333:mine"}}}
	m := &MatchResult{InstanceType: "p5.48xlarge", Region: "us-east-1", MatchedAt: time.Unix(1700000000, 0)}
	pub := &capturePublisher{}
	n := &Notifier{snsClient: pub, topicArn: "arn:aws:sns:us-east-1:111122223333:lagotto-capacity-alerts"}
	err := fmt.Errorf("%w: spawn config sets iam_policy_file: %s", failure.ErrConfigRefused, launchPrivilegeHint)
	if nerr := n.NotifyConfigRefused(context.Background(), w, m, err); nerr != nil {
		t.Fatal(nerr)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("published %d messages, want email + sns", len(pub.msgs))
	}
	email := pub.msgs[0]
	if s := aws.ToString(email.Subject); !strings.Contains(s, "w-1a2b3c4d stopped: its launch config was refused") {
		t.Errorf("subject = %q", s)
	}
	body := aws.ToString(email.Message)
	for _, want := range []string{
		"did not launch it", "spawn config sets iam_policy_file", "lagotto setup --instance-role <role>",
		"set iam_role: <role>", "create a new watch", "Nothing was launched",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "launch config refused: ") {
		t.Errorf("body repeats the sentinel prefix:\n%s", body)
	}
	if p := aws.ToString(pub.msgs[1].Message); !strings.Contains(p, `"event":"config_refused"`) || !strings.Contains(p, `"fix":[`) {
		t.Errorf("sns payload = %s", p)
	}
	// No channel: nothing to send, and no error.
	if err := n.NotifyConfigRefused(context.Background(), &Watch{}, m, err); err != nil {
		t.Errorf("no channels: %v", err)
	}
}
