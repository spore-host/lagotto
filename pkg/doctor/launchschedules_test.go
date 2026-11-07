package doctor

import (
	"errors"
	"strings"
	"testing"
)

func TestEvalLaunchSchedules(t *testing.T) {
	const region = "us-west-2"

	if _, ok := EvalLaunchSchedules(region, nil, nil, nil, nil); ok {
		t.Error("no per-launch schedules should say nothing")
	}

	c, ok := EvalLaunchSchedules(region, nil, errors.New("AccessDenied"), nil, nil)
	if !ok || c.Status != StatusWarn || !strings.Contains(c.Summary, "scheduler:ListSchedules") {
		t.Errorf("list failure: %+v", c)
	}

	c, ok = EvalLaunchSchedules(region, []string{"lagotto-launch-sl-a"}, nil, nil, errors.New("scan denied"))
	if !ok || c.Status != StatusWarn || !strings.Contains(c.Summary, "scan denied") {
		t.Errorf("records failure: %+v", c)
	}

	c, ok = EvalLaunchSchedules(region,
		[]string{"lagotto-launch-sl-a", "lagotto-launch-sl-a-r2"}, nil,
		[]string{"lagotto-launch-sl-a"}, nil)
	if !ok || c.Status != StatusPass {
		t.Errorf("a record and its retry are not orphans: %+v", c)
	}

	c, ok = EvalLaunchSchedules(region,
		[]string{"lagotto-launch-sl-a", "lagotto-launch-sl-orph"}, nil,
		[]string{"lagotto-launch-sl-a", ""}, nil)
	if !ok || c.Status != StatusWarn || !strings.HasPrefix(c.Summary, "1 per-launch") {
		t.Fatalf("orphan: %+v", c)
	}
	detail := strings.Join(c.Detail, "\n")
	if !strings.Contains(detail, "aws scheduler delete-schedule --name lagotto-launch-sl-orph --region us-west-2") ||
		strings.Contains(detail, "--name lagotto-launch-sl-a ") || !strings.Contains(detail, "lagotto teardown") {
		t.Errorf("orphan detail:\n%s", detail)
	}
}
