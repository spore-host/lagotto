package doctor

import (
	"fmt"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// CheckLaunchConfigs names the stored-launch-config check (#309).
const CheckLaunchConfigs = "launch-configs"

// EvalLaunchConfigs reports every armed watch and scheduled launch whose
// stored launch config the poller will refuse at launch: inline IAM armed
// before 0.70.0, an unusable iam_role, or one the deployed poller may not
// pass. Pure. FAIL when there is one: such a watch waits, matches, and then
// stops as failed with nothing launched, which is the worst moment to find
// out. checked counts the records looked at; listErr is a read that failed
// (scheduled launches need a Scan, so administrator credentials), a WARN.
func EvalLaunchConfigs(findings []watcher.LaunchConfigFinding, checked int, listErr error) Check {
	c := Check{Name: CheckLaunchConfigs}
	for i := range findings {
		c.Detail = append(c.Detail, findings[i].Lines()...)
	}
	switch {
	case len(findings) > 0:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("%d armed watch(es)/scheduled launch(es) carry a launch config the poller will refuse when it fires", len(findings))
		c.Fix = []string{"for each one above: authorize a role, name it in the spawn config's iam_role, then 'lagotto update <id> --spawn-config <file>' (run by its owner)"}
	case listErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("stored launch configs not fully checked: %v (listing every watch and scheduled launch needs dynamodb:Scan, an administrator's)", listErr)
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("%d armed watch/scheduled-launch config(s) pass the poller's launch rules", checked)
	}
	if listErr != nil && len(findings) > 0 {
		c.Detail = append(c.Detail, fmt.Sprintf("(not fully checked: %v)", listErr))
	}
	return c
}
