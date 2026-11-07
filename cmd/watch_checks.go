package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/truffle/pkg/quotas"
)

// The checks `watch` makes on the watch it is about to store, factored out so
// `update` (#257) makes exactly the same ones on the watch it is about to
// write: an update must not be able to produce a watch `watch` would refuse.

// errSpawnConfigRequired is the refusal of an EC2 spawn watch with no spawn
// config.
var errSpawnConfigRequired = errors.New("--spawn-config is required when --action=spawn")

// readSpawnConfigFlag reads a --spawn-config source (a local path, s3:// URI or
// "-" for stdin) and returns the self-contained JSON a watch stores
// (loadEC2SpawnConfig).
func readSpawnConfigFlag(ctx context.Context, source string) ([]byte, error) {
	read := watcher.NewConfigReader(func(ctx context.Context) (aws.Config, error) {
		return awscfg.Load(ctx, "")
	}, os.Stdin)
	b, err := loadEC2SpawnConfig(ctx, read, source)
	if err != nil {
		return nil, fmt.Errorf("load spawn config: %w", err)
	}
	return b, nil
}

// validateWatchSearch checks the stored shape of w, with no AWS call: its
// pattern fits its service, an EC2 spawn watch has a spawn config, a fleet's
// --maintain/--until are consistent, and an until-landed watch can still tell
// its owner anything (#239: it needs a notify channel). Notify channels are
// validated where they are parsed (parseNotifyChannels).
func validateWatchSearch(w *watcher.Watch) error {
	if err := watcher.ValidateWatchPattern(w.Service, w.InstanceTypePattern); err != nil {
		return err
	}
	if err := watcher.ValidateServiceAction(w.Service, w.Action); err != nil {
		return err
	}
	if w.Service != watcher.ServiceSageMaker && w.Action == watcher.ActionSpawn && len(w.LaunchConfigJSON) == 0 {
		return errSpawnConfigRequired
	}
	if w.MaxPrice < 0 {
		return fmt.Errorf("--max-price must be >= 0 (0 = any)")
	}
	if err := validateFleetFlags(w.Action, w.DesiredCount, w.CompletionCondition); err != nil {
		return err
	}
	if w.UntilLanded {
		remind := time.Duration(w.RemindEverySeconds) * time.Second
		if err := watcher.ValidateUntilLanded(w.Action, w.DesiredCount, w.NotifyChannels, remind, w.RemindPauseAfter); err != nil {
			return err
		}
	}
	return nil
}

// preflightWatch makes the read-only AWS checks on w, before it is stored:
//
//   - default-subnet coverage (#256): refuses a spawn/hold watch that can never
//     land in a region it names;
//   - the fleet quota feasibility warning (#153): a --maintain goal that exceeds
//     the account's vCPU quota. Deliberately NON-FATAL and without a --force
//     escape hatch: truffle's GetQuotas swallows per-quota errors
//     (truffle#167), so "zero quota" isn't reliably distinguishable from
//     "lookup failed / no permission / throttled" (opt-in strictness is #157);
//   - the instance-role authorization check (#170). Unlike the quota check it
//     CAN be fatal, because it refuses only on direct evidence from the
//     deployed policy (see checkInstanceRoleAuthorized), so a watch that
//     provably cannot launch is never stored to wait for capacity it can't use.
//
// The quota report and the instance-role verdict are returned for the JSON
// output. Single-shot quota refusals (#238) are found by the poller, at launch,
// and have no create-time check.
func preflightWatch(ctx context.Context, cfg aws.Config, errOut io.Writer, w *watcher.Watch) (*quotacheck.Report, *InstanceRoleCheck, error) {
	if _, err := checkWatchSubnetCoverage(ctx, newSubnetCoverageChecker(cfg), errOut, w); err != nil {
		return nil, nil, err
	}
	quotaReport := warnFleetQuotaFeasibility(ctx, quotas.NewClientFromConfig(cfg), errOut, w, cfg.Region)
	roleCheck, err := checkInstanceRoleAuthorizedFromConfig(ctx, cfg, errOut, w)
	if err != nil {
		return nil, nil, err
	}
	return quotaReport, roleCheck, nil
}
