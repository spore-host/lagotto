package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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

// errFleetNeedsRegions refuses a --maintain fleet with no --regions (#275). A
// fleet counts its running workers in each region it names, and with none it
// can't count them, so the poller could never top it up. Resolving "every
// enabled region" at poll time instead would mean a DescribeInstances in every
// enabled region on every cycle, for every fleet, and workers scattered across
// regions away from their data; the --require-quota gate (#157) already treats
// a fleet with no regions as unknown for the same reason.
var errFleetNeedsRegions = errors.New("--maintain needs --regions: a fleet counts its running workers in each region it names, " +
	"so it can't be maintained across every enabled region; name the region(s) to launch in with --regions")

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

// validateFleetRegions refuses a fleet (maintain > 0) with no regions (#275).
// Blank entries don't count: `--regions " "` names no region.
func validateFleetRegions(maintain int, regions []string) error {
	if maintain > 0 && len(cleanList(regions)) == 0 {
		return errFleetNeedsRegions
	}
	return nil
}

// validateWatchSearch checks the stored shape of w, with no AWS call: its
// pattern fits its service, an EC2 spawn watch has a spawn config, a fleet's
// --maintain/--until are consistent and it names its --regions, and an until-landed watch can still tell
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
	if err := validateFleetRegions(w.DesiredCount, w.Regions); err != nil {
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
//     the account's vCPU quota. NON-FATAL by default: truffle's GetQuotas can't
//     always read a quota, and "zero quota" must never be confused with "lookup
//     failed / no permission / throttled";
//   - with requireQuota (--require-quota, #157), the opt-in gate: refuses a
//     fleet whose quota was READ in every region it names and provably can't
//     hold --maintain N. An unreadable quota is unknown, never zero: it is
//     reported and the watch is still created (see enforceFleetQuota);
//   - the instance-role authorization check (#170). Unlike the quota warning it
//     CAN be fatal, because it refuses only on direct evidence from the
//     deployed policy (see checkInstanceRoleAuthorized), so a watch that
//     provably cannot launch is never stored to wait for capacity it can't use.
//
// The quota report, the gate and the instance-role verdict are returned for the
// JSON output. Single-shot quota refusals (#238) are found by the poller, at
// launch, and have no create-time check.
func preflightWatch(ctx context.Context, cfg aws.Config, errOut io.Writer, w *watcher.Watch, requireQuota bool) (preflightResult, error) {
	var res preflightResult
	if _, err := checkWatchSubnetCoverage(ctx, newSubnetCoverageChecker(cfg), errOut, w); err != nil {
		return res, err
	}
	ql := newQuotaLookup(cfg)
	res.Quota = warnFleetQuotaFeasibility(ctx, ql, errOut, w, cfg.Region)
	if requireQuota {
		gate, err := enforceFleetQuota(ctx, ql, errOut, w)
		if err != nil {
			return res, err
		}
		res.Gate = gate
	}
	roleCheck, err := checkInstanceRoleAuthorizedFromConfig(ctx, cfg, errOut, w)
	if err != nil {
		return res, err
	}
	res.Role = roleCheck
	return res, nil
}

// newQuotaLookup builds the Service Quotas reader the preflight uses (#153,
// #157). A var so a test can inject a fake: quotas are AWS-side and change over
// time, so no test may depend on a real account's numbers.
var newQuotaLookup = func(cfg aws.Config) quotacheck.Lookup { return quotas.NewClientFromConfig(cfg) }

// preflightResult is what preflightWatch found, for the JSON output.
type preflightResult struct {
	Quota *quotacheck.Report
	Gate  *quotacheck.Gate
	Role  *InstanceRoleCheck
}

// errRequireQuotaNeedsFleet refuses --require-quota on a watch with no
// --maintain goal: there is no worker count to check the quota against.
var errRequireQuotaNeedsFleet = errors.New("--require-quota applies only to a fleet watch (--maintain N)")

// ExitQuotaInfeasible is the exit status of a watch or update refused by
// --require-quota (#157), distinct from the generic 1 so a pipeline can tell
// "the quota is provably too low" from any other failure.
const ExitQuotaInfeasible = 3

// quotaGateError is the --require-quota refusal. It carries its own exit
// status (see Execute).
type quotaGateError struct{ gate *quotacheck.Gate }

func (e *quotaGateError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "--require-quota: refusing the watch: %s", e.gate.Reason)
	for _, r := range e.gate.Regions {
		fmt.Fprintf(&b, "\n  %s: %d vCPU limit, %d in use, room for %d worker(s)", r.Region, r.Limit, r.Used, r.MaxWorkers)
	}
	if e.gate.Remediation != "" {
		fmt.Fprintf(&b, "\n%s", e.gate.Remediation)
	}
	b.WriteString("\nNothing was stored. Without --require-quota the watch is created and maintains as many workers as the quota allows.")
	return b.String()
}

// ExitCode is the process exit status for this error.
func (e *quotaGateError) ExitCode() int { return ExitQuotaInfeasible }

// quotaGatePerRegionTimeout bounds each region's quota read under
// --require-quota. A region that doesn't answer in time is unknown, not short.
const quotaGatePerRegionTimeout = 5 * time.Second

// enforceFleetQuota is the --require-quota gate (#157). It checks EVERY region
// the watch names (no maxQuotaCheckRegions truncation: the caller asked to be
// sure, and an unchecked region would make the answer unknown anyway), and:
//
//   - feasible: returns the gate, silently;
//   - unknown: prints a note saying the quota could not be proven short and the
//     watch is created anyway (unknown is not zero), and returns the gate;
//   - infeasible: returns a *quotaGateError, so nothing is stored.
func enforceFleetQuota(ctx context.Context, l quotacheck.Lookup, out io.Writer, w *watcher.Watch) (*quotacheck.Gate, error) {
	if w == nil || w.DesiredCount <= 0 {
		return nil, errRequireQuotaNeedsFleet
	}
	rungs := watcher.SplitInstanceTypePatterns(w.InstanceTypePattern)
	g := quotacheck.RequireFeasible(ctx, l, w.Regions, rungs, w.Spot, w.DesiredCount, quotaGatePerRegionTimeout)
	switch g.Verdict {
	case quotacheck.GateInfeasible:
		return &g, &quotaGateError{gate: &g}
	case quotacheck.GateUnknown:
		_, _ = fmt.Fprintf(out, "note: --require-quota could not prove the quota short, so the watch is created anyway (an unreadable quota is unknown, not zero): %s.\n", g.Reason)
		for _, r := range g.Regions {
			if !r.Known {
				_, _ = fmt.Fprintf(out, "  %s: %s\n", r.Region, r.Unknown)
			}
		}
	}
	return &g, nil
}
