package cmd

// Finding stored launch configs the poller will refuse (#309): see
// pkg/watcher/storedconfig.go. `lagotto list` flags the caller's own active
// watches; `lagotto doctor` flags every armed watch and scheduled launch in
// the region.

import (
	"context"
	"fmt"
	"io"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// roleCheck reports what the hosted poller lacks to launch with role, and
// whether that is known. Unknown (no poller policy, or it could not be read)
// is never reported as a problem.
type roleCheck func(role string) (missing []string, known bool)

// newRoleCheck reads the deployed poller's runtime policy on first use, once:
// only a config that names an iam_role needs it. (No sync.Once: the IAM
// inventory resolves a func handed to Once.Do as reachable from every Do in
// the binary, startup included.)
func newRoleCheck(ctx context.Context, client runtimeiam.IAMAPI, accountID string) roleCheck {
	var (
		loaded bool
		grants []runtimeiam.Grant
		ok     bool
	)
	return func(role string) ([]string, bool) {
		if !loaded {
			loaded = true
			grants, ok = readRuntimeGrants(ctx, client, accountID)
		}
		if !ok {
			return nil, false
		}
		return runtimeiam.MissingInstanceRoleGrants(grants, accountID, role), true
	}
}

// readRuntimeGrants reads and parses the deployed poller's runtime policy;
// ok is false when there is none or it cannot be read.
func readRuntimeGrants(ctx context.Context, client runtimeiam.IAMAPI, accountID string) ([]runtimeiam.Grant, bool) {
	if client == nil || accountID == "" {
		return nil, false
	}
	doc, err := runtimeiam.ReadRuntimePolicy(ctx, client)
	if err != nil {
		return nil, false
	}
	g, err := runtimeiam.ParsePolicy(doc)
	return g, err == nil
}

// storedConfigFinding is the problem with one stored launch config, or nil.
func storedConfigFinding(id string, raw []byte, roles roleCheck) *watcher.LaunchConfigFinding {
	if f := watcher.CheckStoredLaunchConfig(id, raw); f != nil {
		return f
	}
	role := watcher.StoredConfigRole(raw)
	if role == "" || roles == nil {
		return nil
	}
	if missing, known := roles(role); known && len(missing) > 0 {
		return watcher.RoleUnauthorizedFinding(id, role, missing)
	}
	return nil
}

// watchLaunchesConfig reports whether the poller launches w from its stored
// spawn config: an active EC2 spawn watch.
func watchLaunchesConfig(w *watcher.Watch) bool {
	return w.Status == watcher.StatusActive && w.Action == watcher.ActionSpawn &&
		w.Service != watcher.ServiceSageMaker && len(w.LaunchConfigJSON) > 0
}

// launchConfigFindings checks every active spawn watch and armed scheduled
// launch, in order.
func launchConfigFindings(watches []watcher.Watch, launches []watcher.ScheduledLaunch, roles roleCheck) []watcher.LaunchConfigFinding {
	var out []watcher.LaunchConfigFinding
	for i := range watches {
		if w := &watches[i]; watchLaunchesConfig(w) {
			if f := storedConfigFinding(w.WatchID, w.LaunchConfigJSON, roles); f != nil {
				out = append(out, *f)
			}
		}
	}
	for i := range launches {
		if sl := &launches[i]; sl.Armed() {
			if f := storedConfigFinding(sl.ScheduleID, sl.LaunchConfigJSON, roles); f != nil {
				out = append(out, *f)
			}
		}
	}
	return out
}

// annotateLaunchConfigProblems sets LaunchConfigProblem on each of the
// caller's active spawn watches the poller will refuse (`lagotto list`).
func annotateLaunchConfigProblems(watches []watcher.Watch, roles roleCheck) {
	for i := range watches {
		if w := &watches[i]; watchLaunchesConfig(w) {
			w.LaunchConfigProblem = storedConfigFinding(w.WatchID, w.LaunchConfigJSON, roles)
		}
	}
}

// writeLaunchConfigProblems prints, below `lagotto list`'s table, every watch
// the poller will refuse to launch, with the fix.
func writeLaunchConfigProblems(out io.Writer, watches []watcher.Watch) {
	header := false
	for i := range watches {
		f := watches[i].LaunchConfigProblem
		if f == nil {
			continue
		}
		if !header {
			_, _ = fmt.Fprintln(out, "\nThe poller will REFUSE to launch these watches when they match (they stop as failed, launching nothing):")
			header = true
		}
		for _, l := range f.Lines() {
			_, _ = fmt.Fprintf(out, "  %s\n", l)
		}
	}
}
