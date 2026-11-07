package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// checkWatchSubnetCoverage is the create-time (and, for #257, update-time)
// default-subnet check (#256). For an EC2 spawn or hold watch it refuses a
// watch that can never land in a region it names (no AZ there both offers the
// type and has a default subnet), naming the AZs and the create-default-subnet
// command, and warns on out about offered AZs it can't land in. Other watches
// pass untouched. A lookup it can't make is a warning, never a refusal.
//
// Call it before the watch is stored, so a refused watch leaves nothing behind.
func checkWatchSubnetCoverage(ctx context.Context, checker *watcher.SubnetCoverageChecker, out io.Writer, w *watcher.Watch) (*watcher.SubnetCoverage, error) {
	cov := checker.Check(ctx, w)
	if err := cov.Refusal(); err != nil {
		return cov, err
	}
	for _, line := range cov.Warnings() {
		_, _ = fmt.Fprintf(out, "Warning: %s\n", line)
	}
	return cov, nil
}
