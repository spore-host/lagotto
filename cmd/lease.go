package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/libs/lifecycle"

	"github.com/spore-host/lagotto/pkg/idle"
)

// newLifecycleClient builds the spore-host-lifecycle client a command leases
// with (lagotto#326). A seam: tests answer it from lifecycletest.
var newLifecycleClient = func(cfg aws.Config, owner string) (*lifecycle.Client, error) {
	return idle.NewClient(cfg, owner)
}

// markActive is every creating command's first write (lagotto#326): deploy,
// watch (and watch --local) and launch mark lagotto active in region before
// they create anything, so a teardown holding the lease there is waited out
// and none can start deleting until a grace period after this.
//
// A teardown that keeps the lease for the whole wait refuses the command
// (non-zero exit, nothing created). A caller whose IAM predates the lease
// table is warned on errOut and carries on, as before lagotto#326.
func markActive(ctx context.Context, cfg aws.Config, owner, region, what string, errOut io.Writer) error {
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	err = idle.MarkActive(ctx, c, region, what)
	if errors.Is(err, idle.ErrLeaseUnavailable) {
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
		return nil
	}
	return err
}
