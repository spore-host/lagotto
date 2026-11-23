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
// watch (and watch --local) and launch mark lagotto active in region, and
// then in the global scope (lagotto#345), before they create anything, so a
// teardown holding either lease is waited out and none can start deleting
// until a grace period after this.
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
	if err != nil {
		return err
	}
	return markGlobalWith(ctx, c, what, errOut)
}

// markGlobal marks lagotto's global scope active (lagotto#345) before a
// command creates or changes anything account-wide: setup's IAM, and
// deploy, watch and launch, which use it. The CLI's account-wide teardown
// (idle.TearDownAccountWide) holds the global lease while it deletes, so a
// creator waits it out; one that marks after its decision moves the global
// item's epoch, which stops the teardown before its next delete, and starts
// the global grace period over.
func markGlobal(ctx context.Context, cfg aws.Config, owner, what string, errOut io.Writer) error {
	if owner == "" {
		owner = "lagotto-cli"
	}
	c, err := newLifecycleClient(cfg, owner)
	if err != nil {
		return fmt.Errorf("refusing to %s: %w. Nothing was created", what, err)
	}
	return markGlobalWith(ctx, c, what, errOut)
}

func markGlobalWith(ctx context.Context, c *lifecycle.Client, what string, errOut io.Writer) error {
	if _, err := c.RegionFor(lifecycle.ScopeGlobal); err != nil {
		return nil // no global scope outside the aws partition (lagotto#347): nothing removes the account-wide IAM there
	}
	err := idle.MarkActive(ctx, c, lifecycle.ScopeGlobal, what)
	if errors.Is(err, idle.ErrLeaseUnavailable) {
		_, _ = fmt.Fprintln(errOut, idle.LeaseWarning(err))
		return nil
	}
	return err
}
