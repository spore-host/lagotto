package cmd

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/lagotto/pkg/arm"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// warnIfPollerPredatesReminders prints the until-landed reminder-poller warning
// for w (an active until-landed watch) to errOut when the hosted poller in this
// region is too old. The decision and its text are arm.CheckReminderPoller
// (#282), shared with every non-CLI watch creator; this is the CLI's printing.
func warnIfPollerPredatesReminders(ctx context.Context, cfg aws.Config, errOut io.Writer, w *watcher.Watch) {
	if msg := arm.CheckReminderPoller(ctx, deploy.New(cfg), cfg.Region, w, ""); msg != "" {
		_, _ = fmt.Fprintln(errOut, msg)
	}
}
