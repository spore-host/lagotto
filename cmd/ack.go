package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var ackCmd = &cobra.Command{
	Use:   "ack <watch-id>",
	Short: "Acknowledge an --until-landed watch so it keeps hunting",
	Long: `Acknowledge an --until-landed watch.

An --until-landed watch has no TTL. Instead it pauses itself unless you
acknowledge it: its deadline is (2+1) × its --remind interval (21 days by
default) from creation or the last acknowledgement. ack moves the deadline a
full window ahead from now. A paused watch (status expired, reason
unacknowledged) is resumed and hunts again.

Only ack and extend count as an acknowledgement; list and status do not.`,
	Args: cobra.ExactArgs(1),
	// A refused ack (not an until-landed watch) is a finding, not a usage error.
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return extendOrAck(cmd, args[0], ackExpiry)
	},
}

func init() {
	rootCmd.AddCommand(ackCmd)
}

// ackExpiry is the deadline an acknowledgement at now sets (#239). A watch with
// a TTL has no deadline to acknowledge; extend is its command.
func ackExpiry(w *watcher.Watch, now time.Time) (time.Time, error) {
	d, ok := w.AckDeadline(now)
	if !ok {
		return time.Time{}, fmt.Errorf("watch %s is not an --until-landed watch, so there is nothing to acknowledge; "+
			"use 'lagotto extend %s --ttl <duration>' to move its expiry", w.WatchID, w.WatchID)
	}
	return d, nil
}
