package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

var historyWatchID string

var historyCmd = &cobra.Command{
	Use:   "history",
	Short: "Show match history",
	RunE:  runHistory,
}

func init() {
	rootCmd.AddCommand(historyCmd)
	historyCmd.Flags().StringVar(&historyWatchID, "watch-id", "", "Filter by watch ID")
}

func runHistory(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	var matches []watcher.MatchResult

	// created maps a watch ID to its creation time, so each match row can surface
	// the derived wait-to-acquire (matched_at − created_at, #139) — MatchResult
	// itself doesn't carry the watch's created_at, so we join it here.
	created := map[string]time.Time{}

	// gaveUp collects watches that ended WITHOUT ever acquiring (#161). They have
	// no match record by definition — that's the whole point — so they can't come
	// from the history table; we read them off the watch records we already fetch
	// for the created_at join, at no extra AWS call.
	var gaveUp []watcher.Watch

	if historyWatchID != "" {
		// Authorize: only the watch's owner may read its history by ID (#41).
		w, oerr := getWatchOwned(ctx, store, sts.NewFromConfig(cfg), historyWatchID)
		if oerr != nil {
			return oerr
		}
		created[w.WatchID] = w.CreatedAt
		if gaveUpWithoutAcquiring(w) {
			gaveUp = append(gaveUp, *w)
		}
		matches, err = store.ListMatchHistory(ctx, historyWatchID)
	} else {
		// Get user's history
		stsClient := sts.NewFromConfig(cfg)
		identity, err2 := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err2 != nil {
			return fmt.Errorf("get caller identity: %w", err2)
		}
		// Fetch the caller's watches (all statuses) to learn each watch's
		// created_at for the wait-to-acquire join (#139).
		if watches, werr := store.ListWatchesByUser(ctx, *identity.Arn, ""); werr == nil {
			for i := range watches {
				created[watches[i].WatchID] = watches[i].CreatedAt
				if gaveUpWithoutAcquiring(&watches[i]) {
					gaveUp = append(gaveUp, watches[i])
				}
			}
		}
		matches, err = store.ListMatchHistoryByUser(ctx, *identity.Arn)
	}
	if err != nil {
		return fmt.Errorf("list history: %w", err)
	}

	// Fill each row's derived wait-to-acquire from its watch's created_at (#139),
	// where we know it and the delta is sane (non-negative).
	for i := range matches {
		if c, ok := created[matches[i].WatchID]; ok && !c.IsZero() && !matches[i].MatchedAt.IsZero() {
			if d := matches[i].MatchedAt.Sub(c); d >= 0 {
				s := d.Seconds()
				matches[i].WaitToAcquireSeconds = &s
			}
		}
	}

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(historyEntries(matches, gaveUp))
	}

	return renderHistory(cmd.OutOrStdout(), matches, gaveUp)
}

// gaveUpWithoutAcquiring reports whether a watch ended without ever getting
// capacity — an expired or terminally-failed watch that never recorded a match
// (#161). A watch that matched and was later retired isn't a give-up: its outcome
// is already in the match history.
func gaveUpWithoutAcquiring(w *watcher.Watch) bool {
	if w == nil || w.MatchCount > 0 {
		return false
	}
	_, ok := w.TimeToGiveUp()
	return ok
}

// matchEntry is a match row in the JSON output. Embedding MatchResult keeps the
// shape every existing consumer already parses; the only change is the additive
// "event" discriminator that tells it apart from a gave-up row.
type matchEntry struct {
	watcher.MatchResult
	Event string `json:"event"`
}

// gaveUpEntry is a watch that ended without ever acquiring (#161). Deliberately
// its OWN shape rather than a synthesized MatchResult: "never acquired" is the
// absence of a match, and faking one would invite a consumer to read it as an
// acquisition of the pattern at price 0.
type gaveUpEntry struct {
	Event               string              `json:"event"`
	WatchID             string              `json:"watch_id"`
	UserID              string              `json:"user_id,omitempty"`
	Project             string              `json:"project,omitempty"`
	Status              watcher.WatchStatus `json:"status"`
	InstanceTypePattern string              `json:"instance_type_pattern"`
	Regions             []string            `json:"regions,omitempty"`
	Action              watcher.ActionMode  `json:"action,omitempty"`
	CreatedAt           time.Time           `json:"created_at"`
	EndedAt             time.Time           `json:"ended_at"`
	TimeToGiveUpSeconds *float64            `json:"time_to_give_up_seconds,omitempty"`
}

// historyEntries builds the flat JSON array: match rows first (unchanged shape
// plus "event":"match"), then the gave-up rows.
func historyEntries(matches []watcher.MatchResult, gaveUp []watcher.Watch) []any {
	entries := make([]any, 0, len(matches)+len(gaveUp))
	for _, m := range matches {
		entries = append(entries, matchEntry{MatchResult: m, Event: "match"})
	}
	for i := range gaveUp {
		w := &gaveUp[i]
		e := gaveUpEntry{
			Event:               "gave_up",
			WatchID:             w.WatchID,
			UserID:              w.UserID,
			Project:             w.Project,
			Status:              w.Status,
			InstanceTypePattern: w.InstanceTypePattern,
			Regions:             w.Regions,
			Action:              w.Action,
			CreatedAt:           w.CreatedAt,
			EndedAt:             w.UpdatedAt,
		}
		if d, ok := w.TimeToGiveUp(); ok {
			s := d.Seconds()
			e.TimeToGiveUpSeconds = &s
		}
		entries = append(entries, e)
	}
	return entries
}

// renderHistory writes the human table: matches, then a clearly separated
// gave-up section (#161). Pure over its inputs (no AWS), so it's unit-testable.
func renderHistory(out io.Writer, matches []watcher.MatchResult, gaveUp []watcher.Watch) error {
	if len(matches) == 0 && len(gaveUp) == 0 {
		fmt.Fprintln(out, "No match history found.")
		return nil
	}

	if len(matches) > 0 {
		fmt.Fprintf(out, "%-12s %-18s %-15s %-15s %-10s %-10s %-25s %s\n",
			"WATCH ID", "INSTANCE TYPE", "REGION", "AZ", "PRICE", "ACTION", "MATCHED AT", "WAIT")
		for _, m := range matches {
			wait := "-"
			if m.WaitToAcquireSeconds != nil {
				wait = watcher.FormatWait(time.Duration(*m.WaitToAcquireSeconds * float64(time.Second)))
			}
			fmt.Fprintf(out, "%-12s %-18s %-15s %-15s $%-9.4f %-10s %-25s %s\n",
				m.WatchID,
				m.InstanceType,
				m.Region,
				m.AvailabilityZone,
				m.Price,
				m.ActionTaken,
				m.MatchedAt.Format(time.RFC3339),
				wait,
			)
		}
	}

	if len(gaveUp) == 0 {
		return nil
	}

	if len(matches) > 0 {
		fmt.Fprintln(out)
	}
	// The other half of the wait distribution (#139/#161): watches that waited and
	// never got capacity. Reported as GAVE UP with the elapsed watch duration, so
	// "we hunted g7e for 48h across 5 regions and never got it" is visible rather
	// than inferred from silence.
	fmt.Fprintln(out, "Gave up without acquiring:")
	fmt.Fprintf(out, "%-12s %-10s %-20s %-25s %-25s %s\n",
		"WATCH ID", "OUTCOME", "PATTERN", "REGIONS", "ENDED AT", "WAITED")
	for i := range gaveUp {
		w := &gaveUp[i]
		waited := "-"
		if d, ok := w.TimeToGiveUp(); ok {
			waited = watcher.FormatWait(d)
		}
		regions := displayRegions(w.Regions)
		if len(regions) > 25 {
			regions = regions[:22] + "..."
		}
		fmt.Fprintf(out, "%-12s %-10s %-20s %-25s %-25s %s\n",
			w.WatchID,
			w.Status,
			truncate(w.InstanceTypePattern, 20),
			regions,
			w.UpdatedAt.Format(time.RFC3339),
			waited,
		)
	}
	return nil
}
