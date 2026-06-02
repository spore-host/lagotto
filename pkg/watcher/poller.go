package watcher

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// Poller checks instance capacity for active watches.
type Poller struct {
	truffle   *truffleaws.Client
	store     *Store
	notifier  *Notifier          // nil = skip notifications
	spawner   *Spawner           // nil = skip auto-spawn
	holder    *Holder            // nil = skip capacity reservations
	sagemaker *SageMakerLauncher // nil = skip SageMaker job submission
	verbose   bool
}

// PollerOpts configures optional Poller dependencies.
type PollerOpts struct {
	Notifier  *Notifier
	Spawner   *Spawner
	Holder    *Holder
	SageMaker *SageMakerLauncher
}

// NewPoller creates a Poller backed by a truffle client and DynamoDB store.
func NewPoller(truffle *truffleaws.Client, store *Store, verbose bool, opts ...PollerOpts) *Poller {
	p := &Poller{
		truffle: truffle,
		store:   store,
		verbose: verbose,
	}
	if len(opts) > 0 {
		p.notifier = opts[0].Notifier
		p.spawner = opts[0].Spawner
		p.holder = opts[0].Holder
		p.sagemaker = opts[0].SageMaker
	}
	return p
}

// PollSummary reports the outcome of one account-wide sweep by watch-state
// transition. The poller is a stateless per-account singleton: it holds no
// per-attempt or retry state, so a cycle's meaning lives entirely in how each
// watch's status changed. Retrying watches stay active and are swept again next
// cycle; the watch TTL is the only stopping limit besides a launch or a terminal
// failure.
type PollSummary struct {
	Watched  int           `json:"watched"`  // active, non-expired watches polled
	Launched int           `json:"launched"` // spawn/hold succeeded → matched
	Notified int           `json:"notified"` // notify-only match → matched
	Retrying int           `json:"retrying"` // capacity unavailable on launch → still active
	Failed   int           `json:"failed"`   // terminal failure → failed
	Expired  int           `json:"expired"`  // TTL elapsed this cycle → expired
	Matches  []MatchResult `json:"matches"`  // launched + notified events
}

// Total returns the number of watches the sweep accounted for.
func (s PollSummary) Total() int { return s.Watched + s.Expired }

// PollAll loads all active watches, expires any past their TTL, and polls the
// rest. Returns a summary of state transitions for the cycle.
func (p *Poller) PollAll(ctx context.Context) (*PollSummary, error) {
	watches, err := p.store.ListActiveWatches(ctx)
	if err != nil {
		return nil, fmt.Errorf("load active watches: %w", err)
	}
	summary := &PollSummary{}
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Polling %d active watches\n", len(watches))
	}

	// EC2 watches are grouped by region+pattern+spot so they share one truffle
	// capacity search. SageMaker watches don't use truffle at all — each submits
	// the user's job directly — so they're handled individually.
	type regionKey struct {
		regions string // sorted, joined
		pattern string
		spot    bool
	}
	type regionGroup struct {
		regions []string
		pattern string
		spot    bool
		watches []*Watch
	}

	// The watch TTL is the sole stopping limit (besides a successful launch or a
	// terminal failure). DynamoDB TTL deletion is lazy — up to ~48h late — so the
	// poller enforces ExpiresAt itself: a past-TTL watch is expired now, not
	// retried, so capacity-failing watches don't keep launching past their TTL.
	now := time.Now().UTC()
	groups := make(map[regionKey]*regionGroup)
	for i := range watches {
		w := &watches[i]
		if !w.ExpiresAt.IsZero() && now.After(w.ExpiresAt) {
			if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusExpired); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to expire watch %s: %v\n", w.WatchID, err)
			} else {
				summary.Expired++
				if p.verbose {
					fmt.Fprintf(os.Stderr, "Watch %s expired (TTL %s elapsed)\n", w.WatchID, w.ExpiresAt.Format(time.RFC3339))
				}
			}
			continue
		}
		summary.Watched++

		// SageMaker: submit/track the user's job directly, no EC2 search.
		if normalizeService(w.Service) == ServiceSageMaker {
			p.pollSageMakerWatch(ctx, w, summary)
			continue
		}

		key := regionKey{
			regions: strings.Join(w.Regions, ","),
			pattern: w.InstanceTypePattern,
			spot:    w.Spot,
		}
		if g, ok := groups[key]; ok {
			g.watches = append(g.watches, w)
		} else {
			groups[key] = &regionGroup{
				regions: w.Regions,
				pattern: w.InstanceTypePattern,
				spot:    w.Spot,
				watches: []*Watch{w},
			}
		}
	}

	for _, g := range groups {
		if err := p.pollGroup(ctx, g.regions, g.pattern, g.spot, g.watches, summary); err != nil {
			// Log but don't fail the entire poll cycle
			fmt.Fprintf(os.Stderr, "Warning: poll failed for pattern %q: %v\n", g.pattern, err)
			continue
		}
	}

	return summary, nil
}

// PollWatch runs a single poll cycle for one watch and returns the resulting
// match events (launched or notified). Useful for testing.
func (p *Poller) PollWatch(ctx context.Context, w *Watch) ([]MatchResult, error) {
	summary := &PollSummary{}
	if normalizeService(w.Service) == ServiceSageMaker {
		p.pollSageMakerWatch(ctx, w, summary)
		return summary.Matches, nil
	}
	if err := p.pollGroup(ctx, w.Regions, w.InstanceTypePattern, w.Spot, []*Watch{w}, summary); err != nil {
		return nil, err
	}
	return summary.Matches, nil
}

// pollSageMakerWatch submits/tracks the user's SageMaker job for one watch and
// records the resulting state transition, mirroring the EC2 capacity→retry,
// terminal→fail, success→matched semantics.
func (p *Poller) pollSageMakerWatch(ctx context.Context, w *Watch, summary *PollSummary) {
	now := time.Now().UTC()
	m := &MatchResult{
		WatchID:      w.WatchID,
		UserID:       w.UserID,
		Service:      ServiceSageMaker,
		InstanceType: w.InstanceTypePattern,
		MatchedAt:    now,
	}

	// notify-only SageMaker watches don't submit a job — there's nothing to do
	// here beyond marking polled (a real capacity signal would require a launch).
	if w.Action == ActionNotify || p.sagemaker == nil {
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		summary.Retrying++
		return
	}

	outcome, err := p.sagemaker.Launch(ctx, w, m)
	if err != nil && p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s (sagemaker, %s): %v\n", w.WatchID, failureLabel(outcome.Kind), err)
	}

	// Persist the in-flight job name (set on submit, cleared on capacity failure).
	if outcome.ClearJobName {
		_ = p.store.UpdateSageMakerJob(ctx, w.WatchID, "")
	} else if outcome.JobName != "" && outcome.JobName != w.SageMakerJobName {
		_ = p.store.UpdateSageMakerJob(ctx, w.WatchID, outcome.JobName)
	}

	p.recordOutcome(ctx, w, m, outcome.Kind, summary)
}

// recordOutcome applies the shared capacity/terminal/success state machine: a
// capacity failure keeps the watch active (retry next cycle); a terminal failure
// stops it as failed; success records the match, notifies, and marks matched.
func (p *Poller) recordOutcome(ctx context.Context, w *Watch, m *MatchResult, failure FailureKind, summary *PollSummary) {
	if failure == FailureCapacity {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: capacity unavailable; will retry next cycle\n", w.WatchID)
		}
		summary.Retrying++
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	if err := p.store.RecordMatch(ctx, w, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record match for %s: %v\n", w.WatchID, err)
	}
	if p.notifier != nil {
		if err := p.notifier.Notify(ctx, w, m); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: notification failed for %s: %v\n", w.WatchID, err)
		}
	}

	endStatus := StatusMatched
	if failure == FailureTerminal {
		endStatus = StatusFailed
		summary.Failed++
	} else if m.ActionTaken == "spawned" || m.ActionTaken == "held" || m.ActionTaken == "sagemaker_launched" {
		summary.Launched++
	} else {
		summary.Notified++
	}
	if err := p.store.UpdateWatchStatus(ctx, w.WatchID, endStatus); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to update watch status for %s: %v\n", w.WatchID, err)
	}
	summary.Matches = append(summary.Matches, *m)
}

func (p *Poller) pollGroup(ctx context.Context, regions []string, pattern string, spot bool, watches []*Watch, summary *PollSummary) error {
	// Convert pattern to regex (support wildcards like "p5.*")
	regexPattern := wildcardToRegex(pattern)
	matcher, err := regexp.Compile(regexPattern)
	if err != nil {
		return fmt.Errorf("compile pattern %q: %w", pattern, err)
	}

	if p.verbose {
		fmt.Fprintf(os.Stderr, "Searching for %q across %d regions (spot=%v)\n", pattern, len(regions), spot)
	}

	// Search for instance types
	results, err := p.truffle.SearchInstanceTypes(ctx, regions, matcher, truffleaws.FilterOptions{
		IncludeAZs: true,
		Verbose:    p.verbose,
	})
	if err != nil {
		return fmt.Errorf("search instance types: %w", err)
	}

	if len(results) == 0 {
		// Pre-filter found nothing offered; nothing to attempt. Watches stay
		// active and are retried next cycle.
		for _, w := range watches {
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		}
		return nil
	}

	// Get Spot pricing if needed
	var spotResults []truffleaws.SpotPriceResult
	if spot {
		spotResults, err = p.truffle.GetSpotPricing(ctx, results, truffleaws.SpotOptions{
			OnlyActive: true,
			Verbose:    p.verbose,
		})
		if err != nil {
			return fmt.Errorf("get spot pricing: %w", err)
		}
	}

	// Evaluate each watch against the results
	now := time.Now().UTC()

	for _, w := range watches {
		var bestMatch *MatchResult

		if spot && len(spotResults) > 0 {
			// Evaluate Spot results
			for i := range spotResults {
				candidate := MatchCandidate{SpotPrice: &spotResults[i]}
				// Find the corresponding InstanceTypeResult
				for j := range results {
					if results[j].InstanceType == spotResults[i].InstanceType && results[j].Region == spotResults[i].Region {
						candidate.InstanceType = results[j]
						break
					}
				}
				if m := Evaluate(w, candidate); m != nil {
					m.MatchedAt = now
					if bestMatch == nil || m.Price < bestMatch.Price {
						bestMatch = m
					}
				}
			}
		} else if !spot {
			// Evaluate On-Demand results
			for i := range results {
				candidate := MatchCandidate{InstanceType: results[i]}
				if m := Evaluate(w, candidate); m != nil {
					m.MatchedAt = now
					if bestMatch == nil || m.Price < bestMatch.Price {
						bestMatch = m
					}
				}
			}
		}

		if bestMatch != nil {
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Match found for watch %s: %s in %s at $%.4f/hr\n",
					w.WatchID, bestMatch.InstanceType, bestMatch.Region, bestMatch.Price)
			}

			// Execute the action. The launch IS the capacity test: cheap signals
			// (offerings, spot price) only decide it's worth attempting — they
			// never prove capacity. So a capacity failure here is expected and
			// must be retried, not treated as terminal.
			failure := FailureNone
			switch w.Action {
			case ActionSpawn:
				if p.spawner != nil {
					if err := p.spawner.Spawn(ctx, w, bestMatch); err != nil {
						failure = ClassifyFailure(err)
						fmt.Fprintf(os.Stderr, "Warning: auto-spawn failed for %s (%s): %v\n", w.WatchID, failureLabel(failure), err)
						bestMatch.ActionTaken = "spawn_failed"
					}
				} else {
					bestMatch.ActionTaken = "notified"
				}
			case ActionHold:
				if p.holder != nil {
					if err := p.holder.Hold(ctx, w, bestMatch); err != nil {
						failure = ClassifyFailure(err)
						fmt.Fprintf(os.Stderr, "Warning: hold failed for %s (%s): %v\n", w.WatchID, failureLabel(failure), err)
						bestMatch.ActionTaken = "hold_failed"
					}
				} else {
					bestMatch.ActionTaken = "notified"
				}
			default:
				bestMatch.ActionTaken = "notified"
			}

			p.recordOutcome(ctx, w, bestMatch, failure, summary)
		} else {
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		}
	}

	return nil
}

// wildcardToRegex converts shell-style wildcards to regex.
// e.g., "p5.*" -> "^p5\..*$", "g5.xlarge" -> "^g5\.xlarge$"
func wildcardToRegex(pattern string) string {
	// If it already looks like a regex, use as-is
	if strings.ContainsAny(pattern, "^$()[]{}+\\") {
		return pattern
	}
	// Escape dots, convert * to .*
	escaped := strings.ReplaceAll(pattern, ".", "\\.")
	escaped = strings.ReplaceAll(escaped, "*", ".*")
	return "^" + escaped + "$"
}
