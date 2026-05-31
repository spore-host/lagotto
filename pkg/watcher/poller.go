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
	truffle  *truffleaws.Client
	store    *Store
	notifier *Notifier // nil = skip notifications
	spawner  *Spawner  // nil = skip auto-spawn
	holder   *Holder   // nil = skip capacity reservations
	verbose  bool
}

// PollerOpts configures optional Poller dependencies.
type PollerOpts struct {
	Notifier *Notifier
	Spawner  *Spawner
	Holder   *Holder
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

	// Group watches by the effective EC2 search so distinct services that resolve
	// to the same EC2 pattern (e.g. ec2 "g5.*" and sagemaker "ml.g5.*") share one
	// truffle call, and so API calls are deduplicated. The search pattern is the
	// EC2-equivalent of each watch.
	type regionKey struct {
		regions    string // sorted, joined
		ec2Pattern string
		spot       bool
	}
	type regionGroup struct {
		regions    []string
		ec2Pattern string
		spot       bool
		watches    []*Watch
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
		ec2Pattern := EC2EquivalentPattern(w.Service, w.InstanceTypePattern)
		key := regionKey{
			regions:    strings.Join(w.Regions, ","),
			ec2Pattern: ec2Pattern,
			spot:       w.Spot,
		}
		if g, ok := groups[key]; ok {
			g.watches = append(g.watches, w)
		} else {
			groups[key] = &regionGroup{
				regions:    w.Regions,
				ec2Pattern: ec2Pattern,
				spot:       w.Spot,
				watches:    []*Watch{w},
			}
		}
	}

	for _, g := range groups {
		if err := p.pollGroup(ctx, g.regions, g.ec2Pattern, g.spot, g.watches, summary); err != nil {
			// Log but don't fail the entire poll cycle
			fmt.Fprintf(os.Stderr, "Warning: poll failed for pattern %q: %v\n", g.ec2Pattern, err)
			continue
		}
	}

	return summary, nil
}

// PollWatch runs a single poll cycle for one watch and returns the resulting
// match events (launched or notified). Useful for testing.
func (p *Poller) PollWatch(ctx context.Context, w *Watch) ([]MatchResult, error) {
	summary := &PollSummary{}
	if err := p.pollGroup(ctx, w.Regions, EC2EquivalentPattern(w.Service, w.InstanceTypePattern), w.Spot, []*Watch{w}, summary); err != nil {
		return nil, err
	}
	return summary.Matches, nil
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
			// For SageMaker watches the EC2 result is a capacity proxy: relabel
			// the match as the ml.* type and record which EC2 type backed it so
			// notifications make the proxy explicit (#7).
			if normalizeService(w.Service) == ServiceSageMaker {
				bestMatch.Service = ServiceSageMaker
				bestMatch.ProxiedFrom = bestMatch.InstanceType
				bestMatch.InstanceType = sageMakerType(bestMatch.InstanceType)
			}

			if p.verbose {
				fmt.Fprintf(os.Stderr, "Match found for watch %s: %s in %s at $%.4f/hr\n",
					w.WatchID, bestMatch.InstanceType, bestMatch.Region, bestMatch.Price)
			}

			// Execute the action. The launch IS the capacity test: cheap signals
			// (offerings, spot price) only decide it's worth attempting — they
			// never prove capacity. So a capacity failure here is expected and
			// must be retried, not treated as terminal. SageMaker matches are
			// proxies for an ml.* type that EC2 spawn/hold cannot act on, so they
			// force notify regardless of the stored action.
			action := w.Action
			if normalizeService(w.Service) == ServiceSageMaker {
				action = ActionNotify
			}

			failure := FailureNone
			switch action {
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

			// A capacity failure means "no capacity right now" — keep the watch
			// active so the next poll retries (bounded by the watch TTL). Only a
			// success or a terminal failure ends polling.
			if failure == FailureCapacity {
				if p.verbose {
					fmt.Fprintf(os.Stderr, "Watch %s: capacity unavailable on launch; will retry next cycle\n", w.WatchID)
				}
				summary.Retrying++
				_ = p.store.UpdateLastPolled(ctx, w.WatchID)
				continue
			}

			// Record the match/attempt (success or terminal failure).
			if err := p.store.RecordMatch(ctx, w, bestMatch); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to record match for %s: %v\n", w.WatchID, err)
			}

			// Send notifications.
			if p.notifier != nil {
				if err := p.notifier.Notify(ctx, w, bestMatch); err != nil {
					fmt.Fprintf(os.Stderr, "Warning: notification failed for %s: %v\n", w.WatchID, err)
				}
			}

			// Terminal failure stops the watch as failed; otherwise it matched.
			endStatus := StatusMatched
			if failure == FailureTerminal {
				endStatus = StatusFailed
				summary.Failed++
			} else if bestMatch.ActionTaken == "spawned" || bestMatch.ActionTaken == "held" {
				summary.Launched++
			} else {
				summary.Notified++
			}
			if err := p.store.UpdateWatchStatus(ctx, w.WatchID, endStatus); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to update watch status for %s: %v\n", w.WatchID, err)
			}
			summary.Matches = append(summary.Matches, *bestMatch)
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
