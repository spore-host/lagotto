package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// leaseTTL bounds how long a poller's processing lease on a watch is honored
// before it's considered stale and re-claimable (#47). It must comfortably
// exceed a single watch's action time (a RunInstances + FSx-create attempt
// across AZs) but stay well under a poll interval so a crashed poller frees the
// watch within one cycle.
const leaseTTL = 2 * time.Minute

// Poller checks instance capacity for active watches.
type Poller struct {
	truffle   *truffleaws.Client
	store     *Store
	notifier  *Notifier          // nil = skip notifications
	spawner   *Spawner           // nil = skip auto-spawn
	holder    *Holder            // nil = skip capacity reservations
	sagemaker *SageMakerLauncher // nil = skip SageMaker job submission
	verbose   bool
	// filter, when non-nil, scopes which watches this poller services (#47): a
	// local `poll --daemon` can poll only its own project/owner/watch-ids instead
	// of every watch in a shared account. nil = poll all (the hosted Lambda).
	filter *WatchFilter
	// leaseOwner, when set, makes the poller claim a short lease on each watch
	// before acting (#47) so two pollers can't double-fire the same watch. Empty
	// = no leasing (the single hosted Lambda needs none).
	leaseOwner string
	// hosted marks the in-account Lambda poller, which has no shell/sandbox and so
	// refuses shell completion conditions on fleet watches (#70). False = CLI daemon.
	hosted bool
}

// WatchFilter scopes a poll sweep to a subset of watches (#47). A zero-value
// filter (all fields empty) matches every watch — equivalent to no filter.
type WatchFilter struct {
	Project  string   // only watches with this Project label
	Owner    string   // only watches whose UserID equals this (caller ARN)
	WatchIDs []string // only these specific watch IDs
}

// matches reports whether a watch is in scope for this filter. An empty filter
// (or empty field) doesn't constrain on that dimension.
func (f *WatchFilter) matches(w *Watch) bool {
	if f == nil {
		return true
	}
	if f.Project != "" && w.Project != f.Project {
		return false
	}
	if f.Owner != "" && w.UserID != f.Owner {
		return false
	}
	if len(f.WatchIDs) > 0 {
		found := false
		for _, id := range f.WatchIDs {
			if id == w.WatchID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Empty reports whether the filter constrains nothing (matches every watch).
func (f *WatchFilter) Empty() bool {
	return f == nil || (f.Project == "" && f.Owner == "" && len(f.WatchIDs) == 0)
}

// Describe renders the active scope for log output (#47), e.g.
// "project=fieldwork, mine, watches=[w-aaa w-bbb]".
func (f *WatchFilter) Describe() string {
	if f.Empty() {
		return "all account watches"
	}
	var parts []string
	if f.Project != "" {
		parts = append(parts, "project="+f.Project)
	}
	if f.Owner != "" {
		parts = append(parts, "mine")
	}
	if len(f.WatchIDs) > 0 {
		parts = append(parts, fmt.Sprintf("watches=%v", f.WatchIDs))
	}
	return strings.Join(parts, ", ")
}

// PollerOpts configures optional Poller dependencies.
type PollerOpts struct {
	Notifier  *Notifier
	Spawner   *Spawner
	Holder    *Holder
	SageMaker *SageMakerLauncher
	// Filter scopes which watches are serviced (#47); nil = all.
	Filter *WatchFilter
	// LeaseOwner enables the double-poller guard (#47); empty = no leasing.
	LeaseOwner string
	// Hosted marks the in-account Lambda poller (refuses shell completion
	// conditions — no sandbox). False = CLI daemon (#70).
	Hosted bool
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
		p.filter = opts[0].Filter
		p.leaseOwner = opts[0].LeaseOwner
		p.hosted = opts[0].Hosted
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
		// Scope to this poller's filter (#47): a local daemon only services its own
		// project/owner/watch-ids, so it never expires, polls, or launches another
		// project's watch in a shared account. An unscoped poller (hosted Lambda)
		// sees every watch.
		if !p.filter.matches(w) {
			continue
		}
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

		// Goal-driven fleet (#70): maintain ~DesiredCount workers until the
		// completion condition holds — a distinct reconcile, not the single-shot
		// match→act→retire path. Handled per-watch (it does its own search + gap
		// fill and must stay active across cycles).
		if w.DesiredCount > 0 {
			p.pollFleetWatch(ctx, w, summary)
			continue
		}

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

// pollFleetWatch reconciles a goal-driven fleet watch (#70): evaluate the
// completion condition; if met, retire the watch as StatusCompleted; otherwise
// count running workers and (re)launch to fill the gap toward DesiredCount,
// keeping the watch active across cycles (including relaunch from zero after a
// correlated total-loss). Capacity failure just means "topped up less this
// cycle" — the watch stays active and retries next poll.
func (p *Poller) pollFleetWatch(ctx context.Context, w *Watch, summary *PollSummary) {
	if p.spawner == nil {
		// No launcher wired (e.g. notify-only poller) — nothing to maintain.
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	// 1. Completion condition. Shell conditions are CLI-daemon only; the hosted
	// Lambda has no sandbox, so it refuses them (fail loud, don't silently spin).
	if w.CompletionCondition != "" {
		if p.hosted && IsShellCondition(w.CompletionCondition) {
			fmt.Fprintf(os.Stderr, "Watch %s: shell completion conditions are not supported on the hosted poller; skipping\n", w.WatchID)
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			return
		}
		cond, err := ParseCondition(w.CompletionCondition, p.spawner.S3Client())
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: watch %s has an invalid --until %q: %v\n", w.WatchID, w.CompletionCondition, err)
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			return
		}
		done, err := cond.Done(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: watch %s completion check failed: %v (will retry)\n", w.WatchID, err)
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			return
		}
		if done {
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Watch %s: completion condition met; retiring fleet\n", w.WatchID)
			}
			if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusCompleted); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to mark watch %s completed: %v\n", w.WatchID, err)
			}
			return
		}
	}

	// 2. Count the live fleet and compute the gap to DesiredCount.
	running, err := p.spawner.countRunningFleet(ctx, w)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s fleet count failed: %v (will retry)\n", w.WatchID, err)
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}
	gap := w.DesiredCount - running
	if gap <= 0 {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: fleet at %d/%d; nothing to top up\n", w.WatchID, running, w.DesiredCount)
		}
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	// 3. Find capacity, then fill the gap this cycle (launch up to `gap` workers).
	// Lease-guard so two pollers don't both top up the same fleet.
	if p.leaseOwner != "" {
		if err := p.store.ClaimLease(ctx, w.WatchID, p.leaseOwner, time.Now().Add(leaseTTL)); err != nil {
			if errors.Is(err, ErrLeaseHeld) {
				summary.Retrying++
				return
			}
			fmt.Fprintf(os.Stderr, "Warning: could not claim lease on %s: %v\n", w.WatchID, err)
			return
		}
		defer func() {
			if err := p.store.ReleaseLease(ctx, w.WatchID, p.leaseOwner); err != nil && p.verbose {
				fmt.Fprintf(os.Stderr, "Warning: could not release lease on %s: %v\n", w.WatchID, err)
			}
		}()
	}

	bestMatch, err := p.searchBestMatch(ctx, w)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: watch %s capacity search failed: %v\n", w.WatchID, err)
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}
	if bestMatch == nil {
		// No capacity offered this cycle; stay active, retry next poll.
		summary.Retrying++
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	launched := p.fillFleetGap(ctx, w, bestMatch, gap, summary)
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s: topped up %d/%d workers (fleet now ~%d/%d)\n",
			w.WatchID, launched, gap, running+launched, w.DesiredCount)
	}
	// The watch stays active (never flips to matched) — it only retires when the
	// completion condition holds (step 1) or its TTL elapses.
	_ = p.store.UpdateLastPolled(ctx, w.WatchID)
}

// fillFleetGap launches up to `gap` workers into the found capacity, each a
// cloned MatchResult so Spawn stamps a distinct instance. It stops early on the
// first launch failure (capacity ran out mid-fill, or a terminal fault): the
// watch stays active and retries next cycle, so a partial fill is fine — the
// completion condition and TTL bound the watch. Returns how many launched.
func (p *Poller) fillFleetGap(ctx context.Context, w *Watch, bestMatch *MatchResult, gap int, summary *PollSummary) int {
	launched := 0
	for i := 0; i < gap; i++ {
		m := bestMatch.clone()
		if err := p.spawner.Spawn(ctx, w, m); err != nil {
			failure := ClassifyFailure(err)
			fmt.Fprintf(os.Stderr, "Warning: fleet top-up launch %d/%d for %s failed (%s): %v\n",
				i+1, gap, w.WatchID, failureLabel(failure), err)
			break
		}
		launched++
		summary.Launched++
		summary.Matches = append(summary.Matches, *m)
		if err := p.store.RecordMatch(ctx, w, m); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to record fleet match for %s: %v\n", w.WatchID, err)
		}
	}
	return launched
}

// searchBestMatch runs a single-watch truffle capacity search and returns the
// best matching MatchResult, or nil if nothing is offered this cycle. Shared
// search/evaluate logic with pollGroup, scoped to one watch (the fleet path).
func (p *Poller) searchBestMatch(ctx context.Context, w *Watch) (*MatchResult, error) {
	matcher, err := regexp.Compile(wildcardToRegex(w.InstanceTypePattern))
	if err != nil {
		return nil, fmt.Errorf("compile pattern %q: %w", w.InstanceTypePattern, err)
	}
	results, err := p.truffle.SearchInstanceTypes(ctx, w.Regions, matcher, truffleaws.FilterOptions{IncludeAZs: true, Verbose: p.verbose})
	if err != nil {
		return nil, fmt.Errorf("search instance types: %w", err)
	}
	if len(results) == 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	var best *MatchResult
	if w.Spot {
		spotResults, err := p.truffle.GetSpotPricing(ctx, results, truffleaws.SpotOptions{OnlyActive: true, Verbose: p.verbose})
		if err != nil {
			return nil, fmt.Errorf("get spot pricing: %w", err)
		}
		for i := range spotResults {
			candidate := MatchCandidate{SpotPrice: &spotResults[i]}
			for j := range results {
				if results[j].InstanceType == spotResults[i].InstanceType && results[j].Region == spotResults[i].Region {
					candidate.InstanceType = results[j]
					break
				}
			}
			if m := Evaluate(w, candidate); m != nil {
				m.MatchedAt = now
				if best == nil || m.Price < best.Price {
					best = m
				}
			}
		}
	} else {
		for i := range results {
			if m := Evaluate(w, MatchCandidate{InstanceType: results[i]}); m != nil {
				m.MatchedAt = now
				if best == nil || m.Price < best.Price {
					best = m
				}
			}
		}
	}
	return best, nil
}

// recordOutcome applies the shared capacity/unknown/terminal/success state
// machine: a capacity failure keeps the watch active (retry next cycle, uncapped)
// and resets its unknown-failure streak; an unknown failure also retries but
// increments the per-watch cap, stopping the watch once it reaches
// MaxConsecutiveFailures; a terminal failure stops it as failed; success records
// the match, notifies, marks matched, and clears the streak.
func (p *Poller) recordOutcome(ctx context.Context, w *Watch, m *MatchResult, failure FailureKind, summary *PollSummary) {
	if failure == FailureCapacity {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: capacity unavailable; will retry next cycle\n", w.WatchID)
		}
		summary.Retrying++
		// A genuine capacity failure clears any prior unknown-failure streak: the
		// cap only counts *consecutive* unclassified faults.
		w.ConsecutiveFailures = 0
		_ = p.store.ResetConsecutiveFailures(ctx, w.WatchID)
		return
	}

	if failure == FailureUnknown {
		n, err := p.store.IncrementConsecutiveFailures(ctx, w.WatchID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to record failure count for %s: %v\n", w.WatchID, err)
			n = w.ConsecutiveFailures + 1 // best-effort local estimate
		}
		w.ConsecutiveFailures = n
		if n < MaxConsecutiveFailures {
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Watch %s: unclassified failure %d/%d; will retry next cycle\n", w.WatchID, n, MaxConsecutiveFailures)
			}
			summary.Retrying++
			return
		}
		// Cap reached — stop the watch as failed instead of retrying forever.
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: %d consecutive unclassified failures; stopping watch\n", w.WatchID, n)
		}
		failure = FailureTerminal
	}

	// Success clears the streak so it persists via RecordMatch's PutWatch.
	if failure == FailureNone {
		w.ConsecutiveFailures = 0
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

			// Double-poller guard (#47): claim a short lease before acting so two
			// pollers (two daemons, or a daemon + the hosted Lambda) can't both fire
			// the same watch — the duplicate-RunInstances / double-launch class.
			// Skip this watch if another poller holds a live lease; we'll re-evaluate
			// next cycle. Released after the action so a still-active (retrying) watch
			// is immediately claimable. No-op when leasing is disabled.
			if p.leaseOwner != "" {
				if err := p.store.ClaimLease(ctx, w.WatchID, p.leaseOwner, time.Now().Add(leaseTTL)); err != nil {
					if errors.Is(err, ErrLeaseHeld) {
						if p.verbose {
							fmt.Fprintf(os.Stderr, "Watch %s: lease held by another poller; skipping this cycle\n", w.WatchID)
						}
						summary.Retrying++
						continue
					}
					fmt.Fprintf(os.Stderr, "Warning: could not claim lease on %s: %v\n", w.WatchID, err)
					continue
				}
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

			// Release the lease now (#47): a capacity-retrying watch stays active, so
			// freeing the lease lets the next cycle (this poller or another) re-claim
			// it immediately rather than waiting out leaseTTL. A matched/failed watch
			// is no longer active, so this is just tidy-up.
			if p.leaseOwner != "" {
				if err := p.store.ReleaseLease(ctx, w.WatchID, p.leaseOwner); err != nil && p.verbose {
					fmt.Fprintf(os.Stderr, "Warning: could not release lease on %s: %v\n", w.WatchID, err)
				}
			}
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
