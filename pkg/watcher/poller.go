package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spore-host/lagotto/pkg/quotacheck"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// leaseTTL bounds how long a poller's processing lease on a watch is honored
// before it's considered stale and re-claimable (#47). It must comfortably
// exceed a single watch's action time (a RunInstances + FSx-create attempt
// across AZs) but stay well under a poll interval so a crashed poller frees the
// watch within one cycle.
const leaseTTL = 2 * time.Minute

// capacitySearcher is the slice of truffle the poller uses to discover capacity —
// an interface so tests inject a fake without a live truffle client. *truffleaws.Client
// satisfies it.
type capacitySearcher interface {
	SearchInstanceTypes(ctx context.Context, regions []string, matcher *regexp.Regexp, opts truffleaws.FilterOptions) ([]truffleaws.InstanceTypeResult, error)
	GetSpotPricing(ctx context.Context, instances []truffleaws.InstanceTypeResult, opts truffleaws.SpotOptions) ([]truffleaws.SpotPriceResult, error)
}

// Poller checks instance capacity for active watches.
type Poller struct {
	truffle   capacitySearcher
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
	// notifyQuotaCap overrides where a #153 quota-cap notification goes. Defaults
	// to p.notifier.NotifyQuotaCap; an unexported seam (mirroring the
	// Spawner.provision / listInstances indirections) so tests can COUNT
	// notifications — ValidateWebhookURL deliberately refuses loopback, so an
	// httptest server can't stand in for a real notify channel.
	notifyQuotaCap func(ctx context.Context, w *Watch, running int, reason string) error
	// notifyExpired overrides where a #161 expiry notification goes — same test
	// seam, same reason (ValidateWebhookURL refuses loopback, so an httptest server
	// can't stand in for a real notify channel).
	notifyExpired func(ctx context.Context, w *Watch) error
	// notifyUnsupported overrides where a #195 retirement notice goes (a stored
	// watch whose service/action can never fire) — same test seam pattern.
	notifyUnsupported func(ctx context.Context, w *Watch, reason string) error
	// reconcileEmail overrides the #240 email-subscription cleanup run at the
	// start of each sweep — same test seam pattern.
	reconcileEmail func(ctx context.Context, active []Watch) (ReconcileResult, error)
	// notifyMatch overrides where a #239 landing's match (or final failure)
	// notification goes — same test seam pattern.
	notifyMatch func(ctx context.Context, w *Watch, m *MatchResult) error
	// notifyLost overrides where a #255 lost-launch notification goes (a launch
	// or hold that did not land) — same test seam pattern.
	notifyLost func(ctx context.Context, w *Watch, m *MatchResult, stopped bool) error
	// notifySporedUnconfirmed overrides where a #255 spored_unconfirmed notice
	// goes — same test seam pattern.
	notifySporedUnconfirmed func(ctx context.Context, w *Watch, m *MatchResult) error
	// notifyReminder overrides where a #239 until-landed reminder goes — same
	// test seam pattern.
	notifyReminder func(ctx context.Context, w *Watch, r ReminderStage, now time.Time) error
	// now is the sweep clock (nil = time.Now). A seam so tests can step through
	// an acknowledgement window without waiting for it.
	now func() time.Time
	// quotas, when non-nil, enriches a quota-cap report with the account's actual
	// vCPU limit/usage numbers (#153). OPTIONAL and nil-safe by design: the cap
	// itself is detected from the RunInstances error, so a poller with no
	// servicequotas read permission (or an older runtime policy) still reports
	// "capped at M/N" — it just reports it without the numbers.
	quotas quotacheck.Lookup
	// throttle is the account's EC2 throttling state (#254): per-region
	// backoff and AZ cool-offs, shared with the spawner and holder and
	// rebuilt from the watches at the start of each sweep. Created on first
	// use (book), so a Poller built as a literal in a test has one too.
	throttle *ThrottleBook
	// groups caches each watch group read this sweep (#287); reset by PollAll.
	groups map[string]*WatchGroup
	// groupCancelled marks the watches a group cancelled this sweep (#287),
	// so the sweep's own copy of a sibling is not polled after it.
	groupCancelled map[string]bool
	// notifyGroupLanded / notifyGroupExtra override where a #287 group's
	// landed notification, and a member's extra-landing notice, go — same
	// test seam pattern.
	notifyGroupLanded func(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult, cancelled []string) error
	notifyGroupExtra  func(ctx context.Context, w *Watch, g *WatchGroup, m *MatchResult) error
	// enabledRegions caches the account's enabled regions for a watch that names
	// none (#306): an empty Regions means "all enabled regions", resolved here at
	// poll time. Filled on first use and kept for enabledRegionsTTL, so one cycle
	// costs at most one DescribeRegions.
	enabledRegions   []string
	enabledRegionsAt time.Time
}

// regionLister is the optional slice of truffle that lists the account's enabled
// regions. *truffleaws.Client satisfies it; a test fake may not, and then a watch
// with no regions is reported as a poll error rather than silently searching none.
type regionLister interface {
	GetEnabledRegions(ctx context.Context) ([]string, error)
}

// enabledRegionsTTL bounds how long the enabled-region list is reused. Enabling or
// disabling a region is rare and takes minutes to settle, so a few minutes is safe.
const enabledRegionsTTL = 10 * time.Minute

// resolveRegions returns regions, or the account's enabled regions when regions
// is empty (#306: "all enabled regions" at poll time). An error means the list
// could not be resolved; the caller records it on the watches instead of
// searching zero regions, which used to look like a typo'd pattern (#135).
func (p *Poller) resolveRegions(ctx context.Context, regions []string) ([]string, error) {
	if len(regions) > 0 {
		return regions, nil
	}
	now := time.Now()
	if p.now != nil {
		now = p.now()
	}
	if len(p.enabledRegions) > 0 && now.Sub(p.enabledRegionsAt) < enabledRegionsTTL {
		return p.enabledRegions, nil
	}
	lister, ok := p.truffle.(regionLister)
	if !ok {
		return nil, fmt.Errorf("watch names no regions and the capacity searcher cannot list the account's enabled regions; set --regions")
	}
	enabled, err := lister.GetEnabledRegions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list enabled regions for a watch with no --regions: %w", err)
	}
	if len(enabled) == 0 {
		return nil, fmt.Errorf("the account reports no enabled regions; set --regions")
	}
	p.enabledRegions, p.enabledRegionsAt = enabled, now
	return enabled, nil
}

// WatchFilter scopes a poll sweep to a subset of watches (#47). A zero-value
// filter (all fields empty) matches every watch — equivalent to no filter.
type WatchFilter struct {
	Project string // only watches with this Project label
	Owner   string // only watches whose UserID equals this (caller ARN)
	// OwnerID is the caller's principal id. An owner-scoped watch is the
	// caller's only when its key is under OwnerID; its user_id attribute, which
	// its writer chose, is not consulted.
	OwnerID  string
	WatchIDs []string // only these specific watch IDs
}

// Matches reports whether a watch is in scope for this filter. Exported so the
// CLI (`list --project/--mine`) can reuse the exact same matching logic the
// poller uses to scope a sweep (#47, #1), rather than re-implementing it.
func (f *WatchFilter) Matches(w *Watch) bool {
	return f.matches(w)
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
	if f.Owner != "" || f.OwnerID != "" {
		if owner := KeyOwner(w.WatchID); owner != "" {
			if owner != f.OwnerID {
				return false
			}
		} else if w.UserID != f.Owner {
			return false
		}
	}
	if len(f.WatchIDs) > 0 {
		found := false
		for _, id := range f.WatchIDs {
			if id == w.WatchID || (IsLegacyKey(id) && ShortID(w.WatchID) == id) {
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
	// Quotas optionally supplies the Service Quotas numbers used to enrich a
	// quota-cap report (#153). nil is fully supported — the cap is detected from
	// the RunInstances error, so this only adds exact limit/usage figures.
	Quotas quotacheck.Lookup
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
		p.quotas = opts[0].Quotas
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
	// QuotaCapped counts fleet watches whose top-up this cycle was bounded by the
	// account's EC2 vCPU quota — either a launch was refused by the quota, or the
	// cycle was suppressed because a known ceiling hadn't been re-probed yet
	// (#153). These watches stay ACTIVE and keep their running workers, so they're
	// counted separately from Retrying (capacity dry) and Failed (terminal).
	QuotaCapped int `json:"quota_capped"`
	// Landing counts spawn/hold watches whose launch or reservation is waiting to
	// land (#239): started this cycle, or still pending. They stay ACTIVE and are
	// checked, not relaunched, next cycle.
	Landing int `json:"landing"`
	// Reminded counts until-landed watches sent an acknowledgement reminder this
	// cycle (#239). They stay active; a reminder is not a state transition.
	Reminded int `json:"reminded"`
	// Throttled counts watches whose attempt EC2 throttled this cycle, or that
	// made none because their candidates are backing off from a throttle
	// (#254). They stay active and are also counted in Retrying.
	Throttled int `json:"throttled,omitempty"`
	// SporedChecks counts matched spawn watches whose spored check-in was
	// followed up this cycle (#255). They are matched; this is not a transition.
	SporedChecks int `json:"spored_checks"`
	// SporedUnconfirmed counts the ones resolved as spored_unconfirmed.
	SporedUnconfirmed int `json:"spored_unconfirmed"`
	// GroupCancelled counts watch-group members cancelled this cycle because
	// their group landed or ended (#287); GroupExtra the members whose launch
	// landed after their group was won; GroupWaiting the members that launched
	// nothing because a sibling holds the group's slot (also in Retrying).
	GroupCancelled int `json:"group_cancelled,omitempty"`
	GroupExtra     int `json:"group_extra,omitempty"`
	GroupWaiting   int `json:"group_waiting,omitempty"`
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
	// Watch groups are re-read each sweep (#287).
	p.groups, p.groupCancelled = nil, nil
	// Rebuild the account's throttle state from every active watch (#254),
	// not only this poller's scope: a throttle is the account's, whoever
	// earned it.
	p.book().StartSweep(p.clock(), watches)
	// Follow up on matched spawns whose spored has not checked in yet (#255).
	// Before this sweep's matches, so a check armed this cycle is not described
	// twice in it.
	p.pollSporedChecks(ctx, summary)

	// Unsubscribe email addresses no active watch names any more (#240). It runs
	// on the list loaded BEFORE this sweep's transitions, so an address whose
	// watch matches or expires in this sweep keeps its subscription until the
	// next one: the final email is published well before its subscription goes.
	// `watches` is every active watch, not this poller's filtered scope, so a
	// project-scoped daemon never drops another project's address.
	// Best-effort: a cleanup failure (a runtime policy that predates the grant,
	// say) must not stop the sweep, and `lagotto notify-gc` is the backstop.
	p.reconcileEmailSubscriptions(ctx, watches)

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
	now := p.clock()
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
		// A sibling its group cancelled earlier in this sweep (#287).
		if p.groupCancelled[w.WatchID] {
			continue
		}
		// A watch whose launch or hold is waiting to land (#239) is checked, never
		// relaunched. It comes before the expiry check so a launch that landed
		// before the deadline still counts. Only a landing that failed (the watch is
		// hunting again) goes on, to expire or to a new attempt this cycle.
		if w.Landing() {
			if !p.pollLanding(ctx, w, summary) {
				summary.Watched++
				continue
			}
		}
		// A watch-group member whose group already landed or was cancelled is
		// cancelled with a tombstone, not polled (#287).
		if groupManaged(w) && p.groupEnded(ctx, w, summary) {
			continue
		}
		if !w.ExpiresAt.IsZero() && now.After(w.ExpiresAt) {
			// An until-landed watch has no TTL; passing ExpiresAt means nobody
			// acknowledged it in time, so it pauses with that reason (#239).
			reason := expiryReason(w)
			if err := p.store.ExpireWatch(ctx, w.WatchID, reason); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: failed to expire watch %s: %v\n", w.WatchID, err)
			} else {
				summary.Expired++
				if p.verbose {
					if reason != "" {
						fmt.Fprintf(os.Stderr, "Watch %s paused (%s: deadline %s passed)\n", w.WatchID, reason, w.ExpiresAt.Format(time.RFC3339))
					} else {
						fmt.Fprintf(os.Stderr, "Watch %s expired (TTL %s elapsed)\n", w.WatchID, w.ExpiresAt.Format(time.RFC3339))
					}
				}
				// Mirror the persisted transition onto the in-memory copy BEFORE
				// notifying: TimeToGiveUp is derived from Status + UpdatedAt, and the
				// loaded record still says active (#161).
				w.Status = StatusExpired
				w.ExpiredReason = reason
				w.UpdatedAt = now
				// "Your watch expired without acquiring" is the mirror of the match
				// notification and the moment a human wants to hear from an unattended
				// hunt (#161). Best-effort: a notify failure must not stop the sweep, and
				// the tombstone is already durable either way.
				if notify := p.expiredNotifier(); notify != nil {
					if err := notify(ctx, w); err != nil {
						fmt.Fprintf(os.Stderr, "Warning: failed to notify expiry of watch %s: %v\n", w.WatchID, err)
					}
				}
			}
			continue
		}
		// An until-landed watch that is still hunting may be due a reminder of
		// its deadline (#239). Claimed and sent here, before this cycle's attempt,
		// so a slow or failing search never delays it.
		if w.UntilLanded {
			p.remindIfDue(ctx, w, now, summary)
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

// clock is the sweep's current time, in UTC.
func (p *Poller) clock() time.Time {
	if p.now != nil {
		return p.now().UTC()
	}
	return time.Now().UTC()
}

// reconcileEmailSubscriptions is the poller's half of the #240 deleter. The
// test seam reconcileEmail replaces the Notifier's.
func (p *Poller) reconcileEmailSubscriptions(ctx context.Context, active []Watch) {
	reconcile := p.reconcileEmail
	if reconcile == nil {
		if p.notifier == nil {
			return
		}
		reconcile = p.notifier.ReconcileEmailSubscriptions
	}
	res, err := reconcile(ctx, active)
	for _, a := range res.Removed {
		fmt.Fprintf(os.Stderr, "Unsubscribed %s from email notifications: no active watch names it\n", a)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: email subscription cleanup failed (run `lagotto notify-gc`; re-run `lagotto setup` if this is AccessDenied): %v\n", err)
	}
}

// PollWatch runs a single poll cycle for one watch and returns the resulting
// match events (launched or notified). Useful for testing.
func (p *Poller) PollWatch(ctx context.Context, w *Watch) ([]MatchResult, error) {
	summary := &PollSummary{}
	p.book().StartSweep(p.clock(), []Watch{*w})
	if w.Landing() && !p.pollLanding(ctx, w, summary) {
		return summary.Matches, nil
	}
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

	// A notify-only SageMaker watch can never fire: SageMaker has no capacity
	// signal short of submitting a job (#14), and notify doesn't submit one.
	// `watch` refuses the combination now (#195), but one stored before that
	// used to be marked polled here every cycle and sit silently until its TTL.
	// Retire it instead, and tell the user once why and what works. Once is
	// structural, as for expiry: only ACTIVE watches are loaded, and this
	// transition makes it failed.
	if w.Action == ActionNotify {
		p.retireUnsupported(ctx, w, sageMakerNotifyUnsupported, summary)
		return
	}
	if p.sagemaker == nil {
		// No SageMaker launcher wired: nothing this poller can do this cycle.
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

	if err == nil && outcome.ClearJobName {
		// A job that failed for capacity reports no error (it is retried), but it
		// is still the attempt's failure, so give the heartbeat something to say.
		err = errors.New("SageMaker job failed for lack of capacity; resubmitting")
	}
	p.recordOutcome(ctx, w, m, outcome.Kind, err, summary)
}

// retireUnsupported stops a stored watch whose service/action combination can
// never work (#195): it marks the watch failed, so it leaves the active set and
// is never polled again, and sends one notice saying why. It never submits or
// launches anything. Best-effort like the expiry notice: a notify failure is
// logged, not retried.
func (p *Poller) retireUnsupported(ctx context.Context, w *Watch, reason string, summary *PollSummary) {
	fmt.Fprintf(os.Stderr, "Warning: watch %s can never fire, retiring it as failed: %s\n", w.WatchID, reason)
	if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusFailed); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to retire watch %s: %v\n", w.WatchID, err)
		// Still active in the store: don't notify, or the next cycle would
		// notify again. It is retried next cycle.
		return
	}
	w.Status = StatusFailed
	w.UpdatedAt = time.Now().UTC()
	summary.Failed++
	if notify := p.unsupportedNotifier(); notify != nil {
		if err := notify(ctx, w, reason); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to notify retirement of watch %s: %v\n", w.WatchID, err)
		}
	}
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
	//
	// A fleet with no regions can't be counted (countRunningFleet needs the
	// regions its workers run in), so it could never be topped up. `watch` and
	// `update` refuse one since #275; one stored before that is reported in
	// last_poll_error, with the fix, on every cycle. Like a failed search, the
	// cycle leaves last_polled_at alone, so LAST POLL goes stale too.
	if len(w.Regions) == 0 {
		err := fmt.Errorf("%w; fix: lagotto update %s --regions <region,...>", ErrFleetNoRegions, w.WatchID)
		fmt.Fprintf(os.Stderr, "Warning: watch %s: %v\n", w.WatchID, err)
		p.recordPollError(ctx, []*Watch{w}, err)
		return
	}
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
		// Reaching the goal means the ceiling isn't binding any more (a quota
		// increase landed, or the count was mis-attributed), so drop the cap state
		// (#153) — otherwise a later dip would be suppressed by a stale ceiling.
		if w.IsQuotaCapped() {
			p.clearQuotaCap(ctx, w)
		}
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	// 2b. Quota-cap backoff (#153, #158). A fleet we KNOW is at its vCPU ceiling
	// must not re-fail a launch every cycle for the rest of its TTL. Placed AFTER
	// the completion-condition check (a capped fleet must still retire on
	// --until) and AFTER the gap<=0 return (it must still clear its cap on
	// reaching the goal), but BEFORE the lease claim and the capacity search, so
	// a suppressed cycle makes no launch and no capacity search.
	//
	// The `running >= w.QuotaCappedCount` conjunct is essential: the backoff
	// applies only to attempts we already know will fail. A worker that died below
	// the known ceiling is replaced on the very next cycle, not up to 30 minutes
	// later.
	//
	// When the quota can be read (#158), the reading decides how long to wait: a
	// raised limit tops up now, no room anywhere waits for the slow backstop, and
	// room (or an unreadable quota) keeps the #153 re-probe interval.
	//
	// IsQuotaCapped, not `QuotaCappedCount > 0`: a quota that refused the very
	// first worker caps the fleet at 0, and that is the cap that most needs the
	// backoff (#274).
	if w.IsQuotaCapped() && running >= w.QuotaCappedCount {
		probe := p.probeQuotaCap(ctx, w)
		wait := QuotaCapReprobeInterval
		switch probe {
		case capProbeRaised:
			wait = 0
		case capProbeNoRoom:
			wait = QuotaCapBackstopInterval
		}
		if since := time.Since(w.QuotaCappedAt); since < wait {
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Watch %s: fleet capped at %d/%d by EC2 quota (%s); next re-probe in %s\n",
					w.WatchID, w.QuotaCappedCount, w.DesiredCount, probe, (wait - since).Round(time.Second))
			}
			summary.QuotaCapped++
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			return
		}
		if probe == capProbeRaised {
			// Logged unconditionally: it is why a capped fleet launched early.
			fmt.Fprintf(os.Stderr, "Watch %s: EC2 quota in %s raised above %d vCPU since the cap; topping up now\n",
				w.WatchID, w.QuotaCapRegion, w.QuotaCapLimit)
		}
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
		p.recordPollError(ctx, []*Watch{w}, err)
		return
	}
	if bestMatch == nil {
		// No capacity offered this cycle; stay active, retry next poll.
		summary.Retrying++
		_ = p.store.UpdateLastPolled(ctx, w.WatchID)
		return
	}

	// Back off from an EC2 throttle (#254), as a single-shot watch does.
	if reason, held := p.candidateThrottled(&Watch{Action: ActionSpawn}, bestMatch, p.clock()); held {
		p.skipThrottled(ctx, w, reason, summary)
		return
	}
	before := p.book().version()
	out := p.fillFleetGap(ctx, w, bestMatch, gap, summary)
	if p.book().version() != before {
		p.persistThrottle(ctx, w, p.clock())
	}
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s: topped up %d/%d workers (fleet now ~%d/%d)\n",
			w.WatchID, out.Launched, gap, running+out.Launched, w.DesiredCount)
	}
	switch {
	case out.QuotaErr != nil:
		// The ceiling is wherever we actually got to this cycle — note that a
		// launched-then-capped cycle correctly records the cap at the NEW higher
		// level and re-notifies ("capped at 3/5", was "2/5").
		p.recordQuotaCap(ctx, w, running+out.Launched, bestMatch, out.QuotaErr, summary)
	case out.Launched > 0 && w.IsQuotaCapped():
		// A launch succeeded while capped → the ceiling moved (a granted increase,
		// or other usage freed up). Clear the cap so we top up at full speed and a
		// future cap notifies again (#153).
		p.clearQuotaCap(ctx, w)
	}
	// The watch stays active (never flips to matched) — it only retires when the
	// completion condition holds (step 1) or its TTL elapses. The cycle's launch
	// attempts ride on this closing write (#239), which used to be a plain
	// last_polled_at touch.
	_ = p.store.recordFleetCycle(ctx, w.WatchID, out.Attempts, out.LastErr)
}

// fillOutcome is what one gap-fill cycle produced: how many workers launched,
// and — when the stop was specifically an exhausted account quota (#153) — the
// error that proves it, so the caller can report the ceiling instead of silently
// re-failing the same launch every poll.
type fillOutcome struct {
	Launched int
	// Attempts is how many Spawn calls the cycle made (launched + the one that
	// stopped it, if any), and LastErr describes that stopping failure (#239).
	Attempts int
	LastErr  string
	// QuotaErr is non-nil only for a quota-exceeded refusal (IsQuotaExceeded).
	// Every other failure (capacity, terminal, unknown) leaves it nil and keeps the
	// pre-existing "warn and stop filling this cycle" behavior exactly as it was.
	QuotaErr error
}

// fillFleetGap launches up to `gap` workers into the found capacity, each a
// cloned MatchResult so Spawn stamps a distinct instance. It stops early on the
// first launch failure (capacity ran out mid-fill, or a terminal fault): the
// watch stays active and retries next cycle, so a partial fill is fine — the
// completion condition and TTL bound the watch.
//
// A quota refusal is reported back via fillOutcome.QuotaErr rather than acted on
// here. Note this does NOT re-classify anything: quota codes stay FailureTerminal
// in pkg/failure, and this function still must not treat a FailureTerminal as
// "fail the watch" — doing so would abandon the fleet's healthy running workers
// (which is exactly why the silence, not the retry, was the #153 defect).
func (p *Poller) fillFleetGap(ctx context.Context, w *Watch, bestMatch *MatchResult, gap int, summary *PollSummary) fillOutcome {
	launched := 0
	for i := 0; i < gap; i++ {
		m := bestMatch.clone()
		if err := p.spawner.Spawn(ctx, w, m); err != nil {
			failure := ClassifyFailure(err)
			fmt.Fprintf(os.Stderr, "Warning: fleet top-up launch %d/%d for %s failed (%s): %v\n",
				i+1, gap, w.WatchID, failureLabel(failure), err)
			out := fillOutcome{Launched: launched, Attempts: launched + 1, LastErr: describeAttemptError(failure, err)}
			if IsQuotaExceeded(err) {
				out.QuotaErr = err
			}
			return out
		}
		launched++
		summary.Launched++
		summary.Matches = append(summary.Matches, *m)
		if err := p.store.RecordMatch(ctx, w, m); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to record fleet match for %s: %v\n", w.WatchID, err)
		}
	}
	return fillOutcome{Launched: launched, Attempts: launched}
}

// recordQuotaCap reports that a fleet watch has hit its EC2 vCPU quota ceiling
// (#153): it composes the human reason, persists the cap level + backoff anchor,
// logs it, and — only the FIRST time at a given level — notifies.
//
// The watch deliberately stays ACTIVE. Quota codes classify FailureTerminal, but
// failing a fleet watch here would throw away its healthy running workers, and a
// quota can be raised mid-watch; the fix is to say so once and back off, not to
// stop.
func (p *Poller) recordQuotaCap(ctx context.Context, w *Watch, running int, m *MatchResult, quotaErr error, summary *PollSummary) {
	summary.QuotaCapped++

	instanceType := w.InstanceTypePattern
	region := ""
	if len(w.Regions) > 0 {
		region = w.Regions[0]
	}
	if m != nil {
		if m.InstanceType != "" {
			instanceType = m.InstanceType
		}
		if m.Region != "" {
			region = m.Region
		}
	}

	rep := p.quotaReport(ctx, w, region, m)
	reason := quotacheck.CapReason(region, instanceType, w.Spot, w.DesiredCount, running, rep)
	// The limit read at cap time is what a later cycle compares against (#158);
	// 0 = it could not be read.
	var limit int32
	if rep != nil && rep.Known {
		limit = rep.Limit
	}

	// isNew must be computed BEFORE the write-back: the persisted level is what
	// makes "report once" possible across stateless poller invocations.
	// A cap at 0 on an uncapped watch is new too (#274): the level alone can't
	// tell "capped at 0" from "not capped".
	isNew := !w.IsQuotaCapped() || w.QuotaCappedCount != running

	if err := p.store.RecordQuotaCap(ctx, w.WatchID, running, reason, limit, region); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record quota cap for %s: %v\n", w.WatchID, err)
	}
	// Mirror recordOutcome's discipline: keep the in-memory watch in step with the
	// store so the rest of this cycle (and the caller) sees the new level.
	w.QuotaCapped = true
	w.QuotaCappedCount = running
	w.QuotaCappedAt = time.Now().UTC()
	w.QuotaCapReason = reason
	w.QuotaCapLimit = limit
	w.QuotaCapRegion = region

	// Log unconditionally, not under p.verbose: for a hosted poller the
	// CloudWatch log is a real audience, and this is the evidence that
	// distinguishes a quota ceiling from a capacity drought.
	fmt.Fprintf(os.Stderr, "Watch %s: %s (quota error: %v)\n", w.WatchID, reason, quotaErr)

	if !isNew {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: quota cap unchanged at %d/%d; not re-notifying\n",
				w.WatchID, running, w.DesiredCount)
		}
		return
	}
	if notify := p.quotaCapNotifier(); notify != nil {
		if err := notify(ctx, w, running, reason); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: quota-cap notification failed for %s: %v\n", w.WatchID, err)
		}
	}
}

// quotaCapNotifier resolves where a quota-cap notification goes: the test seam if
// set, else the wired Notifier, else nowhere (a notify-less watch still gets the
// persisted reason and `lagotto status`).
func (p *Poller) quotaCapNotifier() func(ctx context.Context, w *Watch, running int, reason string) error {
	if p.notifyQuotaCap != nil {
		return p.notifyQuotaCap
	}
	if p.notifier != nil {
		return p.notifier.NotifyQuotaCap
	}
	return nil
}

// unsupportedNotifier resolves where a #195 retirement notice goes: the test
// seam if set, else the wired Notifier, else nowhere.
func (p *Poller) unsupportedNotifier() func(ctx context.Context, w *Watch, reason string) error {
	if p.notifyUnsupported != nil {
		return p.notifyUnsupported
	}
	if p.notifier != nil {
		return p.notifier.NotifyUnsupported
	}
	return nil
}

// matchNotifier resolves where a landing's match notification goes (#239): the
// test seam if set, else the wired Notifier, else nowhere.
func (p *Poller) matchNotifier() func(ctx context.Context, w *Watch, m *MatchResult) error {
	if p.notifyMatch != nil {
		return p.notifyMatch
	}
	if p.notifier != nil {
		return p.notifier.Notify
	}
	return nil
}

// lostNotifier resolves where a #255 lost-launch notification goes: the test
// seam if set, else the wired Notifier, else nowhere.
func (p *Poller) lostNotifier() func(ctx context.Context, w *Watch, m *MatchResult, stopped bool) error {
	if p.notifyLost != nil {
		return p.notifyLost
	}
	if p.notifier != nil {
		return p.notifier.NotifyLandingLost
	}
	return nil
}

// sporedNotifier resolves where a #255 spored_unconfirmed notice goes: the
// test seam if set, else the wired Notifier, else nowhere.
func (p *Poller) sporedNotifier() func(ctx context.Context, w *Watch, m *MatchResult) error {
	if p.notifySporedUnconfirmed != nil {
		return p.notifySporedUnconfirmed
	}
	if p.notifier != nil {
		return p.notifier.NotifySporedUnconfirmed
	}
	return nil
}

// expiredNotifier resolves where a TTL-expiry notification goes: the test seam if
// set, else the wired Notifier, else nowhere (a notify-less watch still gets the
// persisted status=expired tombstone, `lagotto status` and `lagotto history`).
func (p *Poller) expiredNotifier() func(ctx context.Context, w *Watch) error {
	if p.notifyExpired != nil {
		return p.notifyExpired
	}
	if p.notifier != nil {
		return p.notifier.NotifyExpired
	}
	return nil
}

// quotaReport optionally enriches a cap report with the account's real vCPU
// limit/usage. It FAILS OPEN in every direction — no lookup wired, an
// AccessDenied from a poller running a pre-#153 runtime policy, a throttle, a
// timeout — returning nil so CapReason still names the family and the
// remediation. The core fix must never depend on this (#148/#150).
func (p *Poller) quotaReport(ctx context.Context, w *Watch, region string, m *MatchResult) *quotacheck.Report {
	if p.quotas == nil || region == "" {
		return nil
	}
	rungs := SplitInstanceTypePatterns(w.InstanceTypePattern)
	if m != nil && m.InstanceType != "" {
		// The matched type is what RunInstances was actually refused for, so it's a
		// better basis than the pattern's smallest rung.
		rungs = []string{m.InstanceType}
	}
	lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rep, err := quotacheck.Feasibility(lctx, p.quotas, quotacheck.Request{
		Region:  region,
		Rungs:   rungs,
		Spot:    w.Spot,
		Desired: w.DesiredCount,
	})
	if err != nil || !rep.Known {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: quota numbers unavailable (reporting the cap without them): %v\n", w.WatchID, err)
		}
		return nil
	}
	return &rep
}

// capProbe is what the current quota reading says about a capped fleet (#158).
type capProbe int

const (
	// capProbeUnknown: the quota could not be read in every region of the
	// watch (no reader, an error, an unreported family, too many regions).
	// Falls back to the #153 time-based re-probe.
	capProbeUnknown capProbe = iota
	// capProbeRoom: some region shows room for another worker, so the cap may
	// have lifted (other usage released capacity). Keeps the #153 interval,
	// not an immediate attempt: if the reading disagrees with EC2, an
	// immediate attempt would fail every cycle.
	capProbeRoom
	// capProbeNoRoom: every region was read, none has room for another worker,
	// and the cap region's limit is no higher than at cap time. A launch would
	// be refused, so only the slow QuotaCapBackstopInterval re-probe is made.
	capProbeNoRoom
	// capProbeRaised: the cap region's limit is higher than the one recorded at
	// cap time — a granted increase. Tops up on this cycle.
	capProbeRaised
)

func (c capProbe) String() string {
	switch c {
	case capProbeRoom:
		return "the quota shows room"
	case capProbeNoRoom:
		return "quota unchanged and full"
	case capProbeRaised:
		return "quota raised"
	default:
		return "quota not readable"
	}
}

// maxCapProbeRegions bounds how many regions a capped fleet's quota probe
// reads per cycle (each GetQuotas is ~16 GetServiceQuota calls plus
// DescribeInstances, cached by the reader for 5 minutes). A fleet over more
// regions keeps the #153 time-based re-probe.
const maxCapProbeRegions = 3

// probeQuotaCap reads the current family quota in every region of a capped
// fleet and compares it with what was recorded at cap time (#158). It uses
// the same reader, and so the same servicequotas/DescribeInstances reads, as
// the cap report, and it fails open: anything it can't read is
// capProbeUnknown, which keeps today's behaviour. Unknown is never "full".
func (p *Poller) probeQuotaCap(ctx context.Context, w *Watch) capProbe {
	if p.quotas == nil || len(w.Regions) == 0 || len(w.Regions) > maxCapProbeRegions {
		return capProbeUnknown
	}
	rungs := SplitInstanceTypePatterns(w.InstanceTypePattern)
	if len(rungs) == 0 {
		return capProbeUnknown
	}
	room := false
	for _, region := range w.Regions {
		lctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		// Desired 1: "is there room for one more of the smallest rung?" — the
		// most optimistic reading, so a doubtful case probes rather than waits.
		rep, err := quotacheck.Feasibility(lctx, p.quotas, quotacheck.Request{
			Region: region, Rungs: rungs, Spot: w.Spot, Desired: 1,
		})
		cancel()
		if err != nil || !rep.Known {
			if p.verbose {
				why := fmt.Sprintf("no %s-family figure", rep.Family)
				if err != nil {
					why = err.Error()
				}
				fmt.Fprintf(os.Stderr, "Watch %s: quota not readable in %s, keeping the time-based re-probe: %s\n", w.WatchID, region, why)
			}
			return capProbeUnknown
		}
		if region == w.QuotaCapRegion && w.QuotaCapLimit > 0 && rep.Limit > w.QuotaCapLimit {
			return capProbeRaised
		}
		if rep.Feasible {
			room = true
		}
	}
	if room {
		return capProbeRoom
	}
	return capProbeNoRoom
}

// clearQuotaCap drops a watch's quota-cap state (#153) after a successful launch
// or on reaching the fleet goal, so a granted quota increase is picked up with no
// user action and a later cap notifies again.
func (p *Poller) clearQuotaCap(ctx context.Context, w *Watch) {
	if err := p.store.ClearQuotaCap(ctx, w.WatchID); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to clear quota cap for %s: %v\n", w.WatchID, err)
		return
	}
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s: quota cap cleared (was %d/%d)\n", w.WatchID, w.QuotaCappedCount, w.DesiredCount)
	}
	w.QuotaCapped = false
	w.QuotaCappedCount = 0
	w.QuotaCappedAt = time.Time{}
	w.QuotaCapReason = ""
	w.QuotaCapLimit = 0
	w.QuotaCapRegion = ""
}

// searchBestMatch runs a single-watch truffle capacity search and returns the
// best matching MatchResult, or nil if nothing is offered this cycle. Shared
// search/evaluate logic with pollGroup, scoped to one watch (the fleet path).
func (p *Poller) searchBestMatch(ctx context.Context, w *Watch) (*MatchResult, error) {
	matcher, err := regexp.Compile(patternToRegex(w.InstanceTypePattern))
	if err != nil {
		return nil, fmt.Errorf("compile pattern %q: %w", w.InstanceTypePattern, err)
	}
	results, err := p.truffle.SearchInstanceTypes(ctx, w.Regions, matcher, truffleaws.FilterOptions{IncludeAZs: true, Verbose: p.verbose})
	if err != nil {
		return nil, fmt.Errorf("search instance types: %w", err)
	}
	if len(results) == 0 {
		warnUnmatchablePattern(w)
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
//
// Every call is one action attempt, so it also carries the heartbeat (#239):
// attempts, last_attempt_at and, for a failure, last_error. They ride on the
// write each branch already makes (the Reset/Increment UpdateItem, or
// RecordMatch's PutWatch), never on a write of their own. err is the attempt's
// error, used only to describe it.
func (p *Poller) recordOutcome(ctx context.Context, w *Watch, m *MatchResult, failure FailureKind, err error, summary *PollSummary) {
	var hb *attempt
	if failure == FailureNone {
		hb = &attempt{}
	} else {
		hb = failedAttempt(failure, err)
	}
	now := time.Now().UTC()

	if failure == FailureThrottled {
		// EC2 throttled the attempt (#254). Never terminal, and not counted
		// toward MaxConsecutiveFailures: a throttle says nothing about the
		// config. The streak is left as it was. The backoff itself is in the
		// throttle book, already updated by the spawner or holder.
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: throttled by EC2; backing off, will retry\n", w.WatchID)
		}
		summary.Retrying++
		summary.Throttled++
		w.noteAttempt(hb, now)
		if err := p.store.setThrottleState(ctx, w.WatchID, w.Throttle, w.AZCooloffs, hb, ""); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to record throttled attempt for %s: %v\n", w.WatchID, err)
		}
		return
	}

	if failure == FailureCapacity {
		if p.verbose {
			fmt.Fprintf(os.Stderr, "Watch %s: capacity unavailable; will retry next cycle\n", w.WatchID)
		}
		summary.Retrying++
		// A genuine capacity failure clears any prior unknown-failure streak: the
		// cap only counts *consecutive* unclassified faults.
		w.ConsecutiveFailures = 0
		w.noteAttempt(hb, now)
		_ = p.store.resetConsecutiveFailures(ctx, w.WatchID, hb)
		return
	}

	// counted is set once the heartbeat has been written by the Increment below,
	// so the terminal path's PutWatch doesn't count the same attempt twice.
	counted := false
	if failure == FailureUnknown {
		n, attempts, ierr := p.store.incrementConsecutiveFailures(ctx, w.WatchID, hb)
		w.noteAttempt(hb, now)
		if ierr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to record failure count for %s: %v\n", w.WatchID, ierr)
			n = w.ConsecutiveFailures + 1 // best-effort local estimate
		} else if attempts > 0 {
			w.Attempts = attempts // the store's atomic count wins over the local one
		}
		counted = true
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

	// A launch or hold that has not landed yet is not a match (#239): record it
	// as landing (one write, heartbeat included) and check it next cycle.
	if failure == FailureNone && p.startLanding(ctx, w, m, now, summary) {
		return
	}
	// A watch-group member that landed at once (#287) must win its group
	// before it is a match; a sibling that won first makes this an extra.
	var group *WatchGroup
	groupWon := false
	if failure == FailureNone && groupManaged(w) && (m.ActionTaken == "spawned" || m.ActionTaken == "held") {
		won, ok := p.winGroup(ctx, w, m, now, summary)
		if !ok {
			// The group could not be written: record the launch as landing, so
			// the next cycle decides it, rather than a match nobody arbitrated.
			if p.forceLanding(ctx, w, m, now, summary) {
				return
			}
		}
		if ok && !won {
			return
		}
		groupWon = won
		group = p.groups[w.GroupRef()]
	}

	// Success clears the streak so it persists via RecordMatch's PutWatch.
	if failure == FailureNone {
		w.ConsecutiveFailures = 0
		// An instance already running at RunInstances still gets the #255 spored
		// check-in follow-up, armed on the record RecordMatch writes.
		if m.ActionTaken == "spawned" && !m.LandedAt.IsZero() {
			p.armSporedCheck(w, m, false, now)
		}
	}
	// RecordMatch writes the whole record, so the heartbeat travels on it.
	if !counted {
		w.noteAttempt(hb, now)
	}

	if err := p.store.RecordMatch(ctx, w, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record match for %s: %v\n", w.WatchID, err)
	}
	if groupWon {
		// The group's one notification replaces the member's own (#287).
		defer p.finishGroup(ctx, w, m, group, now, summary)
	} else if p.notifier != nil && failure == FailureTerminal && IsConfigRefused(err) && normalizeService(w.Service) == ServiceEC2 {
		// Refused on lagotto's own rules before anything launched (#309): say
		// so, with the fix, instead of a match notice.
		if nerr := p.notifier.NotifyConfigRefused(ctx, w, m, err); nerr != nil {
			fmt.Fprintf(os.Stderr, "Warning: refusal notification failed for %s: %v\n", w.WatchID, nerr)
		}
	} else if p.notifier != nil {
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
	// Convert pattern to regex (support wildcards like "p5.*" and comma-lists
	// like "g6.4xlarge,g6.2xlarge", which match ANY listed rung — #135).
	regexPattern := patternToRegex(pattern)
	matcher, err := regexp.Compile(regexPattern)
	if err != nil {
		return fmt.Errorf("compile pattern %q: %w", pattern, err)
	}

	// An empty region list means every enabled region (#306). Resolve it here so
	// the search never runs across zero regions.
	regions, err = p.resolveRegions(ctx, regions)
	if err != nil {
		p.recordPollError(ctx, watches, err)
		return err
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
		p.recordPollError(ctx, watches, err)
		return fmt.Errorf("search instance types: %w", err)
	}

	if len(results) == 0 {
		// Pre-filter found nothing offered; nothing to attempt. Watches stay
		// active and are retried next cycle. If a watch's pattern matched no
		// offered instance type on its very first poll, that's the #135 symptom
		// (a typo or an unmatchable literal) — warn once so a false "capacity
		// dry" surfaces immediately instead of after hours.
		for _, w := range watches {
			warnUnmatchablePattern(w)
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
			p.recordPollError(ctx, watches, err)
			return fmt.Errorf("get spot pricing: %w", err)
		}
	}

	// Evaluate each watch against the results
	now := time.Now().UTC()

	for _, w := range watches {
		if p.groupCancelled[w.WatchID] {
			continue // its group cancelled it earlier in this sweep (#287)
		}
		// Every candidate this watch matches, one per (type, region), cheapest
		// first. The head is the bestMatch the pre-#238 poller picked.
		candidates := rankedCandidates(w, results, spotResults, spot, now)
		if len(candidates) == 0 {
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			continue
		}

		// Skip candidates EC2 recently refused on quota (#238): a refusal is
		// remembered for QuotaCapReprobeInterval, then the candidate is re-probed.
		live := liveQuotaInfeasible(w, now)
		eligible := candidates[:0:0]
		for _, c := range candidates {
			if _, blocked := live[matchKey(c)]; !blocked {
				eligible = append(eligible, c)
			}
		}
		if len(eligible) == 0 {
			// Every matched candidate is still quota-blocked. Wait for the re-probe
			// rather than fail: the watch only fails right after a fresh refusal,
			// so a transiently thinner search result can't kill it.
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Watch %s: all %d matched candidate(s) are quota-infeasible; waiting for re-probe\n", w.WatchID, len(candidates))
			}
			summary.Retrying++
			_ = p.store.UpdateLastPolled(ctx, w.WatchID)
			continue
		}
		// Skip candidates backing off from an EC2 throttle (#254): a region
		// throttled for the account, or one whose every offered AZ a throttle
		// named impaired. The others are still tried, cheapest first; with
		// none left the watch waits, active, without calling EC2.
		var throttleReason string
		unthrottled := eligible[:0:0]
		for _, c := range eligible {
			if reason, held := p.candidateThrottled(w, c, p.clock()); held {
				if throttleReason == "" {
					throttleReason = reason
				}
				continue
			}
			unthrottled = append(unthrottled, c)
		}
		if len(unthrottled) == 0 {
			p.skipThrottled(ctx, w, throttleReason, summary)
			continue
		}
		eligible = unthrottled
		bestMatch := eligible[0]

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

		// A watch-group member claims its group's slot first (#287), so two
		// members never launch for the same group at once. It is kept while the
		// launch is landing and released otherwise.
		if groupManaged(w) && !p.claimGroupSlot(ctx, w, summary) {
			if p.leaseOwner != "" {
				_ = p.store.ReleaseLease(ctx, w.WatchID, p.leaseOwner)
			}
			continue
		}

		p.actOnCandidates(ctx, w, eligible, results, spotResults, spot, summary)
		if groupManaged(w) && !w.Landing() {
			p.releaseGroupSlot(ctx, w)
		}

		// Release the lease now (#47): a capacity-retrying watch stays active, so
		// freeing the lease lets the next cycle (this poller or another) re-claim
		// it immediately rather than waiting out leaseTTL. A matched/failed watch
		// is no longer active, so this is just tidy-up.
		if p.leaseOwner != "" {
			if err := p.store.ReleaseLease(ctx, w.WatchID, p.leaseOwner); err != nil && p.verbose {
				fmt.Fprintf(os.Stderr, "Warning: could not release lease on %s: %v\n", w.WatchID, err)
			}
		}
	}

	return nil
}

// actOnCandidates runs the watch's action against its eligible candidates in
// order (#238). The first attempt is the cheapest eligible candidate, exactly as
// before. A quota refusal no longer ends the watch: it marks that (type, region)
// infeasible and, within the same cycle, moves to the next candidate, because a
// quota refusal is a fast synchronous rejection that creates nothing billable.
// At most maxQuotaFallthroughsPerCycle such fall-throughs happen per cycle; the
// rest wait for the next one. Any other outcome (launched, capacity, unknown,
// non-quota terminal) goes through recordOutcome as it always has, and ends the
// cycle for this watch.
//
// The watch fails on quota only when every candidate in its universe has a live
// quota-infeasible entry. For a watch with one candidate that is the first
// refusal, the same as before #238.
func (p *Poller) actOnCandidates(ctx context.Context, w *Watch, eligible []*MatchResult,
	results []truffleaws.InstanceTypeResult, spotResults []truffleaws.SpotPriceResult, spot bool, summary *PollSummary) {
	for i, cand := range eligible {
		if i > maxQuotaFallthroughsPerCycle {
			fmt.Fprintf(os.Stderr, "Watch %s: %d candidate(s) refused by quota this cycle; trying the rest next cycle\n", w.WatchID, i)
			summary.Retrying++
			return
		}
		if i > 0 {
			// A distinct history sort key per attempt (matched_at is the range key).
			cand.MatchedAt = time.Now().UTC()
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Watch %s: trying next candidate %s in %s at $%.4f/hr\n",
					w.WatchID, cand.InstanceType, cand.Region, cand.Price)
			}
		}

		before := p.book().version()
		failure, err := p.runAction(ctx, w, cand)
		if p.book().version() != before {
			// The attempt was throttled, or cooled an AZ off (#254): write the
			// account's throttle state onto this watch, so the next sweep (a
			// new invocation) still obeys it.
			p.persistThrottle(ctx, w, p.clock())
		}
		if err == nil || !IsQuotaExceeded(err) {
			p.recordOutcome(ctx, w, cand, failure, err, summary)
			return
		}

		// Quota refusal: this candidate is infeasible until the re-probe interval.
		now := time.Now().UTC()
		q := p.newQuotaInfeasibility(ctx, w, cand, err, now)
		cand.QuotaDetail = q.Describe()
		w.QuotaInfeasible = upsertQuotaInfeasible(w.QuotaInfeasible, q, now)
		// Log unconditionally: for the hosted poller CloudWatch is a real audience.
		fmt.Fprintf(os.Stderr, "Watch %s: %s; candidate is infeasible until %s\n",
			w.WatchID, q.Describe(), q.ReprobeAt().Format(time.RFC3339))

		universe := candidateUniverse(w, results, spotResults, spot)
		if allQuotaInfeasible(universe, liveQuotaInfeasible(w, now)) {
			p.failQuotaInfeasible(ctx, w, cand, universe, summary)
			return
		}
		p.markQuotaInfeasible(ctx, w, cand)
	}
	// Every eligible candidate this cycle was quota-refused, but the universe
	// still holds one that is not (e.g. a spot candidate above --max-price now).
	// Stay active.
	summary.Retrying++
}

// runAction executes the watch's action for one candidate and classifies any
// failure. The launch IS the capacity test: cheap signals (offerings, spot price)
// only decide it's worth attempting — they never prove capacity. So a capacity
// failure here is expected and must be retried, not treated as terminal.
func (p *Poller) runAction(ctx context.Context, w *Watch, m *MatchResult) (FailureKind, error) {
	switch w.Action {
	case ActionSpawn:
		if p.spawner == nil {
			m.ActionTaken = "notified"
			return FailureNone, nil
		}
		if err := p.spawner.Spawn(ctx, w, m); err != nil {
			failure := ClassifyFailure(err)
			fmt.Fprintf(os.Stderr, "Warning: auto-spawn failed for %s (%s): %v\n", w.WatchID, failureLabel(failure), err)
			m.ActionTaken = "spawn_failed"
			return failure, err
		}
	case ActionHold:
		if p.holder == nil {
			m.ActionTaken = "notified"
			return FailureNone, nil
		}
		if err := p.holder.Hold(ctx, w, m); err != nil {
			failure := ClassifyFailure(err)
			fmt.Fprintf(os.Stderr, "Warning: hold failed for %s (%s): %v\n", w.WatchID, failureLabel(failure), err)
			if failure == FailureCapacity {
				// Capacity-reservation admission is stricter than launchability:
				// a hold can be refused for a type that RunInstances would launch.
				// Don't let this read as "the type can't be launched" (#136).
				fmt.Fprintf(os.Stderr, "  note: capacity reservation not admitted (stricter than launch; the type may still be launchable via --action spawn)\n")
			}
			m.ActionTaken = "hold_failed"
			return failure, err
		}
	default:
		m.ActionTaken = "notified"
	}
	return FailureNone, nil
}

// warnUnmatchablePattern emits a one-time (first-poll) warning that a watch's
// pattern matched no offered instance type in its region(s). DescribeInstanceTypes
// reports every type a region offers regardless of current capacity, so an empty
// candidate set means the pattern itself resolves to nothing knowable — almost
// always a typo or unmatchable literal (the #135 symptom), not a capacity dry. It
// does NOT stop the watch: the type may simply not be offered yet, so we warn and
// keep watching. Gated on LastPolledAt.IsZero() so it fires once, not every cycle.
func warnUnmatchablePattern(w *Watch) {
	if w == nil || !w.LastPolledAt.IsZero() {
		return
	}
	regions := "the requested region(s)"
	if len(w.Regions) > 0 {
		regions = strings.Join(w.Regions, ", ")
	}
	fmt.Fprintf(os.Stderr,
		"Warning: watch %s pattern %q matches no known instance type in %s — check for typos; still watching in case it becomes available\n",
		w.WatchID, w.InstanceTypePattern, regions)
}

// recordPollError stamps last_poll_error on each watch whose capacity search
// failed this cycle (#239). Before this, such a cycle wrote nothing at all, so a
// watch whose search kept failing looked exactly like an idle one. Best-effort:
// a failed stamp is logged under -v and never stops the sweep.
func (p *Poller) recordPollError(ctx context.Context, watches []*Watch, searchErr error) {
	msg := describePollError(searchErr)
	now := time.Now().UTC()
	for _, w := range watches {
		if err := p.store.recordPollError(ctx, w.WatchID, msg); err != nil {
			if p.verbose {
				fmt.Fprintf(os.Stderr, "Warning: could not record poll error on %s: %v\n", w.WatchID, err)
			}
			continue
		}
		w.LastPollError, w.LastPollErrorAt = msg, now
	}
}

// SplitInstanceTypePatterns splits a watch pattern into its comma-separated
// sub-patterns (#135). Exported so the CLI (`watch`'s create-time quota
// feasibility check) reuses the exact splitting the poller uses, rather than
// re-implementing comma-list semantics — same wrapper style as WatchFilter.Matches.
func SplitInstanceTypePatterns(pattern string) []string {
	return splitInstanceTypePatterns(pattern)
}

// splitInstanceTypePatterns splits a watch pattern on commas into its sub-patterns,
// trimming surrounding whitespace and dropping empty entries. A pattern with no
// comma yields a single-element slice (the trimmed pattern), so single-pattern
// callers behave exactly as before. Returns nil when nothing usable remains (an
// all-blank/empty input) (#135).
func splitInstanceTypePatterns(pattern string) []string {
	var parts []string
	for _, p := range strings.Split(pattern, ",") {
		if p = strings.TrimSpace(p); p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// patternToRegex converts a watch instance-type pattern into a single anchored
// regex. The pattern may be a comma-separated list of sub-patterns (e.g.
// "g6.4xlarge,g6.2xlarge"); a candidate type matches if it matches ANY sub-pattern
// (OR semantics). A single sub-pattern is converted exactly as before via
// wildcardToRegex, so wildcard/exact behavior is unchanged (#135).
func patternToRegex(pattern string) string {
	parts := splitInstanceTypePatterns(pattern)
	if len(parts) <= 1 {
		if len(parts) == 1 {
			return wildcardToRegex(parts[0])
		}
		// All-blank input: fall back to the raw pattern so an empty/whitespace
		// pattern compiles to the same (empty-matching) regex it did before.
		return wildcardToRegex(pattern)
	}
	// Each sub-pattern is independently converted (keeping its wildcard/exact
	// semantics) and the anchored alternatives are OR'd together.
	subs := make([]string, len(parts))
	for i, p := range parts {
		subs[i] = wildcardToRegex(p)
	}
	return strings.Join(subs, "|")
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
