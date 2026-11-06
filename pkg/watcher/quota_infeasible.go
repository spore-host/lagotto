package watcher

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
	"github.com/spore-host/truffle/pkg/quotas"
)

// ActionQuotaInfeasible is the MatchResult.ActionTaken recorded when EC2 refused a
// launch/hold for a candidate (type, region) because of an account quota (#238).
// It replaces the generic "spawn_failed"/"hold_failed" for that case, so history
// says WHY the attempt failed.
const ActionQuotaInfeasible = "quota_infeasible"

// maxQuotaFallthroughsPerCycle bounds how many quota-refused candidates one watch
// may fall through in a single poll cycle before it waits for the next one
// (#238). A quota refusal is a fast, synchronous rejection that creates nothing
// billable, so trying the next candidate at once is cheap and safe, but each
// attempt still runs spawn's full pre-launch path (IAM profile, AMI resolution,
// RunInstances) under a 2-minute lease. A refused candidate is recorded and
// skipped on later cycles, so a cap only spreads discovery across cycles; it
// never changes the outcome.
const maxQuotaFallthroughsPerCycle = 3

// QuotaInfeasibility records that EC2 refused a launch/hold of one candidate
// (instance type, region) because of an account quota (#238). It is stored on
// the watch so the stateless poller skips the candidate on later cycles, and so
// `lagotto status` can say which candidates are blocked and by how much.
//
// An entry is live for QuotaCapReprobeInterval after At (the same re-probe
// interval the #153 fleet cap uses). After that it no longer blocks the
// candidate: the quota may have been raised or other usage freed, so the next
// cycle tries it again and either succeeds or refreshes the entry.
//
// Every field except the key is optional, and the slice on Watch is omitempty, so
// records written before #238 read back with no entries, and a pre-#238 poller
// ignores the attribute (it just keeps its old terminal-on-quota behaviour).
type QuotaInfeasibility struct {
	InstanceType string    `json:"instance_type" dynamodbav:"instance_type"`
	Region       string    `json:"region" dynamodbav:"region"`
	Spot         bool      `json:"spot,omitempty" dynamodbav:"spot,omitempty"`
	Code         string    `json:"code,omitempty" dynamodbav:"code,omitempty"` // AWS error code, e.g. VcpuLimitExceeded
	At           time.Time `json:"at" dynamodbav:"at"`
	// VCPUs is what one instance of InstanceType needs.
	VCPUs int32 `json:"vcpus,omitempty" dynamodbav:"vcpus,omitempty"`
	// QuotaKnown reports whether QuotaLimit/QuotaUsed were read from Service
	// Quotas. They are optional garnish (#153): a poller without
	// servicequotas:GetServiceQuota still records the refusal, just without them.
	QuotaKnown bool               `json:"quota_known,omitempty" dynamodbav:"quota_known,omitempty"`
	QuotaLimit int32              `json:"quota_limit_vcpus,omitempty" dynamodbav:"quota_limit_vcpus,omitempty"`
	QuotaUsed  int32              `json:"quota_used_vcpus,omitempty" dynamodbav:"quota_used_vcpus,omitempty"`
	Family     quotas.QuotaFamily `json:"family,omitempty" dynamodbav:"family,omitempty"`
}

// Live reports whether the entry still blocks its candidate at now.
func (q QuotaInfeasibility) Live(now time.Time) bool {
	return now.Sub(q.At) < QuotaCapReprobeInterval
}

// ReprobeAt is when the candidate becomes eligible to be tried again.
func (q QuotaInfeasibility) ReprobeAt() time.Time {
	return q.At.Add(QuotaCapReprobeInterval)
}

// Describe renders one entry as a human line, e.g.
// "g7.24xlarge in eu-north-1: VcpuLimitExceeded; needs 96 vCPU, g-family
// On-Demand quota 64 vCPU with 0 in use".
func (q QuotaInfeasibility) Describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s in %s", q.InstanceType, q.Region)
	if q.Spot {
		b.WriteString(" (spot)")
	}
	b.WriteString(": refused by EC2 quota")
	if q.Code != "" {
		fmt.Fprintf(&b, " (%s)", q.Code)
	}
	var parts []string
	if q.VCPUs > 0 {
		parts = append(parts, fmt.Sprintf("needs %d vCPU", q.VCPUs))
	}
	if q.QuotaKnown {
		lifecycle := "On-Demand"
		if q.Spot {
			lifecycle = "Spot"
		}
		avail := q.QuotaLimit - q.QuotaUsed
		if avail < 0 {
			avail = 0
		}
		parts = append(parts, fmt.Sprintf("%s-family %s quota %d vCPU, %d in use, %d available",
			q.Family, lifecycle, q.QuotaLimit, q.QuotaUsed, avail))
	}
	if len(parts) > 0 {
		b.WriteString("; ")
		b.WriteString(strings.Join(parts, ", "))
	}
	return b.String()
}

// candidateKey identifies a (type, region) candidate. Quota is per family per
// region, but a smaller type in the same family can still fit where a larger one
// was refused, so the type is part of the key.
type candidateKey struct {
	instanceType string
	region       string
}

func (q QuotaInfeasibility) key() candidateKey {
	return candidateKey{q.InstanceType, q.Region}
}

func matchKey(m *MatchResult) candidateKey {
	return candidateKey{m.InstanceType, m.Region}
}

// liveQuotaInfeasible returns the watch's entries that still block their
// candidate at now, keyed by candidate.
func liveQuotaInfeasible(w *Watch, now time.Time) map[candidateKey]QuotaInfeasibility {
	out := make(map[candidateKey]QuotaInfeasibility, len(w.QuotaInfeasible))
	for _, q := range w.QuotaInfeasible {
		if q.Live(now) {
			out[q.key()] = q
		}
	}
	return out
}

// LiveQuotaInfeasible returns the entries that still block their candidate at
// now, sorted by type then region, for display.
func (w *Watch) LiveQuotaInfeasible(now time.Time) []QuotaInfeasibility {
	var out []QuotaInfeasibility
	for _, q := range w.QuotaInfeasible {
		if q.Live(now) {
			out = append(out, q)
		}
	}
	sortQuotaInfeasible(out)
	return out
}

func sortQuotaInfeasible(qs []QuotaInfeasibility) {
	sort.SliceStable(qs, func(i, j int) bool {
		if qs[i].InstanceType != qs[j].InstanceType {
			return qs[i].InstanceType < qs[j].InstanceType
		}
		return qs[i].Region < qs[j].Region
	})
}

// upsertQuotaInfeasible replaces (or adds) the entry for q's candidate and drops
// entries that are no longer live, so the stored list stays bounded by the
// candidate set and never carries stale verdicts forward.
func upsertQuotaInfeasible(existing []QuotaInfeasibility, q QuotaInfeasibility, now time.Time) []QuotaInfeasibility {
	out := make([]QuotaInfeasibility, 0, len(existing)+1)
	for _, e := range existing {
		if e.key() == q.key() || !e.Live(now) {
			continue
		}
		out = append(out, e)
	}
	out = append(out, q)
	sortQuotaInfeasible(out)
	return out
}

// QuotaInfeasibleReason composes the terminal message listing every candidate and
// its quota, shown in the notification and `lagotto status`.
func QuotaInfeasibleReason(entries []QuotaInfeasibility) string {
	if len(entries) == 0 {
		return ""
	}
	sorted := append([]QuotaInfeasibility(nil), entries...)
	sortQuotaInfeasible(sorted)
	var b strings.Builder
	if len(sorted) == 1 {
		b.WriteString("The only candidate was refused by an EC2 quota:")
	} else {
		fmt.Fprintf(&b, "All %d candidates were refused by EC2 quotas:", len(sorted))
	}
	for _, q := range sorted {
		b.WriteString("\n  - ")
		b.WriteString(q.Describe())
	}
	// One remediation hint per (region, family, lifecycle), sized for the largest
	// refused type in that bucket.
	type bucket struct {
		region string
		family quotas.QuotaFamily
		spot   bool
	}
	need := map[bucket]QuotaInfeasibility{}
	var order []bucket
	for _, q := range sorted {
		fam := q.Family
		if fam == "" {
			fam = quotas.GetQuotaFamily(q.InstanceType)
		}
		k := bucket{q.Region, fam, q.Spot}
		prev, seen := need[k]
		if !seen {
			order = append(order, k)
		}
		if !seen || q.VCPUs > prev.VCPUs {
			need[k] = q
		}
	}
	for _, k := range order {
		q := need[k]
		target := q.VCPUs
		if q.QuotaKnown {
			target += q.QuotaUsed
			if target <= q.QuotaLimit {
				target = q.QuotaLimit + 1
			}
		}
		if target <= 0 {
			continue
		}
		b.WriteString("\n")
		b.WriteString(quotacheck.Remediation(k.region, k.family, target, k.spot))
	}
	return b.String()
}

// errorCode extracts the AWS error code from err, or "".
func errorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// truncateBytes clips s to at most n bytes without splitting a multi-byte rune.
func truncateBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && s[cut]&0xC0 == 0x80 {
		cut--
	}
	return s[:cut]
}

// newQuotaInfeasibility builds the entry for a quota-refused candidate, enriched
// with the Service Quotas numbers when the poller can read them (fail-open, as
// for #153).
func (p *Poller) newQuotaInfeasibility(ctx context.Context, w *Watch, m *MatchResult, err error, now time.Time) QuotaInfeasibility {
	q := QuotaInfeasibility{
		InstanceType: m.InstanceType,
		Region:       m.Region,
		Spot:         m.IsSpot,
		Code:         errorCode(err),
		At:           now,
		VCPUs:        m.VCPUs,
		Family:       quotas.GetQuotaFamily(m.InstanceType),
	}
	if q.VCPUs == 0 {
		if v, ok := quotacheck.VCPUsForType(m.InstanceType); ok {
			q.VCPUs = v
		}
	}
	if rep := p.quotaReport(ctx, w, m.Region, m); rep != nil {
		q.QuotaKnown, q.QuotaLimit, q.QuotaUsed = true, rep.Limit, rep.Used
		if rep.Family != "" {
			q.Family = rep.Family
		}
	}
	return q
}

// rankedCandidates returns every candidate the watch matches this cycle, one per
// (type, region) (the cheapest AZ wins for spot), ordered cheapest first; ties
// keep search order. The first element is exactly the bestMatch the pre-#238
// poller would have picked.
func rankedCandidates(w *Watch, results []truffleaws.InstanceTypeResult, spotResults []truffleaws.SpotPriceResult, spot bool, now time.Time) []*MatchResult {
	best := map[candidateKey]*MatchResult{}
	var order []candidateKey
	consider := func(m *MatchResult) {
		if m == nil {
			return
		}
		m.MatchedAt = now
		k := matchKey(m)
		prev, ok := best[k]
		if !ok {
			order = append(order, k)
		}
		if !ok || m.Price < prev.Price {
			best[k] = m
		}
	}
	forEachCandidate(w, results, spotResults, spot, func(c MatchCandidate) { consider(Evaluate(w, c)) })
	out := make([]*MatchResult, 0, len(order))
	for _, k := range order {
		out = append(out, best[k])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Price < out[j].Price })
	return out
}

// candidateUniverse is the set of (type, region) candidates that could EVER be
// tried for this watch, used to decide "every candidate is quota-infeasible".
// For on-demand it is the matched set (on-demand prices are static). For spot it
// ignores --max-price, because a spot price above the cap now can drop below it
// later; a candidate that has never been tried is never infeasible, so the watch
// stays active rather than dying on a price it might still meet.
func candidateUniverse(w *Watch, results []truffleaws.InstanceTypeResult, spotResults []truffleaws.SpotPriceResult, spot bool) map[candidateKey]bool {
	probe := w
	if w.Spot && w.MaxPrice > 0 {
		cp := *w
		cp.MaxPrice = 0
		probe = &cp
	}
	out := map[candidateKey]bool{}
	forEachCandidate(probe, results, spotResults, spot, func(c MatchCandidate) {
		if m := Evaluate(probe, c); m != nil {
			out[matchKey(m)] = true
		}
	})
	return out
}

// forEachCandidate walks the search results the way pollGroup always has: spot
// prices joined to their instance-type result, or the on-demand results directly.
func forEachCandidate(w *Watch, results []truffleaws.InstanceTypeResult, spotResults []truffleaws.SpotPriceResult, spot bool, fn func(MatchCandidate)) {
	if spot {
		for i := range spotResults {
			c := MatchCandidate{SpotPrice: &spotResults[i]}
			for j := range results {
				if results[j].InstanceType == spotResults[i].InstanceType && results[j].Region == spotResults[i].Region {
					c.InstanceType = results[j]
					break
				}
			}
			fn(c)
		}
		return
	}
	for i := range results {
		fn(MatchCandidate{InstanceType: results[i]})
	}
}

// allQuotaInfeasible reports whether every candidate in universe has a live
// quota-infeasible entry. An empty universe is never "all infeasible".
func allQuotaInfeasible(universe map[candidateKey]bool, live map[candidateKey]QuotaInfeasibility) bool {
	if len(universe) == 0 {
		return false
	}
	for k := range universe {
		if _, ok := live[k]; !ok {
			return false
		}
	}
	return true
}

// failQuotaInfeasible retires a single-shot watch whose every candidate has been
// refused by an EC2 quota (#238). It records the final attempt (history row and
// watch, including the infeasibility list), marks the watch failed, and sends the
// quota-cap notification once. Once is structural: only ACTIVE watches are
// polled, and this transition makes the watch failed.
func (p *Poller) failQuotaInfeasible(ctx context.Context, w *Watch, m *MatchResult, universe map[candidateKey]bool, summary *PollSummary) {
	now := time.Now().UTC()
	live := liveQuotaInfeasible(w, now)
	var entries []QuotaInfeasibility
	for k := range universe {
		entries = append(entries, live[k])
	}
	reason := QuotaInfeasibleReason(entries)
	w.QuotaCapReason = truncateBytes(reason, maxQuotaInfeasibleReasonBytes)

	fmt.Fprintf(os.Stderr, "Watch %s: every candidate is refused by EC2 quota; stopping watch.\n%s\n", w.WatchID, reason)

	m.ActionTaken = ActionQuotaInfeasible
	w.ConsecutiveFailures = 0
	// The final refusal is an attempt too (#239); RecordMatch's PutWatch carries it.
	w.noteAttempt(&attempt{Err: quotaAttemptError(w, m)}, now)
	if err := p.store.RecordMatch(ctx, w, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record quota-infeasible outcome for %s: %v\n", w.WatchID, err)
	}
	if err := p.store.UpdateWatchStatus(ctx, w.WatchID, StatusFailed); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to update watch status for %s: %v\n", w.WatchID, err)
		// Still active in the store: don't notify, or the next cycle would
		// notify again.
		summary.Matches = append(summary.Matches, *m)
		return
	}
	w.Status = StatusFailed
	w.UpdatedAt = now
	summary.Failed++
	summary.Matches = append(summary.Matches, *m)
	if notify := p.quotaCapNotifier(); notify != nil {
		if err := notify(ctx, w, 0, reason); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: quota notification failed for %s: %v\n", w.WatchID, err)
		}
	}
}

// maxQuotaInfeasibleReasonBytes bounds the terminal message persisted in
// QuotaCapReason. It lists one line per candidate, so it is allowed to be longer
// than the fleet cap's one-paragraph reason, but still bounded.
const maxQuotaInfeasibleReasonBytes = 4096

// markQuotaInfeasible records a quota refusal for one candidate that does NOT end
// the watch: it persists the updated infeasibility list (so the next cycle skips
// the candidate) and writes a quota_infeasible history row, without touching the
// watch's match count or last match.
//
// The refusal is an attempt (#239): its heartbeat rides on the same UpdateItem
// that persists the list, with last_error set to the quota code.
func (p *Poller) markQuotaInfeasible(ctx context.Context, w *Watch, m *MatchResult) {
	m.ActionTaken = ActionQuotaInfeasible
	hb := &attempt{Err: quotaAttemptError(w, m)}
	w.noteAttempt(hb, time.Now().UTC())
	if err := p.store.setQuotaInfeasible(ctx, w.WatchID, w.QuotaInfeasible, hb); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record quota-infeasible candidate for %s: %v\n", w.WatchID, err)
	}
	if err := p.store.RecordHistory(ctx, m); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record quota-infeasible history for %s: %v\n", w.WatchID, err)
	}
}

// quotaAttemptError is the LastError for a quota-refused candidate (#239):
// "quota: <code>: <type> in <region>; needs N vCPU, ..." built from the #238
// entry just recorded for that candidate.
func quotaAttemptError(w *Watch, m *MatchResult) string {
	for _, q := range w.QuotaInfeasible {
		if q.key() != matchKey(m) {
			continue
		}
		detail := q.Describe()
		refused := ": refused by EC2 quota"
		if q.Code != "" {
			refused += " (" + q.Code + ")"
		}
		detail = strings.Replace(detail, refused, "", 1)
		head := ErrClassQuota
		if q.Code != "" {
			head += ": " + q.Code
		}
		return truncateBytes(head+": "+oneLine(detail), MaxLastErrorBytes)
	}
	return ErrClassQuota
}
