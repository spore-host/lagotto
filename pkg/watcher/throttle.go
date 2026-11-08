package watcher

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/spore-host/lagotto/pkg/failure"
)

// EC2 throttling (#254). On 2026-11-06 EC2 throttled every RunInstances in
// us-west-2a ("... throttled due to an operational issue in availability zone
// usw2-az1"). The poller classified RequestLimitExceeded as an unknown
// failure, so the throttle ran watches that had waited 17-49 h into the
// unknown-failure cap and ended them as spawn_failed; and it retried every
// 2-3 s across several watches, which is what EC2's throttling penalises.
//
// Now a throttle is FailureThrottled: never terminal, never counted toward
// MaxConsecutiveFailures. Two pieces of shared state keep the poller from
// hammering:
//
//   - a per-region backoff. A throttle that names no AZ backs the whole region
//     off, exponentially with jitter, for every watch in the account: within
//     a sweep the region is not tried again at all, and across sweeps it is
//     tried again only once the backoff has passed.
//   - an AZ cool-off. A throttle that names an AZ ID is mapped to the
//     account's AZ name, and that AZ is skipped by every watch for
//     AZCooloffDuration while its other AZs and regions are still tried.
//
// Both live in a ThrottleBook the poller shares with its spawner and holder.
// The hosted poller is stateless across invocations, so the book is rebuilt at
// the start of each sweep from the copy every active watch carries
// (Watch.Throttle, Watch.AZCooloffs), and written back onto a watch whenever
// one of its attempts changed it. This needs only dynamodb:UpdateItem, which
// the poller already has; the AZ ID is mapped with ec2:DescribeSubnets, which
// it also already has.

const (
	// ThrottleBackoffBase is the backoff after the first throttle in a region.
	ThrottleBackoffBase = 30 * time.Second
	// ThrottleBackoffMax caps the per-region backoff.
	ThrottleBackoffMax = 10 * time.Minute
	// ThrottleBackoffResetAfter is how long a region must go without a
	// throttle before the next one starts the backoff from the base again.
	ThrottleBackoffResetAfter = 30 * time.Minute
	// AZCooloffDuration is how long an AZ named by an AZ-scoped throttle is
	// skipped. EC2 lifts such throttling when the AZ's operational issue is
	// resolved, which takes tens of minutes, not seconds.
	AZCooloffDuration = 45 * time.Minute
)

// Error classes (#254), the first word of LastError / LastPollError.
const (
	// ErrClassThrottled is an attempt EC2 throttled, or a cycle the poller
	// skipped to back off from one.
	ErrClassThrottled = "throttled"
	// ErrClassAZImpaired prefixes an attempt whose sweep skipped AZs that a
	// throttle named impaired, like ErrClassNoSubnet does for missing subnets.
	ErrClassAZImpaired = "az-impaired"
)

// RegionThrottle is one region's backoff after a throttle that named no AZ.
// Level counts the throttles in a row (each within ThrottleBackoffResetAfter
// of the last); the next attempt in the region waits until Until.
type RegionThrottle struct {
	Region string    `json:"region" dynamodbav:"region"`
	Code   string    `json:"code,omitempty" dynamodbav:"code,omitempty"`
	Level  int       `json:"level" dynamodbav:"level"`
	At     time.Time `json:"at" dynamodbav:"at"`
	Until  time.Time `json:"until" dynamodbav:"until"`
}

// Describe renders the backoff for last_poll_error, e.g. "us-west-2:
// RequestLimitExceeded, backing off until 23:41:07Z (throttle 3 in a row)".
func (r RegionThrottle) Describe() string {
	code := r.Code
	if code == "" {
		code = "throttled"
	}
	return fmt.Sprintf("%s: %s, backing off until %s (throttle %d in a row)",
		r.Region, code, r.Until.UTC().Format("15:04:05Z"), r.Level)
}

// AZCooloff is an AZ that an AZ-scoped throttle named impaired (#254). AZ is
// the account's AZ name (us-west-2a), AZID the zone ID EC2 named (usw2-az1).
// Either may be empty when it could not be determined, but not both.
type AZCooloff struct {
	Region string    `json:"region" dynamodbav:"region"`
	AZ     string    `json:"az,omitempty" dynamodbav:"az,omitempty"`
	AZID   string    `json:"az_id,omitempty" dynamodbav:"az_id,omitempty"`
	Reason string    `json:"reason,omitempty" dynamodbav:"reason,omitempty"`
	At     time.Time `json:"at" dynamodbav:"at"`
	Until  time.Time `json:"until" dynamodbav:"until"`
}

// Live reports whether the AZ is still being skipped at now.
func (c AZCooloff) Live(now time.Time) bool { return now.Before(c.Until) }

// Describe renders the cool-off, e.g. "us-west-2a (usw2-az1) until 00:07Z".
func (c AZCooloff) Describe() string {
	name := c.AZ
	switch {
	case name == "":
		name = c.AZID
	case c.AZID != "":
		name += " (" + c.AZID + ")"
	}
	return name + " until " + c.Until.UTC().Format("15:04Z")
}

func (c AZCooloff) key() string {
	if c.AZ != "" {
		return c.Region + "/" + c.AZ
	}
	return c.Region + "/id:" + c.AZID
}

// ThrottleBackoff is the nominal backoff for the level-th throttle in a row:
// ThrottleBackoffBase doubled per level, capped at ThrottleBackoffMax. The
// delay applied is jittered to between half of it and all of it.
func ThrottleBackoff(level int) time.Duration {
	d := ThrottleBackoffBase
	for i := 1; i < level && d < ThrottleBackoffMax; i++ {
		d *= 2
	}
	return min(d, ThrottleBackoffMax)
}

// ThrottleBook is the account's throttling state (#254), shared by a poller,
// its spawner and its holder. A nil *ThrottleBook is valid and records
// nothing, so a spawner or holder used without a poller behaves as before.
type ThrottleBook struct {
	mu      sync.Mutex
	regions map[string]RegionThrottle
	azs     map[string]AZCooloff
	// sweepStart is when the current sweep began. A region throttled since
	// then stays backed off for the rest of the sweep however short its
	// backoff, so one sweep never spins on a throttled region.
	sweepStart time.Time
	// changes counts updates, so a caller can tell whether an attempt changed
	// the book and so needs writing back to its watch.
	changes int
	// jitter picks the delay actually applied for a nominal backoff d. A seam
	// for tests; nil = a uniform pick in [d/2, d].
	jitter func(d time.Duration) time.Duration
}

// NewThrottleBook returns an empty book.
func NewThrottleBook() *ThrottleBook {
	return &ThrottleBook{regions: map[string]RegionThrottle{}, azs: map[string]AZCooloff{}}
}

func (b *ThrottleBook) init() {
	if b.regions == nil {
		b.regions = map[string]RegionThrottle{}
	}
	if b.azs == nil {
		b.azs = map[string]AZCooloff{}
	}
}

// equalJitter returns a uniform pick in [d/2, d], so the backoff still grows
// with the level while parallel pollers spread out.
func equalJitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(half)+1))
	if err != nil {
		return d
	}
	return half + time.Duration(n.Int64())
}

// StartSweep begins a sweep at now, merging in the state each watch carries.
// For a region the most recent throttle wins; for an AZ the latest cool-off.
func (b *ThrottleBook) StartSweep(now time.Time, watches []Watch) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.init()
	b.sweepStart = now
	for i := range watches {
		for _, r := range watches[i].Throttle {
			if prev, ok := b.regions[r.Region]; !ok || r.At.After(prev.At) {
				b.regions[r.Region] = r
			}
		}
		for _, c := range watches[i].AZCooloffs {
			if prev, ok := b.azs[c.key()]; !ok || c.Until.After(prev.Until) {
				b.azs[c.key()] = c
			}
		}
	}
}

// RegionBackoff reports whether region is backed off at now: its backoff has
// not passed, or it was throttled during the current sweep.
func (b *ThrottleBook) RegionBackoff(region string, now time.Time) (RegionThrottle, bool) {
	if b == nil {
		return RegionThrottle{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.regions[region]
	if !ok {
		return RegionThrottle{}, false
	}
	if now.Before(r.Until) || (!b.sweepStart.IsZero() && !r.At.Before(b.sweepStart)) {
		return r, true
	}
	return r, false
}

// NoteRegionThrottle records a throttle in region that named no AZ, and
// returns the backoff it earned: one level above the region's last throttle
// if that was within ThrottleBackoffResetAfter, otherwise level 1.
func (b *ThrottleBook) NoteRegionThrottle(region, code string, now time.Time) RegionThrottle {
	if b == nil {
		return RegionThrottle{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.init()
	level := 1
	if prev, ok := b.regions[region]; ok && now.Sub(prev.At) < ThrottleBackoffResetAfter {
		level = prev.Level + 1
	}
	jitter := b.jitter
	if jitter == nil {
		jitter = equalJitter
	}
	r := RegionThrottle{Region: region, Code: code, Level: level, At: now, Until: now.Add(jitter(ThrottleBackoff(level)))}
	b.regions[region] = r
	b.changes++
	return r
}

// CoolOffAZ records that az (an AZ name; azID its zone ID, either may be
// empty but not both) in region is impaired, and skips it until
// AZCooloffDuration from now, or longer if it was already cooling off. It is
// the API for any caller that learns an AZ is impaired, not only a throttle.
func (b *ThrottleBook) CoolOffAZ(region, az, azID, reason string, now time.Time) AZCooloff {
	c := AZCooloff{Region: region, AZ: az, AZID: azID, Reason: reason, At: now, Until: now.Add(AZCooloffDuration)}
	if b == nil || (az == "" && azID == "") {
		return c
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.init()
	if prev, ok := b.azs[c.key()]; ok && prev.Until.After(c.Until) {
		c.Until = prev.Until
	}
	b.azs[c.key()] = c
	b.changes++
	return c
}

// AZCooledOff reports whether az (an AZ name or zone ID) in region is cooling
// off at now.
func (b *ThrottleBook) AZCooledOff(region, az string, now time.Time) (AZCooloff, bool) {
	if b == nil || az == "" {
		return AZCooloff{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, c := range b.azs {
		if c.Region == region && (c.AZ == az || c.AZID == az) && c.Live(now) {
			return c, true
		}
	}
	return AZCooloff{}, false
}

// version is the book's update count.
func (b *ThrottleBook) version() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.changes
}

// snapshot returns what a watch should carry: every region throttled within
// ThrottleBackoffResetAfter (needed to escalate the next backoff) and every
// live AZ cool-off, sorted so the record is stable.
func (b *ThrottleBook) snapshot(now time.Time) ([]RegionThrottle, []AZCooloff) {
	if b == nil {
		return nil, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var rs []RegionThrottle
	for _, r := range b.regions {
		if now.Sub(r.At) < ThrottleBackoffResetAfter || now.Before(r.Until) {
			rs = append(rs, r)
		}
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Region < rs[j].Region })
	var cs []AZCooloff
	for _, c := range b.azs {
		if c.Live(now) {
			cs = append(cs, c)
		}
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].key() < cs[j].key() })
	return rs, cs
}

// AZCooloffError wraps an attempt whose sweep skipped (or newly cooled off)
// AZs that a throttle named impaired. It unwraps to the attempt's own error,
// so ClassifyFailure sees what it did before; only the heartbeat changes.
type AZCooloffError struct {
	Cooloffs []AZCooloff
	Err      error
}

func (e *AZCooloffError) Error() string {
	return fmt.Sprintf("impaired AZ(s) skipped: %s: %v", describeCooloffs(e.Cooloffs), e.Err)
}

func (e *AZCooloffError) Unwrap() error { return e.Err }

func describeCooloffs(cs []AZCooloff) string {
	parts := make([]string, 0, len(cs))
	seen := map[string]bool{}
	for _, c := range cs {
		if seen[c.key()] {
			continue
		}
		seen[c.key()] = true
		parts = append(parts, c.Describe())
	}
	return strings.Join(parts, ", ")
}

// noteThrottle records a throttled attempt in book. A throttle naming an AZ
// ID cools that AZ off (mapped to the account's AZ name with resolve; when it
// can't be mapped, the AZ that was attempted is taken to be the one named)
// and reports azScoped, so the caller moves on to the next AZ. Any other
// throttle backs the whole region off, and the caller stops.
func noteThrottle(ctx context.Context, book *ThrottleBook, resolve func(ctx context.Context, region, azID string) string,
	region, attempted string, err error, now time.Time) (cooled AZCooloff, azScoped bool) {
	if id := ThrottledAZID(err); id != "" {
		name := ""
		if resolve != nil {
			name = resolve(ctx, region, id)
		}
		if name == "" {
			name = attempted
		}
		if name != "" {
			return book.CoolOffAZ(region, name, id, errorCode(err), now), true
		}
	}
	book.NoteRegionThrottle(region, errorCode(err), now)
	return AZCooloff{}, false
}

// ── AZ ID → AZ name ───────────────────────────────────────────────────────────

// azIDResolver maps a zone ID to this account's AZ name, from the subnets in
// it (ec2:DescribeSubnets, already granted to the poller; it has no
// ec2:DescribeAvailabilityZones). An account with no subnet in an AZ cannot
// launch there, so an unmappable ID needs no cool-off by name. The mapping
// never changes for an account, so hits are cached for the container's life;
// misses are not cached.
type azIDResolver struct {
	mu       sync.Mutex
	describe func(ctx context.Context, region, azID string) (string, error)
	names    map[string]string
}

func newAZIDResolver(cfg aws.Config) *azIDResolver {
	return &azIDResolver{
		describe: func(ctx context.Context, region, azID string) (string, error) {
			return describeAZNameByID(ctx, ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.Region = region }), azID)
		},
		names: map[string]string{},
	}
}

// describeAZNameByID returns the AZ name of the zone ID azID, from any subnet
// in it, or "" when the account has none there.
func describeAZNameByID(ctx context.Context, api DescribeSubnetsAPI, azID string) (string, error) {
	out, err := api.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters:    []ec2types.Filter{{Name: aws.String("availability-zone-id"), Values: []string{azID}}},
		MaxResults: aws.Int32(5),
	})
	if err != nil {
		return "", err
	}
	for _, sn := range out.Subnets {
		if aws.ToString(sn.AvailabilityZoneId) == azID && aws.ToString(sn.AvailabilityZone) != "" {
			return aws.ToString(sn.AvailabilityZone), nil
		}
	}
	return "", nil
}

// resolve returns azID's AZ name in region, or "" when unknown.
func (r *azIDResolver) resolve(ctx context.Context, region, azID string) string {
	if r == nil || azID == "" {
		return ""
	}
	k := region + "/" + azID
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.names[k]; ok {
		return n
	}
	n, err := r.describe(ctx, region, azID)
	if err != nil || n == "" {
		return ""
	}
	r.names[k] = n
	return n
}

// ── Poller side ───────────────────────────────────────────────────────────────

// book returns the poller's ThrottleBook, creating it on first use and
// sharing it with the spawner and holder, so all three see one account state.
func (p *Poller) book() *ThrottleBook {
	if p.throttle == nil {
		p.throttle = NewThrottleBook()
	}
	if p.spawner != nil && p.spawner.throttle == nil {
		p.spawner.throttle = p.throttle
	}
	if p.holder != nil && p.holder.throttle == nil {
		p.holder.throttle = p.throttle
	}
	return p.throttle
}

// CoolOffAZ marks az (an AZ name; azID its zone ID if known) in region as
// impaired for AZCooloffDuration, for every watch this poller services, and
// writes the state onto w so the next sweep (a new Lambda invocation) still
// skips it. reason is a short word for the record, e.g. the AWS code or
// "early-death". It is the API for anything that learns an AZ is impaired:
// an AZ-scoped throttle (#254), or a launch that died in pending (#255).
func (p *Poller) CoolOffAZ(ctx context.Context, w *Watch, region, az, azID, reason string) AZCooloff {
	now := p.clock()
	c := p.book().CoolOffAZ(region, az, azID, reason, now)
	if w != nil {
		p.persistThrottle(ctx, w, now)
	}
	return c
}

// AZCooledOff reports whether az (an AZ name or zone ID) in region is cooling
// off now, and the cool-off if so.
func (p *Poller) AZCooledOff(region, az string) (AZCooloff, bool) {
	return p.book().AZCooledOff(region, az, p.clock())
}

// persistThrottle writes the book onto w (in memory and in the store), so the
// state outlives this invocation. Best-effort, like the heartbeat.
func (p *Poller) persistThrottle(ctx context.Context, w *Watch, now time.Time) {
	w.Throttle, w.AZCooloffs = p.book().snapshot(now)
	if err := p.store.setThrottleState(ctx, w.WatchID, w.Throttle, w.AZCooloffs, nil, ""); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record throttle state for %s: %v\n", w.WatchID, err)
	}
}

// candidateThrottled reports why m must not be tried now (#254): its region
// is backing off, or every AZ it offers is cooling off. Only actions that
// call EC2 (spawn, hold) are held back; a notify watch makes no call.
func (p *Poller) candidateThrottled(w *Watch, m *MatchResult, now time.Time) (string, bool) {
	if w.Action != ActionSpawn && w.Action != ActionHold {
		return "", false
	}
	book := p.book()
	if r, ok := book.RegionBackoff(m.Region, now); ok {
		return r.Describe(), true
	}
	azs := m.CandidateAZs
	if len(azs) == 0 {
		azs = []string{m.AvailabilityZone}
	}
	var cooled []AZCooloff
	for _, az := range azs {
		c, ok := book.AZCooledOff(m.Region, az, now)
		if !ok {
			return "", false
		}
		cooled = append(cooled, c)
	}
	return fmt.Sprintf("%s: every offered AZ is impaired: %s", m.Region, describeCooloffs(cooled)), true
}

// skipThrottled records a cycle in which w made no attempt because every
// candidate is backing off from a throttle (#254). The watch stays active;
// last_poll_error says why it is waiting, and the book is written onto it.
func (p *Poller) skipThrottled(ctx context.Context, w *Watch, reason string, summary *PollSummary) {
	now := p.clock()
	msg := truncateBytes(ErrClassThrottled+": "+oneLine(reason), MaxLastErrorBytes)
	if p.verbose {
		fmt.Fprintf(os.Stderr, "Watch %s: skipped this cycle, %s\n", w.WatchID, msg)
	}
	w.Throttle, w.AZCooloffs = p.book().snapshot(now)
	if err := p.store.setThrottleState(ctx, w.WatchID, w.Throttle, w.AZCooloffs, nil, msg); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to record throttle backoff for %s: %v\n", w.WatchID, err)
	} else {
		w.LastPollError, w.LastPollErrorAt = msg, now
	}
	summary.Retrying++
	summary.Throttled++
}

// ── Store side ────────────────────────────────────────────────────────────────

// setThrottleState writes a watch's copy of the throttle state (#254) and
// touches last_polled_at, in one UpdateItem on an existing record. An empty
// list removes its attribute. a, when non-nil, is a throttled attempt's
// heartbeat (attempts, last_attempt_at, last_error), and leaves
// consecutive_failures alone: a throttle neither proves the config works nor
// counts against it. pollErr, when set, is a skipped cycle's last_poll_error.
func (s *Store) setThrottleState(ctx context.Context, watchID string, rs []RegionThrottle, cs []AZCooloff, a *attempt, pollErr string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	values := map[string]types.AttributeValue{
		":now": &types.AttributeValueMemberS{Value: now},
	}
	set := []string{"last_polled_at = :now"}
	var remove []string
	if len(rs) == 0 {
		remove = append(remove, "throttle")
	} else {
		v, err := attributevalue.Marshal(rs)
		if err != nil {
			return fmt.Errorf("marshal throttle state: %w", err)
		}
		set = append(set, "throttle = :rt")
		values[":rt"] = v
	}
	if len(cs) == 0 {
		remove = append(remove, "az_cooloff")
	} else {
		v, err := attributevalue.Marshal(cs)
		if err != nil {
			return fmt.Errorf("marshal AZ cool-offs: %w", err)
		}
		set = append(set, "az_cooloff = :az")
		values[":az"] = v
	}
	if pollErr != "" {
		set = append(set, "last_poll_error = :perr", "last_poll_error_at = :now")
		values[":perr"] = &types.AttributeValueMemberS{Value: pollErr}
	}
	set, add := withAttempt(set, nil, values, a)
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		// Only an existing watch, as for recordPollError: never create a stub.
		ConditionExpression:       aws.String("attribute_exists(#st)"),
		UpdateExpression:          aws.String(updateExpr(remove, set, add)),
		ExpressionAttributeNames:  map[string]string{"#st": "status"},
		ExpressionAttributeValues: values,
	})
	if err != nil {
		var cfe *types.ConditionalCheckFailedException
		if errors.As(err, &cfe) {
			return nil // the watch is gone; nothing to record
		}
		return fmt.Errorf("set throttle state: %w", err)
	}
	return nil
}

// errThrottledSkip is the error for an attempt that made no AWS call because
// every AZ it could use is cooling off (#254).
func errThrottledSkip(region string, cs []AZCooloff) error {
	return fmt.Errorf("%w: every candidate AZ in %s is impaired (%s)", failure.ErrThrottled, region, describeCooloffs(cs))
}
