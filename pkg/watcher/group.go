package watcher

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Watch groups (#287): any one of several different searches will do, and the
// first to land makes the rest redundant.
//
// A watch joins a group with `lagotto watch … --group <name>`. Members may
// differ in everything a watch has (pattern, regions, Spot, spawn or hold,
// spawn config). The group is one item in the watches table:
//
//	watch_id = "g-<name>"   (GroupKey; watch ids are "w-…", so they can't collide)
//
// It deliberately carries NO `status` and NO `user_id` attribute. Those are
// the hash keys of the table's two GSIs (status-index, user_id-index), and an
// item without a GSI's key attribute is not in that index. So every poller
// that loads active watches (ListActiveWatches, status-index), every `lagotto
// list` (user_id-index) and spawn's dashboard (user_id-index) never sees a
// group item, including the ones that predate groups. Its state is
// group_state and its owner group_owner instead. The table and its IAM are
// unchanged: no new table, so nothing new for teardown, doctor or the runtime
// policy. Its deleter is the table's DynamoDB TTL on ttl_timestamp (the
// retention horizon past its longest-lived member, as for a watch record) and
// `lagotto teardown`, which deletes the table.
//
// The protocol, all conditional writes on that one item:
//
//   - Slot. Before RunInstances (or CreateCapacityReservation) a member claims
//     the group's slot (ReserveGroupSlot): the group must be hunting and the
//     slot free, already this member's, or expired. A member that cannot claim
//     it launches nothing this cycle. The slot is held while the member's
//     launch is landing and released when it fails, dies in pending (#255) or
//     times out, so the group stays armed.
//   - Win. The winner is decided at landing (`running`, or an `active`
//     reservation; #252), not at RunInstances: CompleteGroup moves the group
//     from hunting to landed, conditionally, naming the winner. Exactly one
//     member can make that write.
//   - Siblings. The winner then cancels every other active member, each with a
//     tombstone (cancelled_reason: "group <name> landed by w-… (i-…, type,
//     AZ)") and a group_cancelled history row, releases a sibling's pending
//     reservation and, by its --group-on-extra policy, terminates a sibling's
//     pending instance. One notification goes out for the group.
//   - Convergence. The group item is the atomic decision; the sibling writes
//     follow it. A sibling whose cancel write was missed can never launch
//     again, because claiming the slot needs a hunting group, and the next
//     sweep that sees it cancels it the same way.
//   - Extra landings. A member whose launch lands after the group was won (its
//     slot had expired, or an older poller launched it) lost the race. By its
//     --group-on-extra policy the instance is terminated through spawn's
//     terminate path (the poller's existing, tag-gated grant), or the
//     reservation released, or (keep) left alone; either way the member is
//     cancelled with a tombstone and notified.
//
// Pollers older than MinGroupPollerVersion ignore every group field, so each
// member they poll is an independent watch, exactly as before. `lagotto watch
// --group` warns when the deployed poller is older.

// GroupKeyPrefix starts the watch_id of a group item.
const GroupKeyPrefix = "g-"

// MinGroupPollerVersion is the first poller release that honours watch groups.
const MinGroupPollerVersion = "0.69.0"

// GroupState is a group's lifecycle state.
type GroupState string

const (
	// GroupHunting: no member has landed; members may claim the slot.
	GroupHunting GroupState = "hunting"
	// GroupLanded: a member landed and won; the others are cancelled.
	GroupLanded GroupState = "landed"
	// GroupCancelled: the owner cancelled the group (lagotto cancel <group>).
	GroupCancelled GroupState = "cancelled"
)

// --group-on-extra policies: what happens to a launch that landed after its
// group was already won.
const (
	GroupOnExtraTerminate = "terminate"
	GroupOnExtraKeep      = "keep"
)

// History actions written by groups.
const (
	// ActionGroupCancelled: a member cancelled because its group completed (or
	// was cancelled). An audit row: it acquired nothing.
	ActionGroupCancelled = "group_cancelled"
	// ActionGroupExtra: a member whose launch landed after its group was won.
	ActionGroupExtra = "group_extra"
)

// groupSlotTTL bounds how long a member holds the slot without releasing it.
// It covers a landing's whole life (PendingLandingTimeout) plus the landing
// marker's lapse, so the slot only expires when no current poller has looked
// at the landing member for that long, or when a poller crashed between
// claiming the slot and launching.
const groupSlotTTL = PendingLandingTimeout + landingMarkerTTL

// WatchGroup is the group item in the watches table.
type WatchGroup struct {
	Key   string     `json:"-" dynamodbav:"watch_id"`
	Name  string     `json:"name" dynamodbav:"group_name"`
	Owner string     `json:"owner" dynamodbav:"group_owner"`
	State GroupState `json:"state" dynamodbav:"group_state"`
	// Generation counts how often the name was re-armed after it ended, so a
	// re-arm is conditional on the generation it read.
	Generation int `json:"generation,omitempty" dynamodbav:"group_generation,omitempty"`
	// Members are the watch ids that joined in this generation.
	Members []string `json:"members,omitempty" dynamodbav:"group_members,stringset,omitempty"`
	// SlotWatch holds the slot until SlotExpiresAt (RFC3339 UTC, so it
	// compares lexically in a condition).
	SlotWatch     string `json:"slot_watch,omitempty" dynamodbav:"slot_watch,omitempty"`
	SlotExpiresAt string `json:"slot_expires_at,omitempty" dynamodbav:"slot_expires_at,omitempty"`
	// The winner, once landed.
	WinnerWatch         string     `json:"winner_watch,omitempty" dynamodbav:"winner_watch,omitempty"`
	WinnerInstanceID    string     `json:"winner_instance_id,omitempty" dynamodbav:"winner_instance_id,omitempty"`
	WinnerReservationID string     `json:"winner_reservation_id,omitempty" dynamodbav:"winner_reservation_id,omitempty"`
	WinnerType          string     `json:"winner_type,omitempty" dynamodbav:"winner_type,omitempty"`
	WinnerRegion        string     `json:"winner_region,omitempty" dynamodbav:"winner_region,omitempty"`
	WinnerAZ            string     `json:"winner_az,omitempty" dynamodbav:"winner_az,omitempty"`
	EndedAt             *time.Time `json:"ended_at,omitempty" dynamodbav:"ended_at,omitempty"`
	// EndReason says why a cancelled group ended.
	EndReason    string    `json:"end_reason,omitempty" dynamodbav:"end_reason,omitempty"`
	CreatedAt    time.Time `json:"created_at" dynamodbav:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" dynamodbav:"updated_at"`
	TTLTimestamp int64     `json:"-" dynamodbav:"ttl_timestamp"`
}

// GroupKey is the watch_id of the group item for name.
func GroupKey(name string) string { return GroupKeyPrefix + name }

// LandedSummary is "w-… (i-…, type, AZ)" for a landed group, or "".
func (g *WatchGroup) LandedSummary() string {
	if g == nil || g.WinnerWatch == "" {
		return ""
	}
	parts := nonEmpty(g.WinnerInstanceID+g.WinnerReservationID, g.WinnerType, firstNonEmpty(g.WinnerAZ, g.WinnerRegion))
	if len(parts) == 0 {
		return g.WinnerWatch
	}
	return g.WinnerWatch + " (" + strings.Join(parts, ", ") + ")"
}

// CancelReason is the tombstone a sibling gets when the group ends.
func (g *WatchGroup) CancelReason() string {
	switch g.State {
	case GroupLanded:
		return fmt.Sprintf("group %s landed by %s", g.Name, g.LandedSummary())
	case GroupCancelled:
		if g.EndReason != "" {
			return fmt.Sprintf("group %s cancelled: %s", g.Name, g.EndReason)
		}
		return fmt.Sprintf("group %s cancelled", g.Name)
	}
	return fmt.Sprintf("group %s ended", g.Name)
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

var groupNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidateGroupName checks a --group name: 1-64 letters, digits, '.', '_' or
// '-', starting with a letter or digit, and not starting like a watch (w-),
// scheduled launch (sl-) or group key (g-), so `lagotto status` and `cancel`
// can tell a group name from an id.
func ValidateGroupName(name string) error {
	if !groupNameRE.MatchString(name) {
		return fmt.Errorf("invalid --group %q: use 1-64 letters, digits, '.', '_' or '-', starting with a letter or digit", name)
	}
	for _, p := range []string{"w-", "sl-", GroupKeyPrefix} {
		if strings.HasPrefix(strings.ToLower(name), p) {
			return fmt.Errorf("invalid --group %q: a group name cannot start with %q (that prefix is a watch, scheduled launch or group id)", name, p)
		}
	}
	return nil
}

// ParseGroupOnExtra validates --group-on-extra; "" is the default, terminate.
func ParseGroupOnExtra(s string) (string, error) {
	switch s {
	case "", GroupOnExtraTerminate:
		return GroupOnExtraTerminate, nil
	case GroupOnExtraKeep:
		return GroupOnExtraKeep, nil
	case "hold":
		return "", fmt.Errorf("--group-on-extra hold is not supported yet (lagotto#287 follow-up); use terminate or keep")
	}
	return "", fmt.Errorf("invalid --group-on-extra %q: must be terminate or keep", s)
}

// GroupOnExtraPolicy is the member's --group-on-extra, defaulting to terminate.
func (w *Watch) GroupOnExtraPolicy() string {
	if w.GroupOnExtra == GroupOnExtraKeep {
		return GroupOnExtraKeep
	}
	return GroupOnExtraTerminate
}

// ValidateGroupMember checks that w can be a group member. Groups are for
// single-shot launches and holds: the only watches with a landing to win.
func ValidateGroupMember(w *Watch) error {
	if w.Group == "" {
		if w.GroupOnExtra != "" {
			return errors.New("--group-on-extra needs --group")
		}
		return nil
	}
	if err := ValidateGroupName(w.Group); err != nil {
		return err
	}
	if _, err := ParseGroupOnExtra(w.GroupOnExtra); err != nil {
		return err
	}
	switch {
	case w.Action == ActionNotify:
		return errors.New("--group needs --action spawn or hold: a notify watch launches and reserves nothing, so it has no landing to win (and notify is deprecated, lagotto#283)")
	case normalizeService(w.Service) == ServiceSageMaker:
		return errors.New("--group is not supported with --service sagemaker yet: only EC2 spawn and hold watches can be group members")
	case w.DesiredCount > 0:
		return errors.New("--group is not supported with --maintain: a fleet keeps launching toward its goal, so it has no single landing to win")
	case w.UntilLanded:
		return errors.New("--group is not supported with --until-landed yet: a group would need one acknowledgement and reminder clock for all its members (lagotto#287 follow-up)")
	}
	return nil
}

// groupManaged reports whether the poller applies the group protocol to w.
// Anything else (an older record, or a combination `watch` refuses) is polled
// as an independent watch.
func groupManaged(w *Watch) bool {
	return w.Group != "" && w.DesiredCount == 0 && !w.UntilLanded &&
		normalizeService(w.Service) != ServiceSageMaker &&
		(w.Action == ActionSpawn || w.Action == ActionHold)
}

// ── errors ──

var (
	// ErrGroupSlotHeld: another member holds the slot; launch nothing this cycle.
	ErrGroupSlotHeld = errors.New("group slot held by another member")
	// ErrGroupClosed: the group is no longer hunting (landed or cancelled).
	ErrGroupClosed = errors.New("group is no longer hunting")
	// ErrGroupMissing: the group item does not exist.
	ErrGroupMissing = errors.New("group not found")
	// ErrGroupNotOwned: the group belongs to another caller.
	ErrGroupNotOwned = errors.New("group belongs to another owner")
)

func isConditionFailed(err error) bool {
	var cond *types.ConditionalCheckFailedException
	return errors.As(err, &cond)
}

func groupKeyAttr(name string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: GroupKey(name)}}
}

func avS(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ── store ──

// GetGroup reads a group (strongly consistent), or nil when it does not exist.
func (st *Store) GetGroup(ctx context.Context, name string) (*WatchGroup, error) {
	out, err := st.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &st.watchesTable,
		Key:            groupKeyAttr(name),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("get group %s: %w", name, err)
	}
	if out.Item == nil {
		return nil, nil
	}
	var g WatchGroup
	if err := attributevalue.UnmarshalMap(out.Item, &g); err != nil {
		return nil, fmt.Errorf("unmarshal group %s: %w", name, err)
	}
	sort.Strings(g.Members)
	return &g, nil
}

// JoinGroup adds watchID to the group name owned by owner, creating the group
// (hunting) if it does not exist, and re-arming it if it ended (a new
// generation: the old members, winner and slot are dropped). ttl is the joining
// watch's retention TTL; the group keeps the largest it has seen. A group owned
// by someone else is ErrGroupNotOwned.
func (st *Store) JoinGroup(ctx context.Context, name, owner, watchID string, ttl int64) (*WatchGroup, error) {
	g, err := st.GetGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	if g != nil && g.Owner != owner {
		return nil, fmt.Errorf("%w: group %s", ErrGroupNotOwned, name)
	}
	if g != nil && g.State != GroupHunting {
		if err := st.rearmGroup(ctx, name, g.Generation); err != nil {
			return nil, err
		}
	}
	if g != nil && g.TTLTimestamp > ttl {
		ttl = g.TTLTimestamp
	}
	now := time.Now().UTC()
	_, err = st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           &st.watchesTable,
		Key:                 groupKeyAttr(name),
		ConditionExpression: aws.String("attribute_not_exists(watch_id) OR (group_owner = :o AND group_state = :h)"),
		UpdateExpression: aws.String("SET group_name = :n, group_owner = if_not_exists(group_owner, :o), " +
			"group_state = if_not_exists(group_state, :h), created_at = if_not_exists(created_at, :now), " +
			"updated_at = :now, ttl_timestamp = :ttl ADD group_members :m"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":n":   avS(name),
			":o":   avS(owner),
			":h":   avS(string(GroupHunting)),
			":now": avS(now.Format(time.RFC3339Nano)),
			":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
			":m":   &types.AttributeValueMemberSS{Value: []string{watchID}},
		},
	})
	if err != nil {
		if isConditionFailed(err) {
			// Another caller took the name, or it ended, between our read and write.
			g, gerr := st.GetGroup(ctx, name)
			if gerr == nil && g != nil && g.Owner != owner {
				return nil, fmt.Errorf("%w: group %s", ErrGroupNotOwned, name)
			}
			return nil, fmt.Errorf("join group %s: it changed while joining; try again", name)
		}
		return nil, fmt.Errorf("join group %s: %w", name, err)
	}
	return st.GetGroup(ctx, name)
}

// rearmGroup returns an ended group to hunting as a new generation.
func (st *Store) rearmGroup(ctx context.Context, name string, generation int) error {
	cond := "group_generation = :g"
	values := map[string]types.AttributeValue{
		":g":   &types.AttributeValueMemberN{Value: strconv.Itoa(generation)},
		":g1":  &types.AttributeValueMemberN{Value: strconv.Itoa(generation + 1)},
		":h":   avS(string(GroupHunting)),
		":now": avS(time.Now().UTC().Format(time.RFC3339Nano)),
	}
	if generation == 0 {
		cond = "(attribute_not_exists(group_generation) OR group_generation = :g)"
	}
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           &st.watchesTable,
		Key:                 groupKeyAttr(name),
		ConditionExpression: aws.String(cond + " AND group_state <> :h"),
		UpdateExpression: aws.String("SET group_state = :h, group_generation = :g1, updated_at = :now " +
			"REMOVE group_members, slot_watch, slot_expires_at, winner_watch, winner_instance_id, winner_reservation_id, " +
			"winner_type, winner_region, winner_az, ended_at, end_reason"),
		ExpressionAttributeValues: values,
	})
	if err != nil && !isConditionFailed(err) {
		return fmt.Errorf("re-arm group %s: %w", name, err)
	}
	// A failed condition means someone else re-armed it first: join that one.
	return nil
}

// ReserveGroupSlot claims the group's slot for watchID until now+groupSlotTTL.
// It succeeds when the group is hunting and the slot is free, already
// watchID's, or expired. Otherwise it returns ErrGroupSlotHeld (with the
// holder), ErrGroupClosed or ErrGroupMissing, and the group as read.
func (st *Store) ReserveGroupSlot(ctx context.Context, name, watchID string, now time.Time) (*WatchGroup, error) {
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           &st.watchesTable,
		Key:                 groupKeyAttr(name),
		ConditionExpression: aws.String("group_state = :h AND (attribute_not_exists(slot_watch) OR slot_watch = :me OR slot_expires_at < :now)"),
		UpdateExpression:    aws.String("SET slot_watch = :me, slot_expires_at = :exp"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":h":   avS(string(GroupHunting)),
			":me":  avS(watchID),
			":now": avS(rfc3339(now)),
			":exp": avS(rfc3339(now.Add(groupSlotTTL))),
		},
	})
	if err == nil {
		return nil, nil
	}
	if !isConditionFailed(err) {
		return nil, fmt.Errorf("reserve group %s slot: %w", name, err)
	}
	g, gerr := st.GetGroup(ctx, name)
	switch {
	case gerr != nil:
		return nil, gerr
	case g == nil:
		return nil, fmt.Errorf("%w: %s", ErrGroupMissing, name)
	case g.State != GroupHunting:
		return g, fmt.Errorf("%w: %s", ErrGroupClosed, g.CancelReason())
	}
	return g, fmt.Errorf("%w (%s, until %s)", ErrGroupSlotHeld, g.SlotWatch, g.SlotExpiresAt)
}

// ReleaseGroupSlot frees the slot if watchID holds it. Not holding it is not an
// error.
func (st *Store) ReleaseGroupSlot(ctx context.Context, name, watchID string) error {
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &st.watchesTable,
		Key:                       groupKeyAttr(name),
		ConditionExpression:       aws.String("slot_watch = :me"),
		UpdateExpression:          aws.String("REMOVE slot_watch, slot_expires_at"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":me": avS(watchID)},
	})
	if err != nil && !isConditionFailed(err) {
		return fmt.Errorf("release group %s slot: %w", name, err)
	}
	return nil
}

// CompleteGroup records m (w's landing) as the group's win, if the group is
// still hunting. It returns the group as it now stands and whether w won. A
// group that already landed or was cancelled is (group, false, nil).
func (st *Store) CompleteGroup(ctx context.Context, w *Watch, m *MatchResult, now time.Time) (*WatchGroup, bool, error) {
	values := map[string]types.AttributeValue{
		":h":   avS(string(GroupHunting)),
		":l":   avS(string(GroupLanded)),
		":w":   avS(w.WatchID),
		":now": avS(now.UTC().Format(time.RFC3339Nano)),
	}
	set := []string{"group_state = :l", "winner_watch = :w", "ended_at = :now", "updated_at = :now"}
	for attr, v := range map[string]string{
		"winner_instance_id": m.InstanceID, "winner_reservation_id": m.ReservationID,
		"winner_type": m.InstanceType, "winner_region": m.Region, "winner_az": m.AvailabilityZone,
	} {
		if v != "" {
			values[":"+attr] = avS(v)
			set = append(set, attr+" = :"+attr)
		}
	}
	sort.Strings(set)
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 &st.watchesTable,
		Key:                       groupKeyAttr(w.Group),
		ConditionExpression:       aws.String("group_state = :h"),
		UpdateExpression:          aws.String("SET " + strings.Join(set, ", ") + " REMOVE slot_watch, slot_expires_at"),
		ExpressionAttributeValues: values,
	})
	won := err == nil
	if err != nil && !isConditionFailed(err) {
		return nil, false, fmt.Errorf("complete group %s: %w", w.Group, err)
	}
	g, gerr := st.GetGroup(ctx, w.Group)
	if gerr != nil {
		if won {
			// The win is durable; the caller only lacks the member list.
			return &WatchGroup{Name: w.Group, State: GroupLanded, WinnerWatch: w.WatchID}, true, nil
		}
		return nil, false, gerr
	}
	if g == nil {
		return nil, false, fmt.Errorf("%w: %s", ErrGroupMissing, w.Group)
	}
	return g, won, nil
}

// CloseGroup ends a hunting group as cancelled, recording reason. A group that
// already ended is returned unchanged with ok false.
func (st *Store) CloseGroup(ctx context.Context, name, reason string) (*WatchGroup, bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           &st.watchesTable,
		Key:                 groupKeyAttr(name),
		ConditionExpression: aws.String("group_state = :h"),
		UpdateExpression:    aws.String("SET group_state = :c, end_reason = :r, ended_at = :now, updated_at = :now REMOVE slot_watch, slot_expires_at"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":h": avS(string(GroupHunting)), ":c": avS(string(GroupCancelled)), ":r": avS(reason), ":now": avS(now),
		},
	})
	ok := err == nil
	if err != nil && !isConditionFailed(err) {
		return nil, false, fmt.Errorf("cancel group %s: %w", name, err)
	}
	g, gerr := st.GetGroup(ctx, name)
	if gerr != nil {
		return nil, ok, gerr
	}
	return g, ok, nil
}

// CancelGroupMember cancels an ACTIVE watch because its group ended: status
// cancelled, the tombstone reason, the retention TTL, and the landing state
// dropped (the caller has dealt with any pending resource). It returns false,
// with no error, when the watch is not active any more (it matched, failed or
// was cancelled first).
func (st *Store) CancelGroupMember(ctx context.Context, watchID, reason string) (bool, error) {
	now := time.Now().UTC()
	_, err := st.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &st.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": avS(watchID),
		},
		ConditionExpression: aws.String("#st = :active"),
		UpdateExpression: aws.String("SET #st = :c, cancelled_reason = :r, updated_at = :now, ttl_timestamp = :ttl " +
			"REMOVE " + pendingRemove),
		ExpressionAttributeNames: map[string]string{"#st": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":active": avS(string(StatusActive)),
			":c":      avS(string(StatusCancelled)),
			":r":      avS(reason),
			":now":    avS(rfc3339(now)),
			":ttl":    &types.AttributeValueMemberN{Value: strconv.FormatInt(RetentionTTL(now), 10)},
		},
	})
	if err != nil {
		if isConditionFailed(err) {
			return false, nil
		}
		return false, fmt.Errorf("cancel group member %s: %w", watchID, err)
	}
	return true, nil
}

// GroupMembers loads the group's member watches that still exist, in id order.
func (st *Store) GroupMembers(ctx context.Context, g *WatchGroup) ([]*Watch, error) {
	var out []*Watch
	for _, id := range g.Members {
		w, err := st.GetWatch(ctx, id)
		if err != nil {
			return out, err
		}
		if w != nil {
			out = append(out, w)
		}
	}
	return out, nil
}

// GroupExposure is a group's cost exposure (#269): only one member is meant to
// land, so it is the largest member's, plus every member's when extras are
// kept.
type GroupExposure struct {
	// Max is the largest member exposure by hourly rate (nil when no member
	// could be priced), and MaxWatch its member.
	Max      *Exposure `json:"max,omitempty"`
	MaxWatch string    `json:"max_watch,omitempty"`
	// KeepHourlyUSD is the sum of the hourly rates of every hunting member, set
	// when at least one of them keeps an extra landing (--group-on-extra keep):
	// what the group can cost if they all land.
	KeepHourlyUSD float64  `json:"keep_hourly_usd,omitempty"`
	KeepWatches   []string `json:"keep_watches,omitempty"`
	Note          string   `json:"note,omitempty"`
}

// ComputeGroupExposure derives a GroupExposure from members whose Exposure is
// already filled. Only active members count: the others cannot land.
func ComputeGroupExposure(members []*Watch) GroupExposure {
	var ge GroupExposure
	var sum float64
	for _, w := range members {
		if w.Status != StatusActive || w.Exposure == nil {
			continue
		}
		e := w.Exposure
		if ge.Max == nil || e.HourlyUSD > ge.Max.HourlyUSD {
			ge.Max, ge.MaxWatch = e, w.WatchID
		}
		sum += e.HourlyUSD
		if w.GroupOnExtraPolicy() == GroupOnExtraKeep {
			ge.KeepWatches = append(ge.KeepWatches, w.WatchID)
		}
	}
	if len(ge.KeepWatches) > 0 {
		ge.KeepHourlyUSD = sum
		ge.Note = fmt.Sprintf("%s keep(s) an extra landing (--group-on-extra keep): if every active member lands, up to $%.2f/hr in total",
			strings.Join(ge.KeepWatches, ", "), sum)
	}
	return ge
}
