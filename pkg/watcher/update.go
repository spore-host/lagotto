package watcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Updating a watch in place (`lagotto update`, #257).
//
// An update is one conditional UpdateItem, never a PutItem: the poller writes
// heartbeat fields (attempts, last_error, last_polled_at, ...) with their own
// UpdateItems, and a whole-item put would clobber whatever it wrote since the
// watch was read. The condition makes it optimistic: the write only lands if
// the record's updated_at is still the value read before editing, the watch is
// still in the status it was read in, it is not landing (#252), and no poller
// holds a live lease on it (#47).

// The attributes `lagotto update` may change. Everything else (action, service,
// user_id, the watch ID, the deadline, the until-landed settings) is immutable
// to it.
const (
	FieldInstanceTypePattern = "instance_type_pattern"
	FieldRegions             = "regions"
	FieldAvailabilityZones   = "availability_zones"
	FieldSpot                = "spot"
	FieldMaxPrice            = "max_price"
	FieldNotifyChannels      = "notify_channels"
	FieldLaunchConfig        = "launch_config_json"
	FieldProject             = "project"
	// FieldOwnerID is written only to fill in the owner id of a watch created
	// before it was recorded (the caller's own, after the ARN check passed);
	// never to change one that is set.
	FieldOwnerID = "owner_id"
)

// updatableFields is every attribute an update may write.
var updatableFields = map[string]bool{
	FieldInstanceTypePattern: true,
	FieldRegions:             true,
	FieldAvailabilityZones:   true,
	FieldSpot:                true,
	FieldMaxPrice:            true,
	FieldNotifyChannels:      true,
	FieldLaunchConfig:        true,
	FieldProject:             true,
	FieldOwnerID:             true,
}

// IsSearchField reports whether changing field changes what the watch hunts
// for, and so invalidates the state the poller derived from the old search
// (consecutive failures, the quota-infeasible candidates and the quota cap).
// Notify channels and the project label do not.
func IsSearchField(field string) bool {
	switch field {
	case FieldInstanceTypePattern, FieldRegions, FieldAvailabilityZones, FieldSpot, FieldMaxPrice, FieldLaunchConfig:
		return true
	}
	return false
}

// derivedSearchState are the attributes the poller derives from a watch's
// search, cleared when the search changes. attempts, last_error, match_count
// and the history are kept, so the heartbeat stays truthful.
var derivedSearchState = []string{
	"consecutive_failures",
	"quota_infeasible",
	"quota_capped",
	"quota_capped_count",
	"quota_capped_at",
	"quota_cap_reason",
	"quota_cap_limit",
	"quota_cap_region",
}

// UpdateLeasePrefix marks the lease an update leaves on the watch. The poller
// acts on the watch as it listed it, before it claims its lease, so a poller
// that listed the old search just before an update could otherwise claim the
// lease just after it and act on (and RecordMatch could write back) the old
// search. The update therefore writes a lease of the poller's own length in the
// same UpdateItem: that poller's claim fails and it skips the watch, and the
// next cycle lists the new search. The lease simply ages out; a later update
// may take it over.
const UpdateLeasePrefix = "update:"

// UpdateLeaseTTL is how long the update's lease fences the watch: the poller's
// own lease length.
const UpdateLeaseTTL = leaseTTL

// ErrUpdateConflict is returned by UpdateWatchFields when its condition failed:
// the watch changed since it was read, a poller holds its lease, it started
// landing, or its status changed. Re-read it to say which.
var ErrUpdateConflict = errors.New("the watch changed while it was being updated")

// WatchUpdate describes one in-place update of a watch.
type WatchUpdate struct {
	// Fields are the attributes to write, taken from the watch passed to
	// UpdateWatchFields. One the watch omits (an omitempty zero value) is
	// REMOVEd, so the record ends up exactly as PutWatch would have written it.
	Fields []string
	// Version is the raw updated_at the watch was read with ("" when the record
	// has none). The write lands only if it is unchanged.
	Version string
	// Status is the status the watch was read in; the write lands only if it
	// still has it.
	Status WatchStatus
	// Reactivate makes an expired or failed watch active again, dropping
	// expired_reason and moving its retention horizon back to its expiry.
	Reactivate bool
	// Owner names the caller in the fencing lease (UpdateLeasePrefix + Owner).
	Owner string
}

// GetWatchVersioned is GetWatch, strongly consistent, also returning the raw
// updated_at attribute ("" when absent): the version an UpdateWatchFields
// condition compares against. The raw string is used, not a re-formatted time,
// because another writer may have stored it in a different (equally valid)
// RFC 3339 form.
func (s *Store) GetWatchVersioned(ctx context.Context, watchID string) (*Watch, string, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: watchID},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, "", fmt.Errorf("get watch: %w", err)
	}
	if result.Item == nil {
		return nil, "", nil
	}
	var w Watch
	if err := attributevalue.UnmarshalMap(result.Item, &w); err != nil {
		return nil, "", fmt.Errorf("unmarshal watch: %w", err)
	}
	version := ""
	if v, ok := result.Item["updated_at"].(*types.AttributeValueMemberS); ok {
		version = v.Value
	}
	return &w, version, nil
}

// UpdateWatchFields writes u.Fields of w to its record with one conditional
// UpdateItem (see the top of this file), sets updated_at to now, and fences the
// watch with an update lease. When any field is a search field it also clears
// the derived search state. It returns ErrUpdateConflict when the condition
// fails, having written nothing.
func (s *Store) UpdateWatchFields(ctx context.Context, w *Watch, u WatchUpdate, now time.Time) error {
	if len(u.Fields) == 0 {
		return fmt.Errorf("update watch %s: no fields to update", w.WatchID)
	}
	item, err := attributevalue.MarshalMap(w)
	if err != nil {
		return fmt.Errorf("marshal watch: %w", err)
	}
	now = now.UTC()
	nowS := now.Format(time.RFC3339)
	values := map[string]types.AttributeValue{
		":now":      &types.AttributeValueMemberS{Value: nowS},
		":oldst":    &types.AttributeValueMemberS{Value: string(u.Status)},
		":fence":    &types.AttributeValueMemberS{Value: UpdateLeasePrefix},
		":me":       &types.AttributeValueMemberS{Value: UpdateLeasePrefix + u.Owner},
		":leaseexp": &types.AttributeValueMemberS{Value: now.Add(UpdateLeaseTTL).Format(time.RFC3339)},
	}
	names := map[string]string{"#st": "status"}
	set := []string{"updated_at = :now", "lease_owner = :me", "lease_expires_at = :leaseexp"}
	var remove []string

	fields := append([]string(nil), u.Fields...)
	sort.Strings(fields)
	search := false
	for i, f := range fields {
		if !updatableFields[f] {
			return fmt.Errorf("update watch %s: %q cannot be updated", w.WatchID, f)
		}
		search = search || IsSearchField(f)
		// Field names go through ExpressionAttributeNames: none is a reserved
		// word today, but this keeps it that way.
		n := fmt.Sprintf("#f%d", i)
		names[n] = f
		if v, ok := item[f]; ok {
			vn := fmt.Sprintf(":f%d", i)
			values[vn] = v
			set = append(set, n+" = "+vn)
		} else {
			remove = append(remove, n)
		}
	}
	if search {
		remove = append(remove, derivedSearchState...)
	}
	if u.Reactivate {
		set = append(set, "#st = :active", "ttl_timestamp = :ttl")
		values[":active"] = &types.AttributeValueMemberS{Value: string(StatusActive)}
		values[":ttl"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", RetentionTTL(w.ExpiresAt))}
		remove = append(remove, "expired_reason")
	}

	version := "attribute_not_exists(updated_at)"
	if u.Version != "" {
		version = "updated_at = :ver"
		values[":ver"] = &types.AttributeValueMemberS{Value: u.Version}
	}
	cond := version + " AND #st = :oldst" +
		" AND attribute_not_exists(pending_instance_id) AND attribute_not_exists(pending_reservation_id)" +
		" AND attribute_not_exists(sagemaker_job_name)" +
		" AND (attribute_not_exists(lease_owner) OR lease_expires_at < :now OR begins_with(lease_owner, :fence))"

	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &s.watchesTable,
		Key: map[string]types.AttributeValue{
			"watch_id": &types.AttributeValueMemberS{Value: w.WatchID},
		},
		ConditionExpression:       aws.String(cond),
		UpdateExpression:          aws.String(updateExpr(remove, set, nil)),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err != nil {
		var cf *types.ConditionalCheckFailedException
		if errors.As(err, &cf) {
			return ErrUpdateConflict
		}
		return fmt.Errorf("update watch %s: %w", w.WatchID, err)
	}
	w.UpdatedAt = now
	if u.Reactivate {
		w.Status = StatusActive
		w.ExpiredReason = ""
		w.TTLTimestamp = RetentionTTL(w.ExpiresAt)
	}
	if search {
		w.ConsecutiveFailures = 0
		w.QuotaInfeasible = nil
		w.QuotaCapped = false
		w.QuotaCappedCount = 0
		w.QuotaCappedAt = time.Time{}
		w.QuotaCapReason = ""
		w.QuotaCapLimit = 0
		w.QuotaCapRegion = ""
	}
	return nil
}

// ActionUpdated is the MatchResult.ActionTaken of the history row an update
// writes (#257). It is an audit row, not a match: it acquired nothing.
const ActionUpdated = "updated"

// maxHistoryKeyAttempts bounds how often RecordUpdate moves its sort key on
// a collision.
const maxHistoryKeyAttempts = 5

// RecordUpdate writes the `updated` history row for an update of w, listing
// the changed fields. History rows are keyed by (watch_id, matched_at), so the
// row is written only if no row has that key (a match recorded at the same
// instant is never overwritten); on a collision it moves on by a nanosecond.
// It does not touch the watch record (no match count, no last match).
func (s *Store) RecordUpdate(ctx context.Context, w *Watch, fields []string, at time.Time) error {
	m := &MatchResult{
		WatchID:       w.WatchID,
		UserID:        w.UserID,
		Service:       w.Service,
		ActionTaken:   ActionUpdated,
		ChangedFields: append([]string(nil), fields...),
		TTLTimestamp:  time.Now().UTC().Add(retentionWindow).Unix(),
	}
	at = at.UTC()
	for i := 0; i < maxHistoryKeyAttempts; i++ {
		m.MatchedAt = at.Add(time.Duration(i))
		item, err := attributevalue.MarshalMap(m)
		if err != nil {
			return fmt.Errorf("marshal update history row: %w", err)
		}
		_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           &s.historyTable,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(matched_at)"),
		})
		if err == nil {
			return nil
		}
		var cf *types.ConditionalCheckFailedException
		if !errors.As(err, &cf) {
			return fmt.Errorf("put update history row: %w", err)
		}
	}
	return fmt.Errorf("put update history row: a history row already exists at each of %d keys from %s", maxHistoryKeyAttempts, at.Format(time.RFC3339Nano))
}
