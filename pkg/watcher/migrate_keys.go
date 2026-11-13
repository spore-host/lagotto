package watcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// MigrateOwnerKeys moves every legacy record (a key with no owner prefix) in
// lagotto's three tables under its owner's prefix (see keys.go). `lagotto
// setup` runs it with an administrator's credentials before it writes the
// owner-scoped lagotto-cli-user policy.
//
// # Safety
//
//   - Each record moves in one DynamoDB transaction: Put the new key (only if
//     it does not exist) and Delete the old one (only if it is unchanged since
//     it was read, and no poller holds its lease). A record is therefore always
//     either at its old key or at its new one, never both and never neither, so
//     an interrupted run loses nothing and two copies of a watch are never
//     active at once. After the transaction the new record is read back
//     (strongly consistent) and checked.
//   - It is idempotent and resumable: a record already moved is skipped, and a
//     record that was busy or changed while it ran is reported and moved by the
//     next run.
//   - A scheduled launch's old key is replaced by a marker naming the new key
//     (in the same transaction), because the EventBridge schedules already armed
//     send the old id; GetScheduledLaunch follows it.
//   - A watch keeps its old key as legacy_watch_id, which its fleet's instance
//     tags may still carry.
//
// # Owners
//
// A record's owner is its owner_id when it has one, else derived from the
// creator's ARN by resolve (an IAM user's or role's unique id, plus the
// session name for a role session; see [ResolveOwnerFromARN]). A record whose
// owner cannot be resolved (no creator, or the principal no longer exists)
// stays where it is and is reported: the poller still services it, and only
// an administrator can change it once the scoped policy is in place.
func MigrateOwnerKeys(ctx context.Context, api MigrateAPI, t MigrateTables, resolve OwnerResolver, now time.Time) (MigrateReport, error) {
	return MigrateOwnerKeysWithProgress(ctx, api, t, resolve, now, nil)
}

// MigrateProgressEvery is how many moved records pass between progress lines.
const MigrateProgressEvery = 25

// MigrateOwnerKeysWithProgress is [MigrateOwnerKeys] reporting as it goes
// (#309): progress, when not nil, gets a line as each table's scan starts and
// every [MigrateProgressEvery] records moved, so a long run is never silent.
func MigrateOwnerKeysWithProgress(ctx context.Context, api MigrateAPI, t MigrateTables, resolve OwnerResolver, now time.Time, progress func(string)) (MigrateReport, error) {
	m := &migrator{api: api, t: t, resolve: resolve, now: now.UTC(), owners: map[string]ownerResult{}, progress: progress}
	m.say("Scanning %s for records to move under their owners' keys...", t.Watches)
	if err := m.watches(ctx); err != nil {
		return m.rep, err
	}
	if t.History != "" {
		m.say("Scanning %s...", t.History)
		if err := m.history(ctx); err != nil {
			return m.rep, err
		}
	}
	if t.Scheduled != "" {
		m.say("Scanning %s...", t.Scheduled)
		if err := m.scheduled(ctx); err != nil {
			return m.rep, err
		}
	}
	sort.Strings(m.rep.Skipped)
	return m.rep, nil
}

// MigrateAPI is the DynamoDB the migration needs. *dynamodb.Client satisfies it.
type MigrateAPI interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// MigrateTables names the tables to migrate; an empty name is skipped.
type MigrateTables struct{ Watches, History, Scheduled string }

// OwnerResolver maps a creator ARN to its principal id (the ${aws:userid} of
// that principal's sessions).
type OwnerResolver func(ctx context.Context, arn string) (string, error)

// MigrateReport is what a run did.
type MigrateReport struct {
	Watches, Groups, History, Scheduled int // records moved by this run
	Fragments                           int // stale fragments of moved watches removed
	// Skipped names every legacy record left in place, with why. Empty means
	// no legacy record remains.
	Skipped []string
}

// Remaining reports whether legacy records were left in place.
func (r MigrateReport) Remaining() bool { return len(r.Skipped) > 0 }

type ownerResult struct {
	id  string
	err error
}

type migrator struct {
	api     MigrateAPI
	t       MigrateTables
	resolve OwnerResolver
	now     time.Time
	owners  map[string]ownerResult
	// moved maps a legacy watch or group key to its new key, from earlier
	// runs (legacy_watch_id on a moved record) and this one.
	moved map[string]string
	rep   MigrateReport
	// progress, when set, receives progress lines (#309); ticks counts moves.
	progress func(string)
	ticks    int
}

func (m *migrator) say(format string, a ...any) {
	if m.progress != nil {
		m.progress(fmt.Sprintf(format, a...))
	}
}

// tick counts one moved record and reports every MigrateProgressEvery.
func (m *migrator) tick() {
	m.ticks++
	if m.ticks%MigrateProgressEvery == 0 {
		m.say("  moved %d record(s) so far...", m.ticks)
	}
}

func (m *migrator) skip(format string, a ...any) {
	m.rep.Skipped = append(m.rep.Skipped, fmt.Sprintf(format, a...))
}

func (m *migrator) owner(ctx context.Context, arn string) (string, error) {
	if arn == "" {
		return "", errors.New("it records no creator")
	}
	if r, ok := m.owners[arn]; ok {
		return r.id, r.err
	}
	id, err := m.resolve(ctx, arn)
	if err == nil && (id == "" || strings.Contains(id, KeySeparator)) {
		err = fmt.Errorf("resolved to an unusable owner id %q", id)
	}
	m.owners[arn] = ownerResult{id, err}
	return id, err
}

// recordOwner is a legacy record's owner: derived from its creator ARN, and,
// when it also carries an owner_id, only if the two agree. Before the move a
// legacy record's attributes were writable by any CLI user, so neither alone
// is evidence when they conflict; such a record is left for an administrator.
func (m *migrator) recordOwner(ctx context.Context, it map[string]types.AttributeValue, creator string) (string, error) {
	recorded := str(it, OwnerIDAttr)
	if creator == "" && recorded != "" {
		return recorded, nil
	}
	derived, err := m.owner(ctx, creator)
	if err != nil {
		return "", err
	}
	if recorded != "" && recorded != derived {
		return "", fmt.Errorf("its owner_id %q does not match its creator %s (%s)", recorded, creator, derived)
	}
	return derived, nil
}

func (m *migrator) scanAll(ctx context.Context, table string) ([]map[string]types.AttributeValue, error) {
	var items []map[string]types.AttributeValue
	p := dynamodb.NewScanPaginator(m.api, &dynamodb.ScanInput{TableName: aws.String(table), ConsistentRead: aws.Bool(true)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			var nf *types.ResourceNotFoundException
			if errors.As(err, &nf) {
				return nil, nil
			}
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		items = append(items, page.Items...)
	}
	return items, nil
}

func str(item map[string]types.AttributeValue, k string) string {
	if v, ok := item[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func copyItem(item map[string]types.AttributeValue) map[string]types.AttributeValue {
	out := make(map[string]types.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}

// unchangedCondition is the Delete condition on the old record: still as read
// (updated_at), and not leased by a poller right now.
func (m *migrator) unchangedCondition(item map[string]types.AttributeValue, keyAttr string) (string, map[string]types.AttributeValue) {
	vals := map[string]types.AttributeValue{":now": avS(m.now.Format(time.RFC3339))}
	cond := "attribute_exists(" + keyAttr + ") AND (attribute_not_exists(lease_owner) OR lease_expires_at < :now)"
	if v, ok := item["updated_at"]; ok {
		cond += " AND updated_at = :ver"
		vals[":ver"] = v
	} else {
		cond += " AND attribute_not_exists(updated_at)"
	}
	return cond, vals
}

// move runs the Put-new / Delete-old (or replace-old) transaction for one
// record and reads the new record back. done is false, with no error, when
// the old record changed or is leased (moved by a later run).
func (m *migrator) move(ctx context.Context, table, keyAttr string, old, next map[string]types.AttributeValue, replaceOld map[string]types.AttributeValue) (done bool, err error) {
	oldKey := map[string]types.AttributeValue{keyAttr: old[keyAttr]}
	if sk, ok := old["matched_at"]; ok && keyAttr == "watch_id" && table == m.t.History {
		oldKey["matched_at"] = sk
	}
	cond, vals := m.unchangedCondition(old, keyAttr)
	second := types.TransactWriteItem{Delete: &types.Delete{
		TableName: aws.String(table), Key: oldKey,
		ConditionExpression: aws.String(cond), ExpressionAttributeValues: vals,
	}}
	if replaceOld != nil {
		second = types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String(table), Item: replaceOld,
			ConditionExpression: aws.String(cond + " AND attribute_not_exists(moved_to)"), ExpressionAttributeValues: vals,
		}}
	}
	_, err = m.api.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{
			TableName: aws.String(table), Item: next,
			ConditionExpression: aws.String("attribute_not_exists(" + keyAttr + ")"),
		}},
		second,
	}})
	if err != nil {
		var tce *types.TransactionCanceledException
		if errors.As(err, &tce) {
			return false, nil
		}
		return false, err
	}
	newKey := map[string]types.AttributeValue{keyAttr: next[keyAttr]}
	if sk, ok := next["matched_at"]; ok && table == m.t.History {
		newKey["matched_at"] = sk
	}
	got, err := m.api.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: newKey, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return true, fmt.Errorf("verify %s: %w", str(next, keyAttr), err)
	}
	if got.Item == nil || str(got.Item, keyAttr) != str(next, keyAttr) {
		return true, fmt.Errorf("verify %s: the moved record is not readable at its new key", str(next, keyAttr))
	}
	return true, nil
}

func (m *migrator) watches(ctx context.Context) error {
	items, err := m.scanAll(ctx, m.t.Watches)
	if err != nil {
		return err
	}
	m.moved = map[string]string{}
	for _, it := range items {
		k := str(it, "watch_id")
		if IsLegacyKey(k) {
			continue
		}
		if l := str(it, "legacy_watch_id"); l != "" {
			m.moved[l] = k
		}
	}
	// Owners and new keys for every legacy record first, so a group's members
	// and a member's group map consistently.
	type plan struct {
		item  map[string]types.AttributeValue
		owner string
	}
	var groups, watches []plan
	for _, it := range items {
		k := str(it, "watch_id")
		if !IsLegacyKey(k) {
			continue
		}
		if dst, ok := m.moved[k]; ok {
			// Moved already; anything at the old key now is a fragment a poller
			// wrote after the move (an update to a key that no longer existed).
			if err := m.dropFragment(ctx, it, dst); err != nil {
				return err
			}
			continue
		}
		isGroup := strings.HasPrefix(k, GroupKeyPrefix)
		creator := str(it, "user_id")
		if isGroup {
			creator = str(it, "group_owner")
		}
		var owner string
		var oerr error
		if isGroup {
			owner, oerr = m.owner(ctx, creator)
		} else {
			owner, oerr = m.recordOwner(ctx, it, creator)
		}
		if oerr != nil {
			m.skip("%s %s: owner unknown (%v)", m.t.Watches, k, oerr)
			continue
		}
		if isGroup {
			groups = append(groups, plan{it, owner})
		} else {
			watches = append(watches, plan{it, owner})
		}
		m.moved[k] = OwnerKey(owner, k)
	}
	mapKey := func(k string) string {
		if n, ok := m.moved[k]; ok {
			return n
		}
		return k
	}
	for _, p := range watches {
		k := str(p.item, "watch_id")
		next := copyItem(p.item)
		next["watch_id"] = avS(m.moved[k])
		next[OwnerIDAttr] = avS(p.owner)
		next["legacy_watch_id"] = avS(k)
		delete(next, "lease_owner")
		delete(next, "lease_expires_at")
		ok, err := m.move(ctx, m.t.Watches, "watch_id", p.item, next, nil)
		if err != nil {
			return err
		}
		if !ok {
			delete(m.moved, k)
			m.skip("%s %s: changed or leased by a poller while moving; re-run setup", m.t.Watches, k)
			continue
		}
		m.rep.Watches++
		m.tick()
	}
	for _, p := range groups {
		k := str(p.item, "watch_id")
		next := copyItem(p.item)
		next["watch_id"] = avS(m.moved[k])
		for _, attr := range []string{"slot_watch", "winner_watch"} {
			if v := str(next, attr); v != "" {
				next[attr] = avS(mapKey(v))
			}
		}
		if ss, ok := next["group_members"].(*types.AttributeValueMemberSS); ok {
			mem := make([]string, 0, len(ss.Value))
			for _, id := range ss.Value {
				// A member that could not be moved stays out of the moved group:
				// it is in another scope now, and the poller ignores it there.
				if n := mapKey(id); !IsLegacyKey(n) && SameScope(n, m.moved[k]) {
					mem = append(mem, n)
				}
			}
			if len(mem) == 0 {
				delete(next, "group_members")
			} else {
				next["group_members"] = &types.AttributeValueMemberSS{Value: mem}
			}
		}
		ok, err := m.move(ctx, m.t.Watches, "watch_id", p.item, next, nil)
		if err != nil {
			return err
		}
		if !ok {
			m.skip("%s %s: changed while moving; re-run setup", m.t.Watches, k)
			continue
		}
		m.rep.Groups++
		m.tick()
	}
	return nil
}

// dropFragment deletes what a poller wrote at a moved watch's old key: an
// item that is not a whole watch (no instance_type_pattern) or group. A whole
// record at a moved key is left alone and reported.
func (m *migrator) dropFragment(ctx context.Context, it map[string]types.AttributeValue, dst string) error {
	k := str(it, "watch_id")
	if _, whole := it["instance_type_pattern"]; whole {
		m.skip("%s %s: a whole record still exists at the old key of %s; not moved", m.t.Watches, k, dst)
		return nil
	}
	if _, group := it["group_name"]; group {
		m.skip("%s %s: a group still exists at the old key of %s; not moved", m.t.Watches, k, dst)
		return nil
	}
	_, err := m.api.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(m.t.Watches),
		Key:                 map[string]types.AttributeValue{"watch_id": avS(k)},
		ConditionExpression: aws.String("attribute_not_exists(instance_type_pattern) AND attribute_not_exists(group_name)"),
	})
	var ccf *types.ConditionalCheckFailedException
	if err != nil && !errors.As(err, &ccf) {
		return fmt.Errorf("remove fragment %s: %w", k, err)
	}
	if err == nil {
		m.rep.Fragments++
	}
	return nil
}

func (m *migrator) history(ctx context.Context) error {
	items, err := m.scanAll(ctx, m.t.History)
	if err != nil {
		return err
	}
	for _, it := range items {
		k := str(it, "watch_id")
		if !IsLegacyKey(k) {
			continue
		}
		dst, ok := m.moved[k]
		if !ok {
			owner, oerr := m.recordOwner(ctx, it, str(it, "user_id"))
			if oerr != nil {
				m.skip("%s %s @ %s: owner unknown (%v)", m.t.History, k, str(it, "matched_at"), oerr)
				continue
			}
			dst = OwnerKey(owner, k)
		}
		next := copyItem(it)
		next["watch_id"] = avS(dst)
		if KeyOwner(dst) != "" {
			next[OwnerIDAttr] = avS(KeyOwner(dst))
		}
		done, err := m.move(ctx, m.t.History, "watch_id", it, next, nil)
		if err != nil {
			return err
		}
		if !done {
			m.skip("%s %s @ %s: changed while moving; re-run setup", m.t.History, k, str(it, "matched_at"))
			continue
		}
		m.rep.History++
		m.tick()
	}
	return nil
}

func (m *migrator) scheduled(ctx context.Context) error {
	items, err := m.scanAll(ctx, m.t.Scheduled)
	if err != nil {
		return err
	}
	for _, it := range items {
		k := str(it, "schedule_id")
		if !IsLegacyKey(k) || str(it, "moved_to") != "" {
			continue // moved, or a marker
		}
		owner, oerr := m.recordOwner(ctx, it, str(it, "user_id"))
		if oerr != nil {
			m.skip("%s %s: owner unknown (%v)", m.t.Scheduled, k, oerr)
			continue
		}
		dst := OwnerKey(owner, k)
		next := copyItem(it)
		next["schedule_id"] = avS(dst)
		next[OwnerIDAttr] = avS(owner)
		next["legacy_id"] = avS(k)
		ttl := it["ttl_timestamp"]
		if ttl == nil {
			ttl = &types.AttributeValueMemberN{Value: strconv.FormatInt(m.now.Add(400*24*time.Hour).Unix(), 10)}
		}
		marker := map[string]types.AttributeValue{"schedule_id": avS(k), "moved_to": avS(dst), "ttl_timestamp": ttl}
		done, err := m.move(ctx, m.t.Scheduled, "schedule_id", it, next, marker)
		if err != nil {
			return err
		}
		if !done {
			m.skip("%s %s: changed while moving; re-run setup", m.t.Scheduled, k)
			continue
		}
		m.rep.Scheduled++
		m.tick()
	}
	return nil
}

// ResolveOwnerFromARN derives the principal id IAM exposes as ${aws:userid}
// for sessions of the principal in arn:
//
//	arn:aws:iam::<acct>:user/[path/]<name>          -> the user's id (AIDA…)
//	arn:aws:sts::<acct>:assumed-role/<role>/<sess>  -> <role id (AROA…)>:<sess>
//	arn:aws:sts::<acct>:federated-user/<name>       -> <acct>:<name>
//	arn:aws:iam::<acct>:root                        -> <acct>
//
// userID and roleID look up a user's and a role's unique id by name.
func ResolveOwnerFromARN(ctx context.Context, arn string, userID, roleID func(ctx context.Context, name string) (string, error)) (string, error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" {
		return "", fmt.Errorf("%q is not an ARN", arn)
	}
	acct, res := parts[4], parts[5]
	switch {
	case parts[2] == "iam" && res == "root":
		return acct, nil
	case parts[2] == "iam" && strings.HasPrefix(res, "user/"):
		name := res[strings.LastIndex(res, "/")+1:]
		return userID(ctx, name)
	case parts[2] == "sts" && strings.HasPrefix(res, "assumed-role/"):
		seg := strings.Split(strings.TrimPrefix(res, "assumed-role/"), "/")
		if len(seg) != 2 || seg[0] == "" || seg[1] == "" {
			return "", fmt.Errorf("%q is not an assumed-role session ARN", arn)
		}
		id, err := roleID(ctx, seg[0])
		if err != nil {
			return "", err
		}
		return id + ":" + seg[1], nil
	case parts[2] == "sts" && strings.HasPrefix(res, "federated-user/"):
		return acct + ":" + strings.TrimPrefix(res, "federated-user/"), nil
	}
	return "", fmt.Errorf("%q is not a principal lagotto can resolve", arn)
}
