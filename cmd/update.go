package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// updateOpts are `lagotto update`'s flags. The *Set fields record whether a
// flag was given, so "--regions ”" (all enabled regions) is distinguishable
// from no --regions at all.
type updateOpts struct {
	instanceType    string
	instanceTypeSet bool
	addTypes        []string
	regions         []string
	regionsSet      bool
	addRegions      []string
	removeRegions   []string
	azs             []string
	azsSet          bool
	spot            bool
	noSpot          bool
	maxPrice        float64
	maxPriceSet     bool
	notify          []string
	notifySet       bool
	addNotify       []string
	removeNotify    []string
	spawnConfig     string
	project         string
	projectSet      bool
	reactivate      bool
	dryRun          bool
	requireQuota    bool
}

var updateFlags updateOpts

var updateCmd = &cobra.Command{
	Use:   "update <watch-id>",
	Short: "Change an existing watch's search in place",
	Long: `Change what an existing watch hunts for (its instance types, regions, AZs,
Spot, maximum price), who it tells (--notify), its spawn config, or its project,
without cancelling it. The watch keeps its ID, its attempt count, last error,
match history and email subscriptions, and keeps hunting throughout.

The updated watch is checked exactly as 'lagotto watch' checks a new one
(including default-subnet coverage and the instance-role check), so an update
cannot produce a watch 'watch' would refuse. Its action, service, owner, ID and
deadline (TTL, or the until-landed acknowledgement deadline) cannot be changed:
use 'lagotto extend' or 'lagotto ack' for the deadline, and a new watch for a
different action.

When the search changes, the state the poller derived from the old search is
cleared: the consecutive-failure count, the quota-refused candidates and the
quota cap. An email address you add is subscribed (and must confirm); one you
remove is unsubscribed unless another active watch names it; the others keep
their confirmed subscriptions. The change is recorded as an 'updated' row in
'lagotto history'.

An update is refused while the poller is acting on the watch (try again in a
moment), while a launch or hold it made is still landing, and while a SageMaker
job it submitted is in flight. An expired or failed watch is only updated with
--reactivate, which also makes it active again; it must still be before its
deadline (run 'lagotto extend' first if not).

--require-quota applies 'watch --require-quota' to a fleet watch's updated
search: the update is refused (exit status 3) only when the quota was read in
every region and provably can't hold the fleet.

--dry-run shows the change, after every check, without writing anything.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE:         runUpdate,
}

func init() {
	rootCmd.AddCommand(updateCmd)
	f := updateCmd.Flags()
	o := &updateFlags
	f.StringVar(&o.instanceType, "instance-type", "", "Replace the instance-type pattern (wildcards and comma-separated rungs, as for 'watch')")
	f.StringSliceVar(&o.addTypes, "add-type", nil, "Add instance types or patterns to the pattern (comma-separated)")
	f.StringSliceVarP(&o.regions, "regions", "r", nil, "Replace the regions (comma-separated; '' = all enabled regions)")
	f.StringSliceVar(&o.addRegions, "add-region", nil, "Add regions to the watch's region list (comma-separated)")
	f.StringSliceVar(&o.removeRegions, "remove-region", nil, "Remove regions from the watch's region list (comma-separated)")
	f.StringSliceVar(&o.azs, "azs", nil, "Replace the availability zones (comma-separated; '' = all AZs in the region)")
	f.BoolVar(&o.spot, "spot", false, "Watch for Spot capacity")
	f.BoolVar(&o.noSpot, "no-spot", false, "Watch for On-Demand capacity")
	f.Float64Var(&o.maxPrice, "max-price", 0, "Replace the maximum price per hour (0 = any)")
	f.StringSliceVar(&o.notify, "notify", nil, "Replace the notification channels (email:, webhook:, sns:; '' = none)")
	f.StringSliceVar(&o.addNotify, "add-notify", nil, "Add notification channels")
	f.StringSliceVar(&o.removeNotify, "remove-notify", nil, "Remove notification channels")
	f.StringVar(&o.spawnConfig, "spawn-config", "", "Replace the spawn config (--action spawn watches): a local path, s3://bucket/key or '-' for stdin, read and stored as 'watch' does")
	f.StringVar(&o.project, "project", "", "Replace the project label ('' = none)")
	f.BoolVar(&o.reactivate, "reactivate", false, "Allow updating an expired or failed watch, and make it active again")
	f.BoolVar(&o.dryRun, "dry-run", false, "Show the change without writing anything")
	f.BoolVar(&o.requireQuota, "require-quota", false, "For a --maintain fleet watch: refuse the update (exit status 3) when the account's EC2 vCPU quota, read in every region of the updated watch, provably can't hold its workers, as 'watch --require-quota' does. An unreadable quota is unknown, not zero: the update then goes ahead, with a note.")
}

func runUpdate(cmd *cobra.Command, args []string) error {
	o := updateFlags
	fl := cmd.Flags()
	o.instanceTypeSet = fl.Changed("instance-type")
	o.regionsSet = fl.Changed("regions")
	o.azsSet = fl.Changed("azs")
	o.maxPriceSet = fl.Changed("max-price")
	o.notifySet = fl.Changed("notify")
	o.projectSet = fl.Changed("project")
	if err := o.validate(); err != nil {
		return err
	}

	ctx := context.Background()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	store := watcher.NewStore(cfg, watchesTable, historyTable)
	stsClient := sts.NewFromConfig(cfg)
	w, version, err := getWatchOwnedVersioned(ctx, store, stsClient, args[0])
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := checkUpdatable(w, o.reactivate, now); err != nil {
		return err
	}
	if o.requireQuota && w.DesiredCount <= 0 {
		return errRequireQuotaNeedsFleet
	}

	nw, changes, err := applyUpdate(w, o, func(src string) ([]byte, error) { return readSpawnConfigFlag(ctx, src) })
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	if len(changes) == 0 {
		_, _ = fmt.Fprintf(out, "Watch %s already has those settings; nothing to update.\n", w.WatchID)
		return nil
	}

	// The same checks `watch` makes (#256, #153 and its --require-quota gate
	// #157, #170), on the watch as it will be stored.
	if err := validateWatchSearch(nw); err != nil {
		return err
	}
	pre, err := preflightWatch(ctx, cfg, os.Stderr, nw, o.requireQuota)
	if err != nil {
		return err
	}
	// Updating a notify watch keeps a deprecated action (#283); its action
	// cannot be changed in place, hence the warning's pointer to a new watch.
	deprecations := watcher.ActionDeprecations(w.Action, true)
	for i := range deprecations {
		deprecations[i].Message += fmt.Sprintf("\n'update' cannot change a watch's action: create a new watch with --action hold or --action spawn, then 'lagotto cancel %s'.", w.WatchID)
	}
	writeDeprecations(cmd.ErrOrStderr(), deprecations)

	reactivate := w.Status != watcher.StatusActive
	res := updateResult{
		WatchID:      w.WatchID,
		DryRun:       o.dryRun,
		Reactivated:  reactivate,
		OldStatus:    w.Status,
		Changes:      changes,
		ResetState:   searchChanged(changes),
		Watch:        nw,
		Quota:        pre.Quota,
		Gate:         pre.Gate,
		Role:         pre.Role,
		Deprecations: deprecations,
	}
	if o.dryRun {
		return writeUpdate(out, getOutputFormat(), res)
	}

	if err := applyWatchUpdate(ctx, cfg, store, w, nw, version, changes, reactivate); err != nil {
		return err
	}
	if err := writeUpdate(out, getOutputFormat(), res); err != nil {
		return err
	}
	// Unsubscribe the removed addresses no other active watch names (#240).
	// Best effort, after the watch is stored: the poller's next sweep or
	// `lagotto notify-gc` retries. Its messages go to stderr with -o json.
	if _, removed := emailDiff(w, nw); len(removed) > 0 {
		msgs := out
		if getOutputFormat() == "json" {
			msgs = os.Stderr
		}
		cleanupWatchEmails(ctx, msgs, os.Stderr, emailSubscriberFor(cfg, os.Getenv, accountFromARN(w.UserID)),
			store.ListActiveWatches, withEmails(w, removed))
	}
	return nil
}

// applyWatchUpdate does the writes of an update, in the order that leaves
// nothing half-done on a refusal: subscribe added addresses and read the
// poller's schedule (either can refuse with the watch unchanged), write the
// watch, switch the schedule on, then the best-effort audit row. The caller
// unsubscribes removed addresses afterwards.
func applyWatchUpdate(ctx context.Context, cfg aws.Config, store *watcher.Store, w, nw *watcher.Watch, version string, changes []fieldChange, reactivate bool) error {
	action := "update watch " + w.WatchID
	added, _ := emailDiff(w, nw)
	// A reactivated watch's addresses may have been unsubscribed when it ended
	// (#240), so all of them are (re)subscribed; Ensure reuses a subscription
	// that is still there.
	subscribe := added
	if reactivate {
		subscribe = watcher.EmailAddresses(nw)
	}
	if len(subscribe) > 0 {
		sw := withEmails(nw, subscribe)
		if err := subscribeForNewWatch(ctx, os.Stderr, cfg, os.Getenv, accountFromARN(w.UserID), deploy.New(cfg), sw); err != nil {
			return errors.New(strings.Replace(err.Error(), "no watch was created", "the watch was not changed", 1))
		}
		copyEmailStates(nw, sw)
	}

	sched := scheduler.NewFromConfig(cfg)
	pollSched, err := pollSchedulePreflight(ctx, sched, action)
	if err != nil {
		return err
	}

	fields := changedFields(changes)
	err = store.UpdateWatchFields(ctx, nw, watcher.WatchUpdate{
		Fields:     fields,
		Version:    version,
		Status:     w.Status,
		Reactivate: reactivate,
		Owner:      uuid.New().String()[:8],
	}, time.Now())
	if errors.Is(err, watcher.ErrUpdateConflict) {
		cur, _, gerr := store.GetWatchVersioned(ctx, w.WatchID)
		return updateConflictError(w.WatchID, cur, gerr, time.Now().UTC())
	}
	if err != nil {
		return err
	}

	if err := enablePollScheduleIfOff(ctx, sched, os.Stderr, pollSched); err != nil {
		if !reactivate {
			return pollScheduleUpdateRefusal(action, "the update was stored, but the poller's schedule is still off, so the watch is not being polled", nil, err)
		}
		var restore error
		if w.Status == watcher.StatusExpired {
			restore = store.ExpireWatch(ctx, w.WatchID, w.ExpiredReason)
		} else {
			restore = store.UpdateWatchStatus(ctx, w.WatchID, w.Status)
		}
		return pollScheduleUpdateRefusal(action, fmt.Sprintf("the update was stored and the watch put back to %s", w.Status), restore, err)
	}

	if err := store.RecordUpdate(ctx, nw, fields, nw.UpdatedAt); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: the watch was updated, but its history row was not written: %v\n", err)
	}
	return nil
}

// validate rejects flag combinations that can't mean anything, and an update
// with nothing to change, before any AWS call.
func (o updateOpts) validate() error {
	if o.spot && o.noSpot {
		return fmt.Errorf("--spot and --no-spot cannot both be given")
	}
	if o.instanceTypeSet && strings.TrimSpace(o.instanceType) == "" {
		return fmt.Errorf("--instance-type must not be empty")
	}
	if o.maxPriceSet && o.maxPrice < 0 {
		return fmt.Errorf("--max-price must be >= 0 (0 = any)")
	}
	if !o.instanceTypeSet && len(o.addTypes) == 0 && !o.regionsSet && len(o.addRegions) == 0 && len(o.removeRegions) == 0 &&
		!o.azsSet && !o.spot && !o.noSpot && !o.maxPriceSet && !o.notifySet && len(o.addNotify) == 0 &&
		len(o.removeNotify) == 0 && o.spawnConfig == "" && !o.projectSet {
		return fmt.Errorf("nothing to update: give at least one of --instance-type, --add-type, --regions, --add-region, " +
			"--remove-region, --azs, --spot, --no-spot, --max-price, --notify, --add-notify, --remove-notify, --spawn-config, --project")
	}
	return nil
}

// checkUpdatable refuses an update the watch's state does not allow: one that
// has ended (unless --reactivate, for expired or failed), one a poller is
// acting on, and one with a launch, hold or SageMaker job in flight. The
// conditional write re-checks all of this atomically; checking here first
// means nothing (no subscription, no schedule change) is touched for a
// refusal.
func checkUpdatable(w *watcher.Watch, reactivate bool, now time.Time) error {
	switch w.Status {
	case watcher.StatusActive:
	case watcher.StatusExpired, watcher.StatusFailed:
		if !reactivate {
			return fmt.Errorf("watch %s is %s: pass --reactivate to update it and make it active again", w.WatchID, w.Status)
		}
		if !w.ExpiresAt.After(now) {
			if w.Status == watcher.StatusExpired {
				return fmt.Errorf("watch %s is expired and its deadline (%s) has passed, so reactivating it would expire it again at once: "+
					"run 'lagotto extend %s' to reactivate it with a new deadline, then update it", w.WatchID, w.ExpiresAt.Format(time.RFC3339), w.WatchID)
			}
			return fmt.Errorf("watch %s is failed and its deadline (%s) has passed: create a new watch instead", w.WatchID, w.ExpiresAt.Format(time.RFC3339))
		}
	default:
		return fmt.Errorf("watch %s is %s; only an active watch (or an expired or failed one, with --reactivate) can be updated", w.WatchID, w.Status)
	}
	if err := landingRefusal(w); err != nil {
		return err
	}
	return leaseRefusal(w, now)
}

// landingRefusal refuses an update while the watch is landing (#252) or has a
// SageMaker job in flight: the poller is checking a resource the old search
// produced. Queuing the change until it resolves (--after-landing in #257) is
// not implemented.
func landingRefusal(w *watcher.Watch) error {
	switch {
	case w.PendingInstanceID != "":
		return fmt.Errorf("watch %s is landing: instance %s (%s) was launched and is not confirmed running yet. "+
			"Update it once that resolves: if it lands the watch is done, and if it does not the watch resumes hunting and can be updated (lagotto status %s)",
			w.WatchID, w.PendingInstanceID, w.PendingRegion, w.WatchID)
	case w.PendingReservationID != "":
		return fmt.Errorf("watch %s is landing: capacity reservation %s (%s) was created and is not active yet. "+
			"Update it once that resolves: if it lands the watch is done, and if it does not the watch resumes hunting and can be updated (lagotto status %s)",
			w.WatchID, w.PendingReservationID, w.PendingRegion, w.WatchID)
	case w.SageMakerJobName != "":
		return fmt.Errorf("watch %s has SageMaker job %s in flight; update it once the poller has seen that job's outcome (lagotto status %s)",
			w.WatchID, w.SageMakerJobName, w.WatchID)
	}
	return nil
}

// leaseRefusal refuses an update while a poller holds a live lease on the
// watch (#47): it is acting on the old search right now. An earlier update's
// fencing lease does not count.
func leaseRefusal(w *watcher.Watch, now time.Time) error {
	if w.LeaseOwner == "" || strings.HasPrefix(w.LeaseOwner, watcher.UpdateLeasePrefix) || !w.LeaseExpiresAt.After(now) {
		return nil
	}
	return fmt.Errorf("a poller is acting on watch %s right now (its lease runs until %s); nothing was changed, try again in a moment",
		w.WatchID, w.LeaseExpiresAt.UTC().Format(time.RFC3339))
}

// updateConflictError explains a failed conditional write from the watch as
// it is now.
func updateConflictError(id string, cur *watcher.Watch, getErr error, now time.Time) error {
	const nothing = "nothing was changed"
	if getErr != nil || cur == nil {
		return fmt.Errorf("watch %s changed while it was being updated; %s, run the update again", id, nothing)
	}
	if err := landingRefusal(cur); err != nil {
		return fmt.Errorf("%w; %s", err, nothing)
	}
	if err := leaseRefusal(cur, now); err != nil {
		return err
	}
	return fmt.Errorf("watch %s changed while it was being updated (it is now %s); %s, run the update again", id, cur.Status, nothing)
}

// applyUpdate returns a copy of w with o applied, and the fields that changed.
// It touches only the mutable fields; loadSpawn reads a --spawn-config source.
func applyUpdate(w *watcher.Watch, o updateOpts, loadSpawn func(string) ([]byte, error)) (*watcher.Watch, []fieldChange, error) {
	nw := *w
	nw.Regions = append([]string(nil), w.Regions...)
	nw.AvailabilityZones = append([]string(nil), w.AvailabilityZones...)
	nw.NotifyChannels = append([]watcher.NotifyChannel(nil), w.NotifyChannels...)

	if o.instanceTypeSet {
		nw.InstanceTypePattern = strings.TrimSpace(o.instanceType)
	}
	if len(o.addTypes) > 0 {
		rungs := watcher.SplitInstanceTypePatterns(nw.InstanceTypePattern)
		for _, t := range o.addTypes {
			for _, r := range watcher.SplitInstanceTypePatterns(t) {
				if !contains(rungs, r) {
					rungs = append(rungs, r)
				}
			}
		}
		nw.InstanceTypePattern = strings.Join(rungs, ",")
	}

	if o.regionsSet {
		nw.Regions = cleanList(o.regions)
	}
	regions, err := editRegions(nw.Regions, cleanList(o.addRegions), cleanList(o.removeRegions))
	if err != nil {
		return nil, nil, err
	}
	nw.Regions = regions
	if o.azsSet {
		nw.AvailabilityZones = cleanList(o.azs)
	}
	if o.spot {
		nw.Spot = true
	}
	if o.noSpot {
		nw.Spot = false
	}
	if o.maxPriceSet {
		nw.MaxPrice = o.maxPrice
	}

	channels, err := editNotify(nw.NotifyChannels, o)
	if err != nil {
		return nil, nil, err
	}
	nw.NotifyChannels = channels

	if o.spawnConfig != "" {
		if nw.Service == watcher.ServiceSageMaker || nw.Action != watcher.ActionSpawn {
			return nil, nil, fmt.Errorf("--spawn-config applies only to an EC2 --action spawn watch; watch %s is --action %s", w.WatchID, w.Action)
		}
		b, err := loadSpawn(o.spawnConfig)
		if err != nil {
			return nil, nil, err
		}
		nw.LaunchConfigJSON = b
	}
	if o.projectSet {
		nw.Project = strings.TrimSpace(o.project)
	}
	return &nw, diffWatch(w, &nw), nil
}

// editRegions applies --add-region / --remove-region. An empty region list
// means every enabled region, so neither may silently turn into the other:
// adding to "all" would narrow the watch to the added regions, and removing
// the last region would widen it to all of them.
func editRegions(regions, add, remove []string) ([]string, error) {
	if len(add) == 0 && len(remove) == 0 {
		return regions, nil
	}
	if len(regions) == 0 {
		return nil, fmt.Errorf("the watch already searches every enabled region; use --regions to give it an explicit list")
	}
	out := append([]string(nil), regions...)
	for _, r := range add {
		if !contains(out, r) {
			out = append(out, r)
		}
	}
	for _, r := range remove {
		if !contains(out, r) {
			return nil, fmt.Errorf("--remove-region %s: the watch does not search that region (regions: %s)", r, strings.Join(out, ","))
		}
		out = without(out, r)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--remove-region would remove every region, and an empty region list means every enabled region; " +
			"use --regions '' if that is what you want")
	}
	return out, nil
}

// editNotify applies --notify, --add-notify and --remove-notify, parsing each
// the way `watch` parses --notify.
func editNotify(cur []watcher.NotifyChannel, o updateOpts) ([]watcher.NotifyChannel, error) {
	out := cur
	if o.notifySet {
		parsed, err := parseNotifyChannels(cleanList(o.notify))
		if err != nil {
			return nil, err
		}
		out = parsed
	}
	add, err := parseNotifyChannels(cleanList(o.addNotify))
	if err != nil {
		return nil, err
	}
	for _, ch := range add {
		if channelIndex(out, ch) < 0 {
			out = append(out, ch)
		}
	}
	remove, err := parseNotifyChannels(cleanList(o.removeNotify))
	if err != nil {
		return nil, err
	}
	for _, ch := range remove {
		i := channelIndex(out, ch)
		if i < 0 {
			return nil, fmt.Errorf("--remove-notify %s:%s: the watch does not notify it", ch.Type, ch.Target)
		}
		out = append(append([]watcher.NotifyChannel(nil), out[:i]...), out[i+1:]...)
	}
	return out, nil
}

func channelIndex(chs []watcher.NotifyChannel, ch watcher.NotifyChannel) int {
	for i, c := range chs {
		if c.Type == ch.Type && c.Target == ch.Target {
			return i
		}
	}
	return -1
}

// fieldChange is one changed field: its stored attribute name and its value
// before and after, in display form.
type fieldChange struct {
	Field  string `json:"field"`
	Before any    `json:"before"`
	After  any    `json:"after"`
}

// updateFields are the mutable fields, in display order, each with its
// display value (normalized, so nil and empty compare equal).
var updateFields = []struct {
	field string
	value func(*watcher.Watch) any
}{
	{watcher.FieldInstanceTypePattern, func(w *watcher.Watch) any { return w.InstanceTypePattern }},
	{watcher.FieldRegions, func(w *watcher.Watch) any { return orEmpty(w.Regions) }},
	{watcher.FieldAvailabilityZones, func(w *watcher.Watch) any { return orEmpty(w.AvailabilityZones) }},
	{watcher.FieldSpot, func(w *watcher.Watch) any { return w.Spot }},
	{watcher.FieldMaxPrice, func(w *watcher.Watch) any { return w.MaxPrice }},
	{watcher.FieldNotifyChannels, func(w *watcher.Watch) any { return channelStrings(w.NotifyChannels) }},
	{watcher.FieldLaunchConfig, func(w *watcher.Watch) any { return configDigest(w.LaunchConfigJSON) }},
	{watcher.FieldProject, func(w *watcher.Watch) any { return w.Project }},
}

// diffWatch lists the mutable fields that differ between a and b.
func diffWatch(a, b *watcher.Watch) []fieldChange {
	var out []fieldChange
	for _, f := range updateFields {
		before, after := f.value(a), f.value(b)
		if !reflect.DeepEqual(before, after) {
			out = append(out, fieldChange{Field: f.field, Before: before, After: after})
		}
	}
	return out
}

func changedFields(changes []fieldChange) []string {
	out := make([]string, len(changes))
	for i, c := range changes {
		out[i] = c.Field
	}
	return out
}

func searchChanged(changes []fieldChange) bool {
	for _, c := range changes {
		if watcher.IsSearchField(c.Field) {
			return true
		}
	}
	return false
}

// configDigest identifies a stored spawn config without printing it (it can
// carry user data): its size and a short SHA-256. "" when there is none.
func configDigest(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("sha256:%s (%d bytes)", hex.EncodeToString(sum[:])[:12], len(b))
}

func channelStrings(chs []watcher.NotifyChannel) []string {
	out := make([]string, 0, len(chs))
	for _, c := range chs {
		out = append(out, c.Type+":"+c.Target)
	}
	return out
}

// emailDiff returns the email addresses nw adds to w and the ones it drops.
func emailDiff(w, nw *watcher.Watch) (added, removed []string) {
	before, after := watcher.EmailAddresses(w), watcher.EmailAddresses(nw)
	for _, a := range after {
		if !contains(before, a) {
			added = append(added, a)
		}
	}
	for _, a := range before {
		if !contains(after, a) {
			removed = append(removed, a)
		}
	}
	return added, removed
}

// withEmails is a copy of w whose only channels are the email channels for
// addrs, for the subscribe and unsubscribe steps, which act on every email
// channel of the watch they are given.
func withEmails(w *watcher.Watch, addrs []string) *watcher.Watch {
	c := *w
	c.NotifyChannels = nil
	for _, a := range addrs {
		c.NotifyChannels = append(c.NotifyChannels, watcher.NotifyChannel{Type: "email", Target: a})
	}
	return &c
}

// copyEmailStates copies the subscription states the subscribe step recorded
// on from onto the matching email channels of to.
func copyEmailStates(to, from *watcher.Watch) {
	for i := range to.NotifyChannels {
		ch := &to.NotifyChannels[i]
		if ch.Type != "email" {
			continue
		}
		for _, f := range from.NotifyChannels {
			if f.Type == "email" && watcher.NormalizeEmail(f.Target) == watcher.NormalizeEmail(ch.Target) {
				ch.Subscription = f.Subscription
			}
		}
	}
}

// updateResult is what `update` reports, and its JSON output.
type updateResult struct {
	WatchID     string              `json:"watch_id"`
	DryRun      bool                `json:"dry_run"`
	Reactivated bool                `json:"reactivated"`
	OldStatus   watcher.WatchStatus `json:"previous_status"`
	Changes     []fieldChange       `json:"changes"`
	ResetState  bool                `json:"reset_derived_state"`
	Watch       *watcher.Watch      `json:"watch"`
	Quota       *quotacheck.Report  `json:"quota_warning,omitempty"`
	Gate        *quotacheck.Gate    `json:"quota_gate,omitempty"`
	Role        *InstanceRoleCheck  `json:"instance_role_check,omitempty"`
	// Deprecations are the deprecated features the watch uses (#283), also
	// printed to stderr.
	Deprecations []watcher.Deprecation `json:"deprecations,omitempty"`
}

// writeUpdate prints the before/after diff of an update (or a dry run).
func writeUpdate(out io.Writer, format string, r updateResult) error {
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	verb := "Updated"
	if r.DryRun {
		verb = "Dry run: would update"
	}
	_, _ = fmt.Fprintf(out, "%s watch %s:\n", verb, r.WatchID)
	for _, c := range r.Changes {
		_, _ = fmt.Fprintf(out, "  %-22s %s -> %s\n", c.Field+":", displayValue(c.Field, c.Before), displayValue(c.Field, c.After))
	}
	if r.Reactivated {
		_, _ = fmt.Fprintf(out, "  %-22s %s -> %s\n", "status:", r.OldStatus, watcher.StatusActive)
	}
	if r.ResetState {
		verb := "were"
		if r.DryRun {
			verb = "would be"
		}
		_, _ = fmt.Fprintf(out, "  The search changed, so the consecutive-failure count and the quota state %s reset; attempts, last error and history are kept.\n", verb)
	}
	if r.Gate != nil {
		_, _ = fmt.Fprintf(out, "  Quota gate: %s (--require-quota)\n", r.Gate.Verdict)
	}
	if r.DryRun {
		_, _ = fmt.Fprintln(out, "Nothing was written.")
		return nil
	}
	writeEmailStates(out, "  Notify:   ", r.Watch)
	return nil
}

// displayValue renders one side of a field change for the human diff.
func displayValue(field string, v any) string {
	switch x := v.(type) {
	case []string:
		if len(x) == 0 {
			switch field {
			case watcher.FieldRegions:
				return "(all enabled)"
			case watcher.FieldAvailabilityZones:
				return "(all in region)"
			default:
				return "(none)"
			}
		}
		return strings.Join(x, ",")
	case float64:
		if field == watcher.FieldMaxPrice {
			if x == 0 {
				return "any"
			}
			return fmt.Sprintf("$%.4f/hr", x)
		}
		return fmt.Sprintf("%g", x)
	case string:
		if x == "" {
			return "(none)"
		}
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// cleanList trims each entry and drops empty ones, so "--regions ”" is an
// empty list.
func cleanList(xs []string) []string {
	var out []string
	for _, x := range xs {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

func orEmpty(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func without(xs []string, x string) []string {
	out := xs[:0:0]
	for _, y := range xs {
		if y != x {
			out = append(out, y)
		}
	}
	return out
}
