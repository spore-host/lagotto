package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/arm"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// Watch groups (#287), the CLI half: `watch --group` joins one, and
// `status <group>` / `cancel <group>` act on a whole group. The protocol is in
// pkg/watcher/group.go.

// validateGroupFlags checks --group / --group-on-extra against the rest of
// the watch (w carries the flags that matter) and returns the policy to store.
func validateGroupFlags(group, onExtra string, onExtraSet bool, w *watcher.Watch) (string, error) {
	if group == "" {
		if onExtraSet {
			return "", errors.New("--group-on-extra needs --group")
		}
		return "", nil
	}
	policy, err := watcher.ParseGroupOnExtra(onExtra)
	if err != nil {
		return "", err
	}
	w.Group, w.GroupOnExtra = group, policy
	if err := watcher.ValidateGroupMember(w); err != nil {
		return "", err
	}
	return policy, nil
}

// joinWatchGroup adds w to its group, creating it when new.
func joinWatchGroup(ctx context.Context, store *watcher.Store, w *watcher.Watch) (*watcher.WatchGroup, error) {
	g, err := store.JoinGroup(ctx, w.Group, w.UserID, w.WatchID, w.TTLTimestamp)
	if errors.Is(err, watcher.ErrGroupNotOwned) {
		return nil, fmt.Errorf("group %s belongs to another caller in this account; choose another --group name", w.Group)
	}
	if err != nil {
		return nil, err
	}
	return g, nil
}

// warnIfPollerPredatesGroups prints arm.CheckGroupPoller's warning, if any.
func warnIfPollerPredatesGroups(ctx context.Context, cfg aws.Config, errOut io.Writer, w *watcher.Watch) {
	if msg := arm.CheckGroupPoller(ctx, deploy.New(cfg), cfg.Region, w); msg != "" {
		_, _ = fmt.Fprintln(errOut, msg)
	}
}

// groupJoinedLine is the "Group:" line of `watch`'s summary.
func groupJoinedLine(g *watcher.WatchGroup, w *watcher.Watch) string {
	n := len(g.Members)
	created := ""
	if n <= 1 {
		created = ", new"
	}
	return fmt.Sprintf("%s (%d member(s)%s; the first to land cancels the rest; an extra landing is %s)",
		g.Name, n, created, extraVerb(w.GroupOnExtraPolicy()))
}

func extraVerb(policy string) string {
	if policy == watcher.GroupOnExtraKeep {
		return "kept"
	}
	return "terminated"
}

// isGroupArg reports whether a status/cancel argument names a group rather
// than a watch (w-…) or scheduled launch (sl-…). ValidateGroupName refuses a
// group name with either prefix, so the two cannot be confused.
func isGroupArg(arg string) bool {
	return !strings.HasPrefix(arg, "w-") && !isScheduledLaunchID(arg)
}

// groupName strips an optional "g-" (the group's key) from a group argument.
func groupName(arg string) string {
	return strings.TrimPrefix(arg, watcher.GroupKeyPrefix)
}

// getGroupOwned reads a group, refusing one the caller does not own with the
// same "not found" a missing one gets (as getWatchOwned does for a watch).
func getGroupOwned(ctx context.Context, store *watcher.Store, stsClient stsIdentityAPI, name string) (*watcher.WatchGroup, error) {
	notFound := fmt.Errorf("no watch or group %q found (a watch id starts w-)", name)
	g, err := store.GetGroup(ctx, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, notFound
	}
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("get caller identity: %w", err)
	}
	if g.Owner != aws.ToString(identity.Arn) {
		return nil, notFound
	}
	return g, nil
}

// groupStatus is `status <group> -o json`.
type groupStatus struct {
	Group    *watcher.WatchGroup    `json:"group"`
	Members  []*watcher.Watch       `json:"members"`
	Exposure *watcher.GroupExposure `json:"exposure,omitempty"`
}

func runGroupStatus(ctx context.Context, out, errOut io.Writer, store *watcher.Store, stsClient stsIdentityAPI, arg string) error {
	g, err := getGroupOwned(ctx, store, stsClient, groupName(arg))
	if err != nil {
		return err
	}
	members, err := store.GroupMembers(ctx, g)
	if err != nil {
		_, _ = fmt.Fprintf(errOut, "Warning: could not read every member: %v\n", err)
	}
	for _, w := range members {
		w.ComputeDurations()
		cliExposure(w)
	}
	ge := watcher.ComputeGroupExposure(members)
	st := groupStatus{Group: g, Members: members, Exposure: &ge}
	if getOutputFormat() == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	writeGroupStatus(out, st, time.Now().UTC())
	return nil
}

// writeGroupStatus renders `status <group>`. now is a parameter for tests.
func writeGroupStatus(out io.Writer, st groupStatus, now time.Time) {
	g := st.Group
	_, _ = fmt.Fprintf(out, "Group:    %s\n", g.Name)
	_, _ = fmt.Fprintf(out, "State:    %s\n", groupStateLine(g, st.Members))
	if g.State == watcher.GroupHunting && g.SlotWatch != "" {
		_, _ = fmt.Fprintf(out, "Slot:     held by %s until %s (no other member launches meanwhile)\n", g.SlotWatch, g.SlotExpiresAt)
	}
	if g.Owner != "" {
		_, _ = fmt.Fprintf(out, "Owner:    %s\n", g.Owner)
	}
	_, _ = fmt.Fprintf(out, "Created:  %s\n", g.CreatedAt.Format(time.RFC3339))
	if st.Exposure != nil && st.Exposure.Max != nil {
		writeExposure(out, "Exposure: ", &watcher.Exposure{
			Summary: fmt.Sprintf("%s (the largest member, %s: only one member is meant to land)", st.Exposure.Max.Summary, st.Exposure.MaxWatch),
			Details: st.Exposure.Max.Details,
		})
		if st.Exposure.Note != "" {
			_, _ = fmt.Fprintf(out, "          %s\n", st.Exposure.Note)
		}
	}
	_, _ = fmt.Fprintf(out, "\nMembers (%d):\n", len(st.Members))
	const row = "  %-12s %-10s %-6s %-20s %-25s %-9s %5s %-9s %s"
	_, _ = fmt.Fprintln(out, strings.TrimRight(fmt.Sprintf(row, "WATCH ID", "STATUS", "ACTION", "PATTERN", "REGIONS", "ON EXTRA", "TRIES", "LAST ERR", "NOTE"), " "))
	for _, w := range st.Members {
		regions := displayRegions(w.Regions)
		if len(regions) > 25 {
			regions = regions[:22] + "..."
		}
		latest, _ := w.LatestError()
		_, _ = fmt.Fprintln(out, strings.TrimRight(fmt.Sprintf(row,
			w.WatchID, statusCol(w), w.Action, truncate(w.InstanceTypePattern, 20), regions, w.GroupOnExtraPolicy(),
			strconv.Itoa(w.Attempts), dashIfEmpty(truncate(watcher.ErrorClass(latest), 9)), memberNote(w, g, now)), " "))
	}
	for _, w := range st.Members {
		if latest, at := w.LatestError(); latest != "" && w.Status == watcher.StatusActive {
			_, _ = fmt.Fprintf(out, "  %s last error: %s (%s ago)\n", w.WatchID, latest, formatAge(now.Sub(at)))
		}
	}
}

// groupStateLine is the group's state: hunting (with how many members still
// are), landed by whom, or cancelled and why.
func groupStateLine(g *watcher.WatchGroup, members []*watcher.Watch) string {
	switch g.State {
	case watcher.GroupLanded:
		s := "landed by " + g.LandedSummary()
		if g.EndedAt != nil {
			s += " at " + g.EndedAt.UTC().Format(time.RFC3339)
		}
		return s
	case watcher.GroupCancelled:
		return "cancelled: " + dashIfEmpty(g.EndReason)
	}
	active := 0
	for _, w := range members {
		if w.Status == watcher.StatusActive {
			active++
		}
	}
	return fmt.Sprintf("hunting (%d of %d member(s) active)", active, len(members))
}

// memberNote is the NOTE column: the member's landing, its win, or its
// tombstone.
func memberNote(w *watcher.Watch, g *watcher.WatchGroup, now time.Time) string {
	switch {
	case g.WinnerWatch == w.WatchID:
		return "winner"
	case w.CancelledReason != "":
		return w.CancelledReason
	}
	return landingDesc(w, now)
}

// groupMemberCanceller is what cancelling a group needs from the store.
type groupMemberCanceller interface {
	CloseGroup(ctx context.Context, name, reason string) (*watcher.WatchGroup, bool, error)
	GroupMembers(ctx context.Context, g *watcher.WatchGroup) ([]*watcher.Watch, error)
	CancelGroupMember(ctx context.Context, watchID, reason string) (bool, error)
}

// cancelGroup is `cancel <group>` after the AWS wiring: it ends the group
// (hunting → cancelled), then cancels every active member the way `cancel
// <watch>` cancels one (a held reservation is released; a launch still
// landing is the caller's, and its TTL applies), with the group's tombstone.
// It returns the members it cancelled. An already-ended group still has any
// member left active cancelled.
func cancelGroup(ctx context.Context, out, errOut io.Writer, store groupMemberCanceller, g *watcher.WatchGroup,
	rel reservationReleaser, confirm func(string) bool) ([]*watcher.Watch, error) {
	members, err := store.GroupMembers(ctx, g)
	if err != nil {
		return nil, fmt.Errorf("read group %s members: %w", g.Name, err)
	}
	var active []*watcher.Watch
	for _, w := range members {
		if w.Status == watcher.StatusActive {
			active = append(active, w)
		}
	}
	if g.State != watcher.GroupHunting && len(active) == 0 {
		return nil, fmt.Errorf("group %s is not hunting (%s) and has no active member", g.Name, groupStateLine(g, members))
	}
	ids := make([]string, len(active))
	for i, w := range active {
		ids[i] = w.WatchID
	}
	sort.Strings(ids)
	if !confirm(fmt.Sprintf("Cancel group %s and its %d active member(s) (%s)?", g.Name, len(active), strings.Join(ids, ", "))) {
		_, _ = fmt.Fprintln(out, "Aborted.")
		return nil, nil
	}
	if ng, ok, err := store.CloseGroup(ctx, g.Name, "cancelled by its owner (lagotto cancel "+g.Name+")"); err != nil {
		return nil, err
	} else if ok {
		g = ng
		_, _ = fmt.Fprintf(out, "Cancelled group %s\n", g.Name)
	}
	reason := g.CancelReason()
	var cancelled []*watcher.Watch
	setStatus := func(ctx context.Context, id string, _ watcher.WatchStatus) error {
		ok, err := store.CancelGroupMember(ctx, id, reason)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("watch %s is no longer active", id)
		}
		return nil
	}
	always := func(string) bool { return true }
	for _, w := range active {
		if err := cancelWatch(ctx, out, errOut, w, rel, setStatus, always); err != nil {
			_, _ = fmt.Fprintf(errOut, "Warning: %v\n", err)
			continue
		}
		cancelled = append(cancelled, w)
	}
	return cancelled, nil
}

func runCancelGroup(ctx context.Context, out, errOut io.Writer, store *watcher.Store, stsClient stsIdentityAPI,
	rel reservationReleaser, arg string, cleanup func(w *watcher.Watch)) error {
	g, err := getGroupOwned(ctx, store, stsClient, groupName(arg))
	if err != nil {
		return err
	}
	confirm := func(prompt string) bool { return cancelYes || confirmCancel(prompt) }
	cancelled, err := cancelGroup(ctx, out, errOut, store, g, rel, confirm)
	if err != nil {
		return err
	}
	for _, w := range cancelled {
		cleanup(w)
	}
	return nil
}
