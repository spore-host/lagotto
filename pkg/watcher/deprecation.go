package watcher

// The deprecation of --action notify (#283). Removal is a later 0.x release.
//
// A notify watch fires when the instance type is OFFERED in a region at or
// under the watch's --max-price (an offering/price match, see matcher.go). That
// is not capacity: it launches and reserves nothing, and for a scarce type it
// fires within minutes while a real launch gets InsufficientInstanceCapacity
// (#239). --action hold and --action spawn are the forms that find capacity.
//
// The text is exported so the other lagotto front ends (spore-host-mcp,
// lagotto-ts) can show the same words.

// DeprecationActionNotify is the Deprecation.ID of the --action notify
// deprecation, a stable key for scripts reading -o json.
const DeprecationActionNotify = "action-notify"

// NotifyOfferedNote is the short form of what a notify watch checks, used in
// its own notifications.
const NotifyOfferedNote = "offered, not a capacity check"

// NotifyDeprecation is the deprecation warning `lagotto watch --action notify`
// and `lagotto update` on a notify watch print, and that `lagotto doctor`
// attaches to its notify-watch check.
const NotifyDeprecation = "--action notify is deprecated and will be removed in a later release. " +
	"A notify watch only checks that the instance type is offered in the region at or under your max price, " +
	"not a capacity check: it launches and reserves nothing, and for a scarce type it can fire while a real launch " +
	"fails with InsufficientInstanceCapacity.\n" +
	"Use instead:\n" +
	"  --action hold   reserves the capacity when it is found: a targeted On-Demand Capacity Reservation " +
	"for 30 minutes, billed at the On-Demand rate from the moment it is held, even if unused.\n" +
	"  --action spawn  launches the instance when it is found (needs --spawn-config): the launch is the capacity test, " +
	"and the instance bills until its TTL."

// NotifyDefaultActionNote prefixes NotifyDeprecation when the watch was given
// no --action, so it is a notify watch by default. The default does not change
// in the deprecation release.
const NotifyDefaultActionNote = "no --action given, so this is a notify watch (deprecated); pass --action explicitly."

// Deprecation is one deprecated feature a command used, for its -o json
// output's "deprecations" field.
type Deprecation struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

// NotifyDeprecationMessage is the warning for a notify watch. actionGiven is
// false when the action came from the --action default rather than the user.
func NotifyDeprecationMessage(actionGiven bool) string {
	if actionGiven {
		return NotifyDeprecation
	}
	return NotifyDefaultActionNote + "\n" + NotifyDeprecation
}

// ActionDeprecations returns the deprecations a watch with this action uses:
// one for notify, none for spawn or hold.
func ActionDeprecations(action ActionMode, actionGiven bool) []Deprecation {
	if action != ActionNotify {
		return nil
	}
	return []Deprecation{{ID: DeprecationActionNotify, Message: NotifyDeprecationMessage(actionGiven)}}
}
