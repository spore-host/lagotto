package watcher

import "fmt"

// HoldLaunchCommand renders the `spawn launch` command that launches into a
// fired hold's Capacity Reservation (#208). It is the one source for that text:
// the match email, `lagotto status` and (rendered with placeholders, checked by
// a test) the README.
//
// Each part is required for the command to work as written. spawn needs a
// spore name, as the positional or --name, and errors without one. It does not
// read the instance type from the reservation, so --instance-type must be
// given (without it spawn opens its wizard on a TTY and fails otherwise). The
// reservation is targeted and AZ-bound, so --region, --az and
// --reservation-id all have to match it. The #203 text omitted the name and
// the type, so a user copying it inside the reservation's short window got an
// error instead of an instance.
func HoldLaunchCommand(name, region, az, instanceType, reservationID string) string {
	return fmt.Sprintf("spawn launch %s --region %s --az %s --instance-type %s --reservation-id %s",
		name, region, az, instanceType, reservationID)
}

// HoldLaunchCommandFor is HoldLaunchCommand for a watch's fired hold. The watch
// id is the spore name: it is unique, already a valid spore name (lowercase
// letters, digits and hyphens), and it ties the instance back to the watch. A
// match recorded without its instance type renders <instance-type>, to fill
// in, rather than an empty flag spawn would reject.
func HoldLaunchCommandFor(w *Watch, m *MatchResult) string {
	instanceType := m.InstanceType
	if instanceType == "" {
		instanceType = "<instance-type>"
	}
	return HoldLaunchCommand(w.WatchID, m.Region, m.AvailabilityZone, instanceType, m.ReservationID)
}
