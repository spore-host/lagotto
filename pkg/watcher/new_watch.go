package watcher

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Lifetime is how long a new watch hunts: a TTL, or until-landed (#239) with a
// reminder cadence.
type Lifetime struct {
	// TTL is how long an ordinary watch keeps watching. It must be > 0, and
	// zero for an until-landed watch.
	TTL time.Duration

	// UntilLanded makes the watch keep hunting with no TTL, behind a rolling
	// acknowledgement deadline of (RemindPauseAfter+1) × RemindEvery.
	UntilLanded bool
	// RemindEvery is the until-landed reminder interval (DefaultRemindEvery,
	// within MinRemindEvery..MaxRemindEvery).
	RemindEvery time.Duration
	// RemindPauseAfter is how many unacknowledged reminders precede the pause
	// (DefaultRemindPauseAfter).
	RemindPauseAfter int
}

// NewWatch returns a new active watch, with a fresh ID, created at now and
// living for life. The caller fills in what to watch (owner, pattern, regions,
// action, channels, ...) and stores it.
//
// It is the ONE place a watch's timestamps are set at creation (#266):
// CreatedAt and UpdatedAt are now (UTC); ExpiresAt is when lagotto STOPS
// WATCHING, now+TTL, or for an until-landed watch the first acknowledgement
// deadline; and TTLTimestamp, when DynamoDB FORGETS THE RECORD, is
// RetentionTTL(ExpiresAt). Those two must never be the same instant: a record
// whose ttl_timestamp equals its expiry is deleted out from under the
// status=expired tombstone the poller writes at expiry. Setting it by hand is
// how that bug happened three times (#41, #161, spore-host-mcp#77).
//
// An until-landed watch's deadline is stored in ExpiresAt so a poller that
// predates #239 still stops it there (see until_landed.go).
func NewWatch(now time.Time, life Lifetime) (*Watch, error) {
	now = now.UTC()
	w := &Watch{
		WatchID:   newWatchID(),
		Status:    StatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
	switch {
	case life.UntilLanded:
		if life.TTL != 0 {
			return nil, errors.New("an until-landed watch has no TTL")
		}
		if err := ValidateRemindEvery(life.RemindEvery); err != nil {
			return nil, err
		}
		if life.RemindPauseAfter < 0 {
			return nil, fmt.Errorf("until-landed pause-after must not be negative (got %d)", life.RemindPauseAfter)
		}
		w.UntilLanded = true
		w.RemindEverySeconds = int64(life.RemindEvery / time.Second)
		w.RemindPauseAfter = life.RemindPauseAfter
		w.ExpiresAt = now.Add(time.Duration(life.RemindPauseAfter+1) * life.RemindEvery)
	case life.TTL <= 0:
		return nil, fmt.Errorf("invalid TTL %s: it must be greater than zero", life.TTL)
	default:
		w.ExpiresAt = now.Add(life.TTL)
	}
	w.TTLTimestamp = RetentionTTL(w.ExpiresAt)
	return w, nil
}

// newWatchID returns a fresh watch ID: "w-" and 8 hex digits.
func newWatchID() string {
	return "w-" + uuid.New().String()[:8]
}
