package watcher

import (
	"errors"
	"strings"
	"time"

	"github.com/aws/smithy-go"
)

// Heartbeat (#239 phase 1). A long-lived watch used to show only LastPolledAt,
// and that only in `status`: nothing said how many launches it had tried or why
// they failed. These helpers build the attempts / last_error / last_poll_error
// fields. The store folds them into the write each cycle already makes, so the
// heartbeat costs no extra DynamoDB writes.

// MaxLastErrorBytes bounds LastError and LastPollError. An AWS error message
// can be long (spawn wraps several layers), and the record is rewritten every
// cycle, so keep it to what a human reads.
const MaxLastErrorBytes = 256

// Error classes, the first word of LastError / LastPollError. `lagotto list`
// shows just the class; `status`, `list -o wide` and JSON show the whole string.
const (
	ErrClassCapacity = "capacity"
	ErrClassQuota    = "quota"
	ErrClassTerminal = "terminal"
	ErrClassUnknown  = "unknown"
	ErrClassSearch   = "search"
)

// attempt is one action attempt's heartbeat, handed to the store write that the
// cycle makes anyway. Err is the formatted LastError ("" for a success, which
// leaves last_error untouched).
type attempt struct {
	Err string
}

// failedAttempt returns the heartbeat for a failed attempt of the given kind.
func failedAttempt(kind FailureKind, err error) *attempt {
	return &attempt{Err: describeAttemptError(kind, err)}
}

// errorClass maps a failure kind (and whether it was a quota refusal) to its
// class word.
func errorClass(kind FailureKind, err error) string {
	if err != nil && IsQuotaExceeded(err) {
		return ErrClassQuota
	}
	switch kind {
	case FailureCapacity:
		return ErrClassCapacity
	case FailureTerminal:
		return ErrClassTerminal
	default:
		return ErrClassUnknown
	}
}

// describeAttemptError renders a failed attempt as "<class>: <code>: <message>"
// (or "<class>: <message>" when there is no AWS code), on one line, truncated to
// MaxLastErrorBytes. A nil error gives just the class.
func describeAttemptError(kind FailureKind, err error) string {
	class := errorClass(kind, err)
	if err == nil {
		return class
	}
	var b strings.Builder
	b.WriteString(class)
	msg := err.Error()
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if code := apiErr.ErrorCode(); code != "" {
			b.WriteString(": ")
			b.WriteString(code)
		}
		msg = apiErr.ErrorMessage()
		if msg == apiErr.ErrorCode() {
			msg = "" // nothing the code didn't already say
		}
	}
	if msg = oneLine(msg); msg != "" {
		b.WriteString(": ")
		b.WriteString(msg)
	}
	return truncateBytes(b.String(), MaxLastErrorBytes)
}

// describePollError renders a failed capacity search as "search: <message>".
func describePollError(err error) string {
	return truncateBytes(ErrClassSearch+": "+oneLine(err.Error()), MaxLastErrorBytes)
}

// oneLine collapses all whitespace runs (newlines included) to single spaces.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// ErrorClass returns the class word of a LastError / LastPollError string (the
// text before the first colon), or "" for an empty one.
func ErrorClass(e string) string {
	if i := strings.IndexByte(e, ':'); i >= 0 {
		return e[:i]
	}
	return e
}

// LatestError returns the more recent of LastError and LastPollError, and when
// it happened. Both empty gives "", zero time.
func (w *Watch) LatestError() (string, time.Time) {
	if w.LastPollError != "" && (w.LastError == "" || w.LastPollErrorAt.After(w.LastErrorAt)) {
		return w.LastPollError, w.LastPollErrorAt
	}
	return w.LastError, w.LastErrorAt
}

// noteAttempt mirrors a heartbeat write onto the in-memory watch, so a later
// write in the same cycle (RecordMatch's whole-record PutWatch) carries it.
func (w *Watch) noteAttempt(a *attempt, now time.Time) {
	if a == nil {
		return
	}
	w.Attempts++
	w.LastAttemptAt = now
	if a.Err != "" {
		w.LastError = a.Err
		w.LastErrorAt = now
	}
}

// updateExpr assembles a DynamoDB update expression from its clauses, in the
// order REMOVE, SET, ADD. Each clause is omitted when empty, so a write with no
// heartbeat produces exactly the expression it did before #239.
func updateExpr(remove, set, add []string) string {
	var parts []string
	if len(remove) > 0 {
		parts = append(parts, "REMOVE "+strings.Join(remove, ", "))
	}
	if len(set) > 0 {
		parts = append(parts, "SET "+strings.Join(set, ", "))
	}
	if len(add) > 0 {
		parts = append(parts, "ADD "+strings.Join(add, ", "))
	}
	return strings.Join(parts, " ")
}
