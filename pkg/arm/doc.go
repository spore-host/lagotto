// Package arm is what a program must do, beyond storing it, to create or end a
// watch that the hosted poller will actually service and whose email channels
// actually deliver (#266).
//
// `lagotto watch` does three things around PutWatch that every other creator of
// a watch must do too. They lived unexported in cmd/, so spore-host-mcp's
// lagotto_watch skipped them and shipped two silent-failure bugs
// (spore-host-mcp#78, #81), then fixed them by copying the code. This package is
// that code, exported, so the CLI and every other caller run the same steps:
//
//   - The poll schedule (#231). The hosted poller switches its own schedule off
//     when no watch is active, so arming a watch must switch it back on:
//     PreflightPollSchedule BEFORE storing the watch, store it, then
//     EnablePollScheduleIfOff; if that fails, undo the store and return
//     PollScheduleUpdateRefusal. pollschedule.go has the full sequence.
//   - The alerts topic (#241): ResolveAlertsTopic finds the SNS topic email
//     channels are delivered through, the way the hosted poller gets it.
//   - Email subscriptions (#240): SubscribeWatchEmails subscribes each email
//     address on the watch BEFORE it is stored (refusing if one cannot be), and
//     CleanupWatchEmails is a cancel path's half of the deleter.
//
// Constructing the watch itself is watcher.NewWatch, which sets the expiry and
// the record's retention TTL together (spore-host-mcp#77).
//
// The functions are writer-free: they return what the CLI prints (an
// "enabled" flag, warnings, a ReconcileResult) rather than printing it, so a
// non-CLI caller renders them its own way. Error text is the CLI's, verbatim.
//
// Why a package of its own: these steps are one protocol that every watch
// creator runs together, so they are imported together. pkg/watcher cannot host
// the topic resolution without importing pkg/deploy (the topic's name and ARN),
// which would put deploy's whole dependency graph into the Lambda poller, which
// links pkg/watcher. pkg/deploy is the install/teardown of the infrastructure,
// not a per-watch runtime step. arm imports both and neither imports it.
package arm
