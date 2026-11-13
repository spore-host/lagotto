# Changelog

All notable changes to **lagotto** are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`docs-gen/_levels.json`: the disclosure level of every command and flag** (#297), for the spore.host portal's CLI⇄GUI parity gate. `lagotto gen-docs` writes it beside the markdown reference, which is unchanged. Each command's level comes from its `spore.host/level` cobra annotation, and each flag's from the same annotation on the flag (unannotated flags are `advanced`). Every command must set a level explicitly, and a test fails when one does not. The basic set is `watch` (with `--action`), `list`, `status`, `ack`, `cancel` and `history`. The format, `spore.host/cli-levels/v1`, is proposed for spawn and truffle too.

### Fixed

- **A watch created without `--regions` now matches** (#306). An empty region list means "all enabled regions" (and `lagotto list` shows it as `(all enabled)`), but the hosted poller passed the empty list straight to the capacity search, which then searched zero regions: the watch never matched, and the only sign was the "check for typos" warning. The poller now resolves an empty list to the account's enabled regions (`ec2:DescribeRegions`, already granted; cached for 10 minutes), and records a poll error on the watch if they can't be listed, rather than searching nothing.
- **`lagotto deploy --teardown` deletes the poller's log group** `/aws/lambda/lagotto-capacity-poller` in the region it tears down, after the function (#305). Lambda used to create the group implicitly on the first invocation, so nothing owned it and every teardown left it behind. `lagotto deploy` now creates it before the function, with a 30-day retention and the `lagotto:managed=cli` tag; an existing group is tagged, and given the retention only if it has none. A re-run of `deploy --teardown` removes a group an earlier teardown left. `deploy` needs `logs:CreateLogGroup`, `logs:TagResource`, `logs:DescribeLogGroups` and `logs:PutRetentionPolicy`, and `deploy --teardown` needs `logs:DeleteLogGroup` (administrator commands; `lagotto-cli-user` is unchanged).

## [0.70.0] - 2026-11-12

### Added

- `SECURITY.md`: report vulnerabilities privately through GitHub's private vulnerability reporting (the repository's Security tab), not public issues.
- `lagotto setup --sagemaker-role ARN` / `--revoke-sagemaker-role ARN`: the SageMaker execution roles a `--service sagemaker` watch may name in its `RoleArn`.

### Security

- **The hosted poller no longer creates or modifies IAM roles.** Its runtime policy drops `iam:CreateRole`, `iam:PutRolePolicy`, `iam:AttachRolePolicy` and the other role and instance-profile writes on `spawn-instance*` and `spored*`. `iam:PassRole` is granted only on exact role ARNs: `lagotto-instance-role` and the roles authorized with `setup --instance-role` (to EC2), and the roles authorized with `setup --sagemaker-role` (to SageMaker, no longer on `*`). SageMaker job actions are scoped to `training-job/lagotto-*`.
- **Watches are isolated per owner by IAM.** Every watch, group, match-history row and scheduled launch is keyed under its creator's principal id (`<id>/w-…`), and `lagotto-cli-user` allows DynamoDB item access only where `dynamodb:LeadingKeys` is under the caller's own id (`${aws:userid}`); it no longer grants `dynamodb:Scan` or queries on `status-index`/`user_id-index`. `lagotto list` reads a new keys-only `owner_id-index`. The poller resolves a watch's group, and a group's members, only within the same owner. `lagotto setup` adds the index and moves existing records under their owners, each in one transaction (idempotent, resumable); records whose creator cannot be resolved are left in place and listed.
- **Launch settings that grant privilege are validated when a watch is created and again by the poller before every launch.** `iam_policy`, `iam_policy_file` and `iam_policy_document` are refused; `iam_role` may not name `spored-instance-role` (or any `spored*` role), a spawn nested-spawn role or a lagotto poller role; a user-data file reference is refused at launch (the stored watch carries its user data inline); a SageMaker job must name a plain, unreserved `RoleArn`. A stored watch that breaks these rules stops as failed without launching.

### Changed

- **Upgrade order:** deploy the new poller, then run `lagotto setup`, then upgrade the CLI everywhere. CLIs older than this release cannot read or create watches once `setup` has written the owner-scoped policy, and `watch` warns when the deployed poller predates owner keys. `lagotto notify-gc` and doctor's table checks now need administrator credentials (the poller still removes unused email subscriptions on each sweep).
- **Watches and scheduled launches record their creator's principal id** (`owner_id`, the STS `UserId`) next to the ARN, and ownership is checked against it when present, so a role deleted and re-created under the same name does not inherit the old role's watches. A watch created before this records it on its next `lagotto update`.
- **A watch with no `iam_role` now launches with `lagotto-instance-role`**, which `lagotto setup` creates with spawn's spored/SSM baseline, instead of spawn's shared `spored-instance-role`. `setup --instance-role NAME` now requires the role to exist, adds the same baseline to it and creates its instance profile; `--revoke-instance-role` deletes the profile setup created. `lagotto teardown --iam-roles` deletes `lagotto-instance-role` and those profiles. **After upgrading, run `lagotto setup`** (with the new poller deployed): until then hosted launches fail. A workload that needs more than the baseline (for example S3 access) needs a role an administrator authorizes with `setup --instance-role`, named by `iam_role`.

## [0.69.2] - 2026-11-11

### Fixed

- **The generated CLI reference escapes `<group>` in `lagotto list`'s help** (libs v0.51.5, spore-host/libs#73). A wrapped, indented help line reached the spore.host docs site raw, where it compiled as an unclosed HTML tag and failed the docs build, blocking the shared CLI-reference sync (spore-host/spore-host#647).

## [0.69.1] - 2026-11-11

### Security

- Build and test with Go 1.26.11 / 1.27.4: GO-2026-6859 (net/http: an HTTP/1 server crash from a `MaxBytesReader` header race), published 2026-11-11.

## [0.69.0] - 2026-11-08

### Added

- **Watch groups: the first member to land cancels its siblings** (#287).
  `lagotto watch … --group <name>` creates or joins a group; members can
  differ in pattern, regions, Spot, action (spawn or hold) and spawn config.
  - Before it launches or reserves, a member claims the group's slot with a
    DynamoDB conditional write; a member that can't launches nothing that
    cycle. The winner is decided at landing (`running`, or an `active`
    reservation), so a launch that dies in pending frees the slot and the group
    keeps hunting.
  - When a member wins, every other active member is cancelled with a
    tombstone (`cancelled_reason`: "group <name> landed by w-… (i-…, type,
    AZ)", shown by `lagotto status`) and a `group_cancelled` history row. A
    sibling's pending reservation is released. The group sends one
    notification, to every member's channels, in place of the winner's own
    (webhook/SNS `"event": "group_landed"`, with `group` and `cancelled`).
  - `--group-on-extra terminate|keep` (default `terminate`): a launch that lands
    after the group was won is terminated through spawn's terminate path (or
    its reservation released), or kept. Either way the member is cancelled, a
    `group_extra` history row is written and the member is notified
    (`"event": "group_extra"`).
  - `lagotto list` has a `GROUP` column. `lagotto status <group>` shows the
    group's state (hunting, landed by whom, or cancelled), who holds the slot,
    each member's status, attempts and last error, and the group's cost
    exposure: the largest member's, plus the total when a member keeps extras.
    `lagotto cancel <group>` ends the group and cancels every member.
  - Not with `--action notify` (deprecated, #283), `--maintain`,
    `--until-landed` (one acknowledgement clock per group is #290) or
    `--service sagemaker`. A group belongs to the caller who created it; a
    group that ended is re-armed when a new member joins.
  - The group is one item in the watches table (`watch_id` `g-<name>`) with no
    `status` or `user_id` attribute, so it is in neither GSI: no poller, no
    `lagotto list` and no spawn dashboard sees it as a watch. No new table, no
    IAM change; its deleter is the table's TTL and `lagotto teardown`.
  - Pollers before this release ignore the group fields and poll each member as
    an independent watch. `lagotto watch --group` warns when the deployed
    poller is older (`arm.CheckGroupPoller`, `arm.GroupPollerWarning`).
- **The until-landed reminder-poller warning is exported** (#282) for watch
  creators other than the CLI, such as spore-host-mcp's `lagotto_watch`,
  `lagotto_ack` and `lagotto_status`. `arm.CheckReminderPoller(ctx, inspector,
  region, w, resumeHint)` inspects the hosted poller (an `arm.PollerInspector`,
  which `*deploy.Deployer` satisfies). `arm.ReminderPollerWarning(info,
  inspectErr, region, w, resumeHint)` decides from an inspection you already
  have. Both return the warning, or `""` when there is nothing to warn about or
  no way to know. The poller is inspected only for an active until-landed
  watch. `resumeHint` names your resume step (for example `lagotto_ack`); `""`
  gives the CLI's `'lagotto ack <id>'` (`arm.CLIResumeHint`). The CLI now calls
  this through a thin wrapper, and what it prints is unchanged.
- **`watcher.StalePollAfter`** (15 minutes, #282) is exported: the age at which
  `lagotto list` flags an active watch's last poll with `!` and `status` calls
  it stale. Other callers can flag the same staleness without guessing a
  threshold.

### Deprecated

- **`--action notify` is deprecated** (#283) and will be removed in a later
  0.x release. A notify watch fires when the type is *offered* in the region at
  or under your max price. That is not a capacity check: it launches and
  reserves nothing, and a p5.48xlarge notify watch fired within 3 minutes while
  a real launch in the same AZs got `InsufficientInstanceCapacity`. Use
  `--action hold` (reserve the capacity) or `--action spawn` (launch it).
  Notify still works in this release:
  - `lagotto watch --action notify`, a `lagotto watch` with no `--action` (the
    default is still notify, and that warning asks you to pass `--action`
    explicitly), and `lagotto update` on a notify watch print a deprecation
    warning on stderr. Stdout is unchanged; `-o json` adds a `deprecations`
    field (`[{"id": "action-notify", "message": …}]`).
  - The `--action` help marks notify deprecated.
  - A notify watch's notification now says the type was *offered, not a
    capacity check* (the email subject no longer says "capacity found"), and its
    webhook and SNS payloads carry `"deprecated": true`, a `note` and the
    `deprecation` text.
  - `lagotto doctor` has a `notify-watches` warning listing the active notify
    watches. It never fails, and is omitted when there are none.
  - The warning text is exported as `watcher.NotifyDeprecation` (with
    `NotifyDefaultActionNote` and `ActionDeprecations`) for the other front ends.
  - Notification channels (`--notify email:/webhook:/sns:`) are not deprecated:
    spawn and hold watches, reminders and fleet events report through them.

### Fixed

- **EC2 throttling no longer ends a watch, and the poller backs off** (#254).
  During an AZ operational issue EC2 throttles `RunInstances` with
  `RequestLimitExceeded` ("... throttled due to an operational issue in
  availability zone usw2-az1"). The poller counted that as an unrecognised
  failure, so a throttle ended watches that had waited 17-49 h for capacity as
  `spawn_failed`, and it retried every 2-3 s across watches, which is what EC2's
  throttling penalises. Now:
  - `RequestLimitExceeded`, `Throttling` and `ThrottlingException` are a new
    `throttled` class. A throttle never ends a watch and never counts toward
    the 10-failure cap for unrecognised errors.
  - A throttle that names no AZ backs the whole region off for every watch in
    the account: not tried again in the same sweep, then exponential backoff
    with jitter from 30 s to a 10-minute cap. It resets after 30 minutes with
    no throttle.
  - A throttle that names an AZ ID cools that AZ off for 45 minutes. The poller
    maps the ID to the account's AZ name with `ec2:DescribeSubnets`, which it
    already has. Every watch skips that AZ and still tries its other AZs and
    regions.
  - The state is stored on the watches (`throttle`, `az_cooloff`), so the
    hosted poller keeps it between invocations. The runtime policy is
    unchanged.
  - `lagotto list` and `status` show it. A throttled attempt is
    `throttled: RequestLimitExceeded: ...`, a sweep that skipped an impaired AZ
    starts `impaired: us-west-2a (usw2-az1) until 00:07Z; ...`, and a cycle
    skipped while backing off sets `last_poll_error` to
    `throttled: us-west-2: RequestLimitExceeded, backing off until ...`. The
    poll summary has a `throttled` count.
  - A Capacity Block scheduled launch that is throttled at its start time is
    retried, as a capacity error there already is. A throttled SageMaker
    `CreateTrainingJob` is retried too; before, it ended the watch.
  - `pkg/failure` exports `FailureThrottled`, `IsThrottled`, `ThrottledAZID`
    and `ErrThrottled`. `pkg/watcher` exports `Poller.CoolOffAZ` and
    `Poller.AZCooledOff`, so other code that learns an AZ is impaired can use
    the same cool-off.
- **A spawn or hold lost before it lands is now notified, and history keeps
  why** (#255). Since 0.67.0 (#239) a launch only matches once it is
  `running`, so a watch no longer completes as `spawned` on an instance EC2
  then kills in `pending`. What was still missing:
  - Every lost launch or hold (`landing_failed`, `landing_timed_out`) is now
    sent to the watch's channels, with the instance or reservation, its state,
    EC2's reason, and whether the watch is hunting again or stopped. Before,
    only the tenth unclassified loss in a row was sent, and it went out as the
    match email ("capacity found"). The email subject is
    `[lagotto] <type> launch in <region> lost before it landed: still hunting`
    (or `watch stopped`). Webhook and SNS get a new event payload: `event`
    (the history action), `status`, `instance_id` or `reservation_id`,
    `state`, `reason`, `launched_at`, `lost_at`. The match payload is unchanged.
  - The history row also records `lost_at` (when the poller saw the loss;
    `matched_at` stays the launch time) and EC2's `StateTransitionReason`,
    which carries EC2's own timestamp, alongside the `StateReason`. The code is
    no longer repeated when EC2's message starts with it.
  - `lagotto history` prints a detail line under a lost landing (instance,
    state, reason, when seen) and no longer gives it a wait-to-acquire.
  - **spored check-in follow-up (advisory).** A spawn watch still matches at
    `running`. After that the poller checks the instance for spored's
    `spawn:last-heartbeat` or `spawn:spored-version` tag, once per sweep, for
    up to 10 minutes. It uses the DescribeInstances the landing check already
    makes, so no new IAM is needed. If the tag appears, the match row and the
    watch's `last_match` get `spored_confirmed_at` and nothing else happens.
    If it does not appear in time, or the instance stops or terminates first,
    the poller writes a `spored_unconfirmed` history row and notifies the
    watch's channels (webhook/SNS `event: spored_unconfirmed`). Both give the
    instance, how long it has been running, and a note that a watch whose
    spawn config names its own `iam_role` may lack `ec2:CreateTags`. lagotto
    never terminates the instance, and the follow-up then stops. Holds are not
    checked. The pending check is stored as three attributes on the matched
    watch (`spored_check_instance_id`, `_region`, `_until`), which an older
    poller ignores.
  - A spawn EC2 terminates in `pending` with an insufficient-capacity reason
    in a known AZ now cools that AZ off through the #254 cool-off
    (`CoolOffAZ`, reason `early-death`). The relaunch in the same sweep, and
    later sweeps, go to another AZ. A user- or instance-initiated shutdown, a
    Spot reclaim, `Server.InternalError`, a stop, a timeout or a failed hold
    does not cool off an AZ.

## [0.68.0] - 2026-11-07

### Added

- **`lagotto setup` writes a least-privilege policy for the people who run
  lagotto, `lagotto-cli-user`** (#232), so an administrator no longer works out
  and hand-grants what the everyday commands need. Attach it to a group, user
  or role.
  - Covers `watch`, `update`, `ack`, `extend`, `cancel`, `list`, `status`,
    `history`, `launch`, `notify-gc` and `doctor`. Not `poll` (it needs the
    poller's own launch permissions, including writing and passing instance
    roles) or the administrator commands `setup`, `deploy` and `teardown`.
  - Resource-scoped to lagotto's three tables (and their indexes, query only),
    the poller's schedule, the `lagotto-launch-sl-*` schedules (create, and delete
    for `cancel sl-…`, #279),
    the alerts topic (subscribe by email only), lagotto's two roles (read only),
    the poller function and doctor's stack and artifact buckets.
    `iam:PassRole` only on `lagotto-capacity-poller-scheduler-invoke`, only to
    EventBridge Scheduler; `ec2:CancelCapacityReservation` only on
    reservations tagged `lagotto:managed=true`. `*` only for actions IAM does
    not scope (EC2 describes, Service Quotas, `scheduler:ListSchedules`,
    `s3:ListAllMyBuckets`, `cloudwatch:GetMetricStatistics`).
  - Generated from the CLI's own AWS calls: CI fails if an everyday command
    calls an action the policy lacks, or the policy grants one nothing calls.
  - s3:// spawn configs are readable only from buckets named with
    `setup --config-bucket <bucket>`; they are kept on later runs, and
    `--revoke-config-bucket` removes one.
  - A changed policy is written as a new default version, deleting the oldest
    non-default one when IAM's five-version limit is reached; an unchanged one
    is left alone. A same-named policy without lagotto's `lagotto:managed=cli`
    tag is never touched.
  - `lagotto teardown --iam-roles` deletes it: it detaches the policy from
    every user, group and role (and prints each), deletes its old versions,
    then the policy.
  - `lagotto doctor` has a new `caller-permissions` check: it simulates your
    identity against the policy (`iam:SimulatePrincipalPolicy`; an assumed
    role is simulated as its IAM role) and lists what is denied, with the
    attach command. It warns, never fails, and reports "unknown" when the
    simulation itself is not permitted. Three granted actions are listed as
    "not simulated" rather than checked, because the IAM policy simulator
    denies them for this exact policy although real IAM allows them:
    `ec2:CancelCapacityReservation` (its `ec2:ResourceTag` condition), and
    `sns:GetSubscriptionAttributes` and `sns:Unsubscribe` (subscription ARNs).

- **`--require-quota` on `lagotto watch` and `lagotto update`: refuse a fleet
  that provably can't reach `--maintain N`** (#157). Opt-in: without it the
  #153 quota warning is unchanged and the watch is still created.
  - Refuses only when the EC2 vCPU quota was read in every `--regions` region
    and, summed across those regions (a fleet counts its workers in all of
    them), holds fewer than N workers. The refusal exits with status 3, so a
    pipeline can tell "quota too low" from any other failure, and nothing is
    stored.
  - An unreadable quota is unknown, not zero: a failed or denied lookup, a
    family the region doesn't report, a wildcard-only pattern with free vCPUs
    (no worker size to count), or a watch with no `--regions` never refuses.
    The watch is created and a note on stderr says which region could not be
    checked and why.
  - Unlike the warning, the gate checks every listed region, not just the
    first three. `-o json` adds a `quota_gate` object with the verdict
    (`feasible`, `infeasible`, `unknown`), the per-region figures and the
    reason. It uses the Service Quotas reads the CLI already makes; no IAM
    change.

- **`lagotto update <watch-id>`: change an existing watch's search in place**
  (#257), instead of cancelling and re-creating it. The watch keeps its ID,
  attempt count, last error, match history and confirmed email subscriptions,
  and keeps hunting.
  - Changes the instance-type pattern (`--instance-type`, or `--add-type`),
    the regions (`--regions`, `--add-region`, `--remove-region`), `--azs`,
    `--spot` / `--no-spot`, `--max-price`, the notify channels (`--notify`,
    `--add-notify`, `--remove-notify`), `--spawn-config` and `--project`. The
    action, service, owner, ID and deadline cannot be changed.
  - The updated watch goes through the same checks as `lagotto watch`,
    including default-subnet coverage (#256), the fleet quota warning and the
    instance-role check, so an update cannot make a watch `watch` would
    refuse.
  - Prints a before/after diff of the changed fields; `--dry-run` runs every
    check and shows the diff without writing anything. `-o json` gives the
    same as a document.
  - When the search changes, the consecutive-failure count and the quota state
    (quota-refused candidates, quota cap) are reset; attempts, last error,
    match count and history are kept.
  - Only added email addresses are subscribed (and must confirm); a removed
    one is unsubscribed unless another active watch names it.
  - Refused while a poller is acting on the watch, while a launch or hold it
    made is still landing, and while a SageMaker job it submitted is in
    flight; queuing the change until landing resolves is not implemented. An
    expired or failed watch needs `--reactivate`, and must still be before
    its deadline (`lagotto extend` first if not).
  - The write is a single conditional update on the `updated_at` read, so it
    never overwrites anything the poller wrote meanwhile, and it holds the
    watch for one lease length (2 minutes) so a poller that read the old
    search just before cannot act on it.
  - Each update adds an `updated` row to `lagotto history`, listing the
    changed fields (`"event": "updated"` in `-o json`). It is not a match and
    has no wait-to-acquire.
  - Needs the same IAM as `lagotto watch`, plus `dynamodb:UpdateItem` and
    `sns:Unsubscribe`; see `docs/reference/cli-iam-actions.md`.

- **`lagotto watch --until-landed`: keep hunting until the launch or hold
  lands, with no TTL** (#239, phase 3). Instead of a TTL the watch has a
  rolling acknowledgement deadline: it pauses itself unless you run the new
  **`lagotto ack <watch-id>`** within 3 × `--remind` (default `--remind 7d`, so
  21 days; `--remind` is 1h to 30d) of creation or the last ack. `ack` moves
  the deadline a full window ahead from now. A paused watch shows as
  `status: expired` with the new `expired_reason: unacknowledged`, gets a
  "paused" notification instead of the usual expiry one, and `ack` (or
  `extend`) resumes it.
  - For `--action spawn` and `--action hold` only. `--action notify` is
    refused for now: notify fires when a type is offered, which is not a sign
    a launch would succeed, so the watch would end on its first poll.
  - Requires a `--notify` channel, and asks for confirmation before creating
    the watch (pass `--yes` when not running in a terminal). It cannot be
    combined with `--ttl`, `--maintain` or `--until`. If its only channels
    are email addresses that haven't confirmed their subscription yet, the
    watch is created with a warning.
  - `lagotto list` shows `EXPIRES` as `until landed (ack in 4d)`, or
    `paused (unacked)`; `lagotto status` shows the deadline and the command
    that moves it.
  - Safe with older pollers: the deadline is stored as the watch's ordinary
    expiry, so a poller that predates this release simply expires the watch
    there. Its record is retained for the usual 90 days after that.

- **Until-landed reminders** (#239, phase 4). An `--until-landed` watch now
  reminds you before it pauses, on its `--notify` channels (email, webhook
  and sns; `"event": "reminder"` in the webhook/sns JSON). With the default
  `--remind 7d` you hear from it on day 7 and day 14 after creation or your
  last `lagotto ack`, and get a final warning 24 hours before the day-21
  pause. With `--remind` of 24h or less, the last reminder is the final
  warning.
  - Each reminder says what the watch is hunting, how the hunt is going
    (attempts, last error, last poll), when it pauses, and the `lagotto ack`
    command that moves the deadline.
  - Each reminder is sent at most once, even with several pollers (the
    hosted poller and a `poll --daemon`) sweeping the same watch. A poller
    that was down when reminders fell due sends only the latest one. A
    reminder that fails to send is not retried; the watch still pauses safely
    at its deadline.
  - `lagotto ack` starts the reminder count again for the new deadline.
  - Reminders are sent by the hosted poller, so **they start only after you
    run `lagotto deploy`** with this release; until then the watch pauses at
    its deadline without reminding you. `lagotto watch --until-landed`,
    `lagotto ack` and `lagotto status` warn when the poller in your region is
    older than the first release that sends reminders. That check reads the
    poller Lambda's `Version` tag, as `lagotto doctor` does, so `watch`,
    `ack`, `extend` and `status` now call `lambda:GetFunction`
    (`docs/reference/cli-iam-actions.md`); without that permission the check
    is skipped silently. The poller's own permissions are unchanged.
  - `lagotto poll` and the poller log report how many reminders a cycle sent.
- **Exported Go API for programs that create watches** (#266), so they no
  longer copy lagotto's unexported `cmd/` code (spore-host-mcp did, and missed
  steps twice: spore-host-mcp#78, #81). The CLI now calls the same code; its
  output, errors and call order are unchanged.
  - New package `pkg/arm`, with the steps `lagotto watch` runs around storing
    a watch: `PreflightPollSchedule`, `EnablePollScheduleIfOff` and
    `PollScheduleUpdateRefusal` turn the hosted poller's schedule back on (#231).
    `ResolveAlertsTopic` finds the alerts topic (#241). `SubscribeWatchEmails`,
    `SetChannelStates` and `CleanupWatchEmails` subscribe and unsubscribe
    email channels (#240). These functions return warnings and results rather
    than printing them.
  - `watcher.NewWatch(now, watcher.Lifetime{...})` builds a new watch, either
    with a TTL or until-landed. It is now the only place that sets a new
    watch's ID, `CreatedAt`, `UpdatedAt`, `ExpiresAt` and `TTLTimestamp`. It
    always sets `TTLTimestamp` to `RetentionTTL(ExpiresAt)`, never to the
    expiry itself (the bug in #41, #161 and spore-host-mcp#77).
    `lagotto watch` uses it.
  - The #214 hermetic test guard moved from `internal/hermetic` to
    `pkg/testutil/hermetic`, so other modules can import it. It is for tests
    only, and only `_test.go` files and `go test -exec` wrappers may import
    it. New `Install` and `Recorder.Take` support a `TestMain` that does its
    own setup.
- **Cost exposure: what a watch can cost if it lands** (#239, phase 5).
  - `lagotto watch` (and the `--until-landed` confirmation prompt) and
    `lagotto status` show an `Exposure:` line. For spawn it is
    "if this lands: up to $X/hr (≈$Y/day)", multiplied by N for a
    `--maintain N` fleet. For hold it is "30 min reserved at up to $Y/hr
    (≈$Z), billed even if unused". Notify watches show "launches and
    reserves nothing", and SageMaker watches say the cost is unknown.
  - `lagotto list` has a new `EXPOSURE` column, e.g. `≤$32.77/hr`,
    `30m@$32.77/hr`, `≤$5.00/hr cap`, `none` or `unknown`. A `*` marks a rate
    from the static price list, and a note under the table says when that
    list was collected and why it was used. `-o json` on `list`, `status` and
    `watch` carries an `exposure` object.
  - Until-landed reminders include the same lines, and their webhook/sns JSON
    has an `exposure` key.
  - **Where the rate comes from.** The `--max-price` cap when you set one,
    labelled as your cap and not a quote. Otherwise the on-demand rate:
    - the CLI takes it from the static spore.host price list and says so,
      with the date it was collected. The CLI does not call the live AWS
      Price List, so it needs no new permission;
    - the poller, for reminders, uses the live AWS Price List, which its
      existing policy already allows. If the live API fails it falls back to
      the static list, labelled the same way.

    The static list has no GPU or accelerator family newer than p4d, so in
    the CLI a watch for p5, g6e, g7e and similar types shows `unknown`. On-demand is the
    ceiling for spot, so a spot watch's "up to" is still an upper bound.
  - A wildcard pattern (e.g. `c7g.*`) or a watch on several types or regions
    shows the highest rate, labelled with the range. A wildcard is expanded
    against the static price list, so matching types the list lacks are not
    counted, and the line says so.
  - The poller's runtime policy is unchanged.

### Changed

- **CI and release builds use Go 1.27.3** (from 1.26.10) and golangci-lint
  2.15.0 (from 2.14.0; the older binary can't read Go 1.27 export data), per
  the org-wide pin move (spore-host/spore-host#609). `go.mod` still says
  `go 1.26.0`, so importers are not forced onto 1.27.

- **A quota-capped fleet skips its doomed re-probes when it can read the
  quota** (#158). At cap time the poller now records the family vCPU limit it
  read in the cap region (new `quota_cap_limit` / `quota_cap_region` watch
  attributes). On each later capped cycle it reads the quota in every region
  of the watch:
  - limit raised in the cap region: tops up on this poll, instead of up to
    30 minutes later;
  - every region read, none with room for another worker, limit not raised:
    no launch attempt. Only a slow 6-hour backstop re-probe is made, because
    the usage half of the reading is computed from `DescribeInstances` and
    could disagree with EC2. That is about 8 doomed `RunInstances` over a 48h
    watch, down from about 96;
  - room in some region (other usage released capacity), or a quota that
    can't be read (no permission, throttled, more than 3 regions): unchanged
    #153 behaviour, one attempt per 30 minutes. Unknown is never treated as
    full.
  It uses the poller's existing `servicequotas:GetServiceQuota` and
  `ec2:DescribeInstances` grants, so the runtime policy is unchanged. The
  quota-cap notification now names both intervals.

- `lagotto watch` refuses a negative `--max-price` (0 still means any price).
  It used to store it, and the poller treated it as no limit. The check is
  shared with `lagotto update` (#257).

- **`--ttl` must be greater than zero**, for both `lagotto watch` and
  `lagotto extend` (#239). `--ttl 0` and negative values used to be accepted
  and expired the watch on the next poll; use `lagotto cancel` to stop a
  watch, or `--until-landed` for one with no TTL.
- **`lagotto extend` on an `--until-landed` watch acts as `lagotto ack`**: it
  moves the acknowledgement deadline and resumes a paused watch. Passing
  `--ttl` to it is refused, since such a watch has no TTL.
- The `EXPIRES` column of `lagotto list` is 5 characters wider, to fit the
  until-landed form.
- **`lagotto watch` checks default-subnet coverage for spawn and hold
  watches** (#256). The poller launches into the default VPC with no explicit
  subnet, so it can only land in an AZ that has a default subnet; since #150
  it skipped the others without saying so. For each region the watch names,
  `watch` now compares the AZs that offer the type
  (`DescribeInstanceTypeOfferings`, narrowed by `--azs`) with the AZs that
  have a default subnet:
  - none in common: the watch is refused, naming the AZs and the
    `aws ec2 create-default-subnet --region ... --availability-zone ...`
    command. (A watch with no `--regions` is refused only if no enabled
    region can serve it.)
  - some offered AZs lack one: a warning per AZ, e.g. "us-west-2d offers
    p5.4xlarge but has no default subnet; this watch can't land there".
  - a lookup that fails (e.g. missing permission) is a warning, never a
    refusal. Notify and SageMaker watches are not checked.
  - `lagotto watch` therefore calls `ec2:DescribeInstanceTypeOfferings`,
    `ec2:DescribeSubnets` and `ec2:DescribeRegions` with your credentials
    (see `docs/reference/cli-iam-actions.md`). The poller's policy is
    unchanged.
- **`lagotto doctor` has a `subnet-coverage` check** (#256): a warning per
  active spawn/hold watch whose regions have AZs that offer its type but have
  no default subnet, with the commands that fix them. It needs the same three
  EC2 read actions.

- **A failed attempt that skipped AZs with no default subnet says so** (#256).
  Its `last_error` now starts `no-subnet: <azs>; ` ahead of the usual
  `<class>: <code>: <message>`, so `lagotto list` shows `no-subnet` instead of
  a plain `capacity` miss. The retry decision is unchanged.
- **EC2's capacity hint survives the 256-byte `last_error` cut** (#256). The
  AZs an `InsufficientInstanceCapacity` message says do have capacity come
  last in the message and were cut off; they are now moved ahead of it, as
  `EC2 suggests us-west-2a, us-west-2b, us-west-2d; ...`.

### Fixed

- **`lagotto cancel sl-<id>` cancels a scheduled launch** (#279). There was no
  lagotto way to cancel one: `cancel` only knew watches (`watch sl-… not
  found`), and `launch` told you to run `aws scheduler delete-schedule`, which
  the `lagotto-cli-user` policy (#232) does not grant and which left the
  `lagotto-scheduled-launches` record pending, so the poller stayed armed
  until the record's TTL.
  - `cancel` dispatches on the `sl-` prefix; `cancel w-…` is unchanged. A new
    `--region` flag reaches a launch made with `launch --region`.
  - It marks the record `cancelled` (status and `updated_at`, as the poller
    records a fired or failed launch; the TTL is kept), then deletes the
    launch's schedule and every `-r<n>` boundary-retry schedule (#62), listed
    by the launch's own prefix. A schedule already gone is not an error, and
    each one is reported.
  - A one-shot that already fired (launched or failed) is refused, and a
    launched one names its instance and `spawn terminate`. A `--cron` launch
    is cancellable at any time. A launch that fires while `cancel` runs is
    not overwritten. Rerunning `cancel` on a cancelled launch deletes any
    schedule a failed deletion left behind.
  - A cancelled launch no longer counts as pending, so the poller can disable
    itself. The poller does not fire a cancelled launch, and its boundary
    retry no longer writes a cancelled record back to `retrying` and re-arms
    it.
  - `launch` prints `Cancel with: lagotto cancel sl-<id> --region <region>`.
  - `lagotto-cli-user` gains `scheduler:DeleteSchedule` on
    `schedule/default/lagotto-launch-sl-*` (the `LaunchSchedules`
    statement), through the inventory gate; `doctor`'s caller check simulates
    it. Rerun `lagotto setup` to update the policy.

- **A fleet whose quota refuses its first worker now backs off** (#274). A
  quota cap was recorded as the number of workers running when the quota
  refused the next launch, and 0 meant "not capped". So a `--maintain` fleet
  whose family quota couldn't fit even one worker was never treated as
  capped: it retried the refused launch every poll cycle (about 576
  RunInstances calls over a 48 h watch) instead of waiting for the #153
  re-probe or the #158 backstop, was never notified, and `lagotto status`
  showed no cap. A cap now also stores a `quota_capped` marker, so a cap at
  0 workers backs off, notifies once and shows as "capped at 0/N". Records
  written by an older poller have no marker and read exactly as before.
- **`lagotto watch` and `lagotto update` refuse a `--maintain` fleet with no
  `--regions`** (#275). A fleet counts its running workers in each region it
  names, so a fleet with no regions could never be counted: it was stored as
  active, logged a warning on every poll and never launched a worker. Both
  commands now refuse it, before any AWS call for `watch`, with an error that
  points at `--regions`. A fleet stored without regions by an older CLI now
  reports the problem, and the `lagotto update <id> --regions …` that fixes
  it, in its last search error (`lagotto status`) on every poll, and its LAST
  POLL goes stale instead of looking healthy. `--require-quota` behaves as
  before for single-shot watches; a fleet never reaches it without regions.

- **`lagotto deploy --teardown` now deletes the artifact bucket** (#264).
  `lagotto deploy` creates `lagotto-lambda-<account>-<region>` for the poller
  zip, but no command deleted it, so every region ever deployed kept an S3
  bucket. Teardown now empties it of every object version and delete marker,
  page by page, and deletes it, after the poller. Only when lagotto manages
  it: the name is the default, this account owns it (every S3 call carries
  `ExpectedBucketOwner`) and it carries the `lagotto:managed=cli` tag. An
  untagged bucket is retained and reported; a `--lambda-bucket` you supplied
  is never tagged or deleted.
  - `lagotto deploy` tags the bucket when it creates it, and adds the tag
    (keeping any existing tags) to a default-named bucket this account
    already owns, so a bucket from an older deploy becomes deletable after
    the next `deploy`. A failed back-fill is reported in the deploy output and
    does not fail the deploy.
  - `lagotto doctor` adds an `artifact-buckets` check: it lists every
    `lagotto-lambda-<account>-*` bucket and warns (never fails) about each in
    a region with no poller, with the command that deletes it.
  - New CLI permissions, in `docs/reference/cli-iam-actions.md`:
    `lagotto deploy` needs `s3:GetBucketTagging`, `s3:PutBucketTagging`,
    `s3:ListBucketVersions`, `s3:DeleteObject`, `s3:DeleteObjectVersion` and
    `s3:DeleteBucket`; `lagotto doctor` needs `s3:ListAllMyBuckets` and
    `s3:GetBucketTagging`. The inventory now counts `s3:DeleteObjectVersion`
    for a `DeleteObjects` call that names versions. The poller's runtime
    policy is unchanged.

- **`lagotto teardown` now deletes the `lagotto-scheduled-launches` table**
  (#250). `lagotto deploy` and the first `lagotto launch` create it, but
  teardown deleted only the watches and match-history tables, so the scheduled
  table was left behind, while `deploy --teardown` said `lagotto teardown`
  owned it. Teardown deletes the scheduled table only when it carries the
  `lagotto:managed=cli` tag, which every lagotto that created it has set; an
  untagged table of that name is left in place and reported. A missing table
  is listed as already absent, not an error. When tables still hold records,
  the refusal without `--force` now names them, including a pending scheduled
  launch. The `deploy --teardown` prompt and summary name all three tables.
  `lagotto teardown` now also needs `dynamodb:DescribeTable` and
  `dynamodb:ListTagsOfResource` (`docs/reference/cli-iam-actions.md`).
- **`lagotto teardown` now deletes the per-launch `lagotto-launch-sl-*`
  EventBridge Scheduler schedules** (#260). `lagotto launch` creates one per
  scheduled launch, and nothing deleted them: with `--force`, teardown deleted
  the scheduled-launches table, the only record of them, and left them armed.
  A one-shot fired once into a torn-down poller and removed itself; a cron
  schedule kept firing indefinitely, and would launch again if the poller was
  redeployed. Teardown now deletes, before any table, every schedule a
  scheduled-launch record names and every `lagotto-launch-sl-*` schedule in the
  default group (which also catches the poller's boundary-retry schedules and
  any whose record is already gone). Each is reported as deleted or already
  absent, alongside the tables. If the schedules cannot be listed or deleted,
  teardown stops before deleting any table. Without `--force` a pending launch
  still blocks teardown. `lagotto deploy --teardown` still retains them, since
  they belong to the scheduled launches in the tables it retains; its prompt
  now says `lagotto teardown` deletes both. `lagotto doctor` warns about a
  per-launch schedule that no scheduled-launch record names. `lagotto
  teardown` now also needs `scheduler:ListSchedules` and
  `scheduler:DeleteSchedule`, and `lagotto doctor` needs
  `scheduler:ListSchedules` and `dynamodb:Scan`
  (`docs/reference/cli-iam-actions.md`). The poller's runtime policy is
  unchanged. Re-running teardown while its table deletes are still in
  progress now reports those tables as already absent instead of failing
  with `ResourceInUseException`.

- **The poller notices a default subnet you create** (#256). Its per-region
  default-subnet lookup was cached for the life of a warm Lambda container,
  so after `aws ec2 create-default-subnet` it kept skipping the new AZ until a
  cold start. The lookup now expires after 15 minutes, and is re-read on the
  next cycle after any failed attempt that skipped an AZ. Takes effect on the
  hosted poller once redeployed with this release (`lagotto deploy`).

### Documentation

- README and DEPLOYMENT.md describe what v0.68.0 adds (#239): how long a watch
  lives (`--ttl`, or `--until-landed` with `lagotto ack`, reminders and the
  `unacknowledged` pause), `lagotto update`, cost exposure, the default-subnet
  coverage check, which features need the poller redeployed, the Go API for
  programs that create watches (`watcher.NewWatch`, `pkg/arm`,
  `pkg/testutil/hermetic`), and the full teardown order, including the
  scheduled-launches table, the per-launch schedules and the artifact bucket.
  The README also says that a `notify` watch fires when a type is offered,
  which does not mean a launch would succeed.

## [0.67.0] - 2026-11-06

### Added

- **A heartbeat on every watch** (#239, phase 1), so a long-running hunt is
  visibly alive and you can see why it hasn't landed:
  - New fields, all optional: `attempts` (launch/hold/notify attempts made),
    `last_attempt_at`, `last_error` (the failure class, AWS code and message,
    e.g. `capacity: InsufficientInstanceCapacity: ...`, cut at 256 bytes),
    `last_error_at`, and `last_poll_error` / `last_poll_error_at`, stamped when
    the capacity search itself fails. A quota refusal (#238) counts as an
    attempt, with the quota code as its `last_error`. A cycle whose search
    fails makes no attempt and leaves `last_polled_at` alone.
  - `lagotto list` has three new columns: `LAST POLL` (the age of the last
    poll; an active watch is flagged `!` past 15 minutes), `TRIES` and
    `LAST ERR` (the error class only). `lagotto list -o wide` adds the full
    latest error, and `lagotto status` shows all of them with their times.
    `-o json` carries every field.
  - No extra DynamoDB writes: the fields ride on the write each poll cycle
    already makes. The only new write is on a cycle whose search failed,
    which used to write nothing and now writes `last_poll_error`.
  - Old watch records read fine (the fields show as `0` / `-`), and a poller
    from before this release ignores them. The hosted poller Lambda only
    starts filling them in once it is redeployed with this release
    (`lagotto deploy`).

- **A spawn or hold watch now matches only once its launch lands** (#239,
  phase 2). RunInstances returning used to end the hunt, although the instance
  was still `pending` and EC2 could still terminate it
  (`Server.InsufficientInstanceCapacity`), or a fail-closed bootstrap shut it
  down; a hold matched as soon as the reservation was created, whatever its state.
  - **Launch.** The watch stays `active` and records `pending_instance_id`,
    `pending_region`, `pending_since` and `pending_match`
    (`pending_reservation_id` for a hold). It launches nothing more while they
    are set.
  - **Each cycle** the poller describes the instance (DescribeInstances) or the
    reservation (DescribeCapacityReservations):
    - `running` (`active` for a hold) is the match: the notification goes out,
      the watch becomes `matched`, and history records the instance's real AZ,
      `landed_at` and `instance_state` (`reservation_state`).
    - `terminated` / `shutting-down` (`failed` for a hold) clears the pending
      fields and resumes the hunt in the same cycle. `last_error` gets the state
      reason, and history gets a `landing_failed` row. A capacity reason is
      retried without a cap, like a refused launch; any other reason counts
      toward the 10-consecutive-failures stop, so a launch that dies the same
      way every time does not repeat until the watch expires.
    - `stopping` / `stopped` is not a landing either: it is what spawn's
      fail-closed bootstrap leaves when it cannot terminate itself (spawn sets
      no shutdown behaviour, so its shutdown stops the instance). The poller
      terminates it and resumes; if the terminate fails it still resumes,
      since a stopped instance cannot start again on its own, and `last_error`
      names the instance to terminate.
    - **Still `pending` after 10 minutes:** the poller terminates it through
      spawn's terminate path, which cancels a Spot request and releases a
      spawn-allocated Elastic IP, then resumes and records `landing_timed_out`.
      A hold's reservation is cancelled. If the terminate fails, the watch stays
      landing and retries; it never launches a second instance in the meantime.
    - If the watch's deadline passes while it is landing, a running instance
      still counts as the match; otherwise the instance is terminated and the
      watch expires.
  - `lagotto list` shows such a watch as `landing`, with
    `landing i-… (pending 3m)` at the end of the row. `lagotto status` adds a
    `Landing:` line with the time it is given up on. The `poll` summary line
    gains `N landing`, and its JSON a `landing` count.
  - `lagotto cancel` on a landing hold releases the pending reservation. On a
    landing spawn it says the instance was not terminated, and how to.
  - Fleet (`--maintain`) watches and scheduled launches are unchanged.
  - **Upgrading.** Old watches have no pending fields and behave as before.
    A poller from before this release ignores the pending fields, so it could
    launch again. To stop it, a landing watch also holds its lease as
    `landing:<id>`, which every poller since v0.47.0 refuses to override. That
    excludes only pollers older than v0.47.0, or run with `poll --no-lease`.
  - **IAM.** No new grants for the hosted poller: DescribeInstances,
    DescribeCapacityReservations, the tag-gated TerminateInstances and
    CancelCapacityReservation are already in its runtime policy (#242). In
    `docs/reference/cli-iam-actions.md`, `lagotto poll` adds
    `ec2:DescribeCapacityReservations` and `ec2:CancelCapacityReservation`
    for local pollers running hold watches.

- **`lagotto teardown --iam-roles` deletes lagotto's two IAM roles** (#229),
  `lagotto-capacity-poller-role` and `lagotto-capacity-poller-scheduler-invoke`,
  with their inline and attached policies. Nothing deleted them before;
  DEPLOYMENT.md said to delete them by hand. The roles are shared by the poller
  in every region, so it first looks for the poller in every region enabled in
  the account and refuses, before deleting anything, while one exists (naming
  the `lagotto deploy --teardown --region` command for each) or when a region
  cannot be checked. `--force` does not override that check.
  `docs/reference/cli-iam-actions.md`: `lagotto teardown` gains the IAM delete
  actions, `ec2:DescribeRegions` and `lambda:GetFunction`.

### Changed

- **The recorded AZ is the one EC2 placed the instance in** (#239). It used to be
  the AZ lagotto attempted, which was empty for an unpinned attempt. This applies
  to watch, fleet and `snipe` launches.

- **Built against spawn v0.161.0 (from v0.160.0)**, in the root module and in
  `lambda/capacity-poller`. What reaches instances lagotto launches:
  - **The acpid step no longer OOM-kills small instances** (spawn#1521). It
    used to run `dnf install -y acpid || yum install -y acpid`; on AL2023 `yum`
    is dnf, so on a t4g.nano the OOM killer took dnf, the same install ran
    again as `yum`, and that second run took spored and systemd-resolved with
    it. Now acpid that ships on the image is just enabled, an absent one is
    installed with exactly one package manager and never retried under the
    other name, and below 768 MiB of `MemTotal` no package manager runs at
    all. This changes the bootstrap body: lagotto-launched instances fetch
    the one spawn v0.161.0 published (sha256 `3563692d…f739`) and check it
    against the hash compiled into this build.
  - **New launches install spawn v0.161.0's spored**, which publishes an egress
    proxy's first activity after a quiet period at once (spawn#1528). This
    matters only for a launch that uses an egress proxy.
  - Nothing else in spawn v0.161.0 reaches the packages lagotto imports: the
    compute-profile change (spawn#1538) is in functions lagotto never calls,
    and the rest is spawn's own CLI, examples and Lambdas. The
    golang.org/x/net v0.62.0 bump in spawn v0.161.0 was already made here
    (below). `docs/reference/cli-iam-actions.md` is unchanged.
- **Built against spawn v0.160.0 (from v0.135.1), libs v0.51.4 (from v0.51.1)
  and truffle v0.63.0 (from v0.61.0)**, in the root module and in
  `lambda/capacity-poller`. Supersedes Dependabot's #236. What reaches
  instances lagotto launches, and the poller:
  - **The Linux bootstrap fails closed.** An instance that cannot read its
    instance ID or its tags, or has no `aws` CLI and cannot install the pinned
    one, now stops with a failed `bootstrap` completion record, so
    `on_complete` fires, instead of running on defaults until its TTL. On an
    image with no `aws` CLI (stock Ubuntu) the bootstrap installs the pinned
    AWS CLI 2.39.3 from `awscli.amazonaws.com`, checked against its sha256.
    The instance fetches the bootstrap body spawn v0.160.0 published and checks
    it against the sha256 compiled into this build; lagotto pins neither.
  - **New launches install spawn v0.160.0's spored**, because the installed
    spored follows the spawn module lagotto is built with.
  - **Some launches are refused before RunInstances instead of by EC2:** user
    data over EC2's 16 KB limit, or an instance that would carry more than 50
    tags.
  - **`--account` / `SPORE_ACCOUNT` / `account =` in the shared spore config now
    guards the spawn client the poller launches with.** When one is set and the
    credentials belong to another account, the launch is refused. When none is
    set, nothing changes and no STS call is made.
  - **`docs/reference/cli-iam-actions.md` is regenerated.** `lagotto poll` gains
    Elastic IP, route-table and VPC-endpoint, and S3 staging-bucket actions that
    the new spawn code can reach in a static call graph. The PR lists which of
    them lagotto's `--spawn-config` can actually reach. The hosted poller's IAM
    policy is unchanged.

### Fixed

- **The hosted poller can now finish what it starts: an `fsx_create` watch and
  `lagotto launch --if-exists replace` work there** (#242). Its runtime policy
  had no terminate, FSx or S3 grants. An `fsx_create` watch launched its
  instance, was refused creating the S3 bucket, was refused terminating the
  instance, and failed, leaving the instance running without its filesystem
  until its TTL. `--if-exists replace` failed without launching. **Re-run
  `lagotto setup` once** (in any region; the role is account-global) to apply
  the new policy. It adds:
  - `ec2:TerminateInstances` and `ec2:CancelSpotInstanceRequests`, only on
    resources tagged `spawn:managed=true`, the tag spawn puts on every
    instance it launches. The poller cannot end an instance spawn did not
    launch.
  - `ec2:DescribeAddresses`, which spawn's terminate uses to find an Elastic
    IP to release.
  - `fsx:CreateFileSystem` and `fsx:TagResource` only for filesystems created
    with `spawn:managed=true`, `fsx:DeleteFileSystem` only on such
    filesystems, the FSx service-linked role, and the VPC reads FSx makes on
    the poller's behalf.
  - `s3:CreateBucket`, `s3:ListBucket`, `s3:GetBucketTagging` and
    `s3:PutBucketTagging`, only on buckets named `lagotto-fsx-*`.
  - `iam:TagRole` and `iam:TagInstanceProfile` on the instance roles the
    poller may already create. spawn creates them with tags, so without these
    the first launch in an account, and every watch with `iam_policy`, was
    refused at `CreateRole`. `lagotto doctor` reports a policy written before
    this as missing these grants.

  **Changed, for `fsx_create`: the backing bucket must be named
  `lagotto-fsx-<something>`.** spawn puts no naming rule on `fsx_s3_bucket`, so
  lagotto does, and the poller can then touch no bucket lagotto didn't name for
  itself. `lagotto watch` and `lagotto launch` refuse any other name with the
  reason. The poller refuses a watch or scheduled launch stored before this
  release the same way, before launching anything, and the watch fails as
  terminal with that reason. Your data bucket can still be anything: point
  `fsx_import_path` / `fsx_export_path` at it. A watch without `fsx_create`
  (including `fsx_id`) is unaffected.

  CI now checks the policy against the poller Lambda's own AWS calls,
  discovered from the code, as the CLI's permissions page already is. A call
  the policy does not grant fails the build unless it is listed, with the
  reason, as unreachable or deliberately denied (Elastic IP allocation for
  EFA, the no-public-IP preflight, the launch-config object, the account-name
  lookup, and table deletion under `AUTO_DELETE_TABLES`).
  `docs/reference/cli-iam-actions.md`: commands that create an FSx filesystem
  gain `fsx:TagResource`.
- **`lagotto setup` now creates the Scheduler invoke role too** (#229). It
  created only the poller's execution role, so in an account set up for the
  CloudFormation path no lagotto command short of `lagotto deploy` created
  `lagotto-capacity-poller-scheduler-invoke`, the role the stack's schedule
  assumes to invoke the poller. Without it no watch was ever polled, and the
  documented workaround was to deploy a poller and tear it down again. Now
  `setup` get-or-creates both roles, writes both policies, and prints
  `Created IAM role <name>` for each role it created. It is idempotent, and
  because both roles are account-global one run in any region serves every
  region. DEPLOYMENT.md's CloudFormation step 1 is just `lagotto setup` again,
  and `lagotto doctor`'s `iam-roles` check now prescribes `lagotto setup`
  rather than a deploy.
- **`--notify email:` now reaches the address it names, and only that address
  (#240).** Before, an email channel published to the shared
  `lagotto-capacity-alerts` topic with no recipient: with no subscriber (the
  default after `lagotto deploy`) nothing was delivered and nothing said so,
  and an address someone had subscribed by hand received every user's email
  events. Now:
  - **`lagotto watch` subscribes the address** to the alerts topic in this
    account and region, with a filter policy on the `lagotto_email` message
    attribute that matches only that address. An address already subscribed is
    reused, never subscribed twice. AWS sends a confirmation email; `watch`
    says to check your inbox, and nothing is delivered until the link is
    clicked. With no alerts topic, or when the address cannot be subscribed,
    `watch` refuses and names `lagotto deploy`, instead of arming a watch whose
    emails reach no one. Addresses are stored lower-cased.
  - **Every email publish carries `lagotto_email=<address>`** (match, expiry,
    quota cap and retirement), so each subscriber receives only the events of
    watches that name it. A publish without the attribute reaches no filtered
    subscription.
  - **`lagotto list` and `lagotto status` show each email channel's state**:
    confirmed, pending confirmation, or not subscribed (also in `-o json`, as
    `notify_channels[].subscription`). `lagotto extend` re-subscribes the
    addresses of a watch it reactivates.
  - **`lagotto doctor` has an `email-subscriptions` check**: addresses on
    active watches that are pending confirmation or not subscribed, unused
    subscriptions, and any email subscription with no filter policy (it
    receives every user's email events). All are warnings.
  - **Subscriptions are removed when no active watch names the address.** The
    poller does it at the start of each sweep, so a matched, expired, failed
    or completed watch's address goes on the next sweep, after its last email.
    `lagotto cancel` does it for the cancelled watch at once, and the new
    `lagotto notify-gc` does it on demand (`--resubscribe` also re-subscribes
    active watches' addresses that have no subscription). Only subscriptions
    carrying lagotto's filter policy are removed. A subscription still pending
    confirmation cannot be unsubscribed; AWS deletes it within 3 days.
  - **Every SNS call on a topic runs in the region its ARN names**, not the
    CLI's or the poller's: an alerts topic set with `LAGOTTO_SNS_TOPIC_ARN` in
    another region, and an `sns:` channel's own topic in another region, used
    to be refused by SNS with `InvalidParameter: TopicArn`.
  - **Re-run `lagotto setup` after upgrading**: the poller's runtime policy
    gains `sns:ListSubscriptionsByTopic`, `sns:GetSubscriptionAttributes` and
    `sns:Unsubscribe` on the alerts topic and its subscriptions (never
    `sns:Subscribe`). Until then the poller logs a warning each sweep, `doctor`
    reports the missing grants, and `notify-gc` cleans up instead.
    `docs/reference/cli-iam-actions.md` lists the new CLI actions: `watch`,
    `extend` and `notify-gc` call `sns:Subscribe`; `cancel` and `notify-gc`
    call `sns:Unsubscribe`; `list`, `status` and `doctor` read subscriptions.

- **`lagotto poll` and `poll --daemon` now send notifications without
  `LAGOTTO_SNS_TOPIC_ARN`** (#241). The local poller built its notifier only
  when that undocumented variable was set. Without it, no channel fired, not
  even a webhook or your own `sns:` topic, neither of which needs lagotto's
  topic. A `--action notify` watch was consumed (`status=matched`) and nobody
  was told. Spawn and hold watches launched or reserved with no notice,
  including the hold notice that carries the reservation id. Expiry and
  quota-cap notices were dropped the same way. Now:
  - **Webhook and `sns:` channels are always delivered.**
  - **`email:` channels go to `lagotto-capacity-alerts`**, the topic
    `lagotto deploy` creates and the hosted poller publishes to. `poll` uses
    that topic in your account and region when it exists. If you may not call
    `sns:GetTopicAttributes`, it uses the topic by name.
    `LAGOTTO_SNS_TOPIC_ARN` still overrides it, and is now documented in
    `lagotto poll --help` and the README.
  - **No topic is said out loud.** `poll` warns at start and names each
    in-scope watch whose email channel will not be delivered. Each skipped
    email is reported as a failed notification, and the watch's other
    channels are still delivered.
  - `docs/reference/cli-iam-actions.md`: `lagotto poll` gains
    `sns:GetTopicAttributes`, the check `lagotto deploy` and `lagotto doctor`
    already make.

  The hosted poller is unchanged: `lagotto deploy` always sets its topic.

- **A quota refusal no longer kills a multi-region or multi-type watch (#238).**
  When EC2 refuses a single-shot `--action spawn` or `--action hold` launch
  with `VcpuLimitExceeded`, `InstanceLimitExceeded` or
  `MaxSpotInstanceCountExceeded`, lagotto now marks only that candidate
  (instance type, region) as quota-infeasible and moves on to the next
  cheapest candidate in the same poll cycle. It moves past at most 3 refused
  candidates per watch per cycle, and the rest wait for the next cycle. Before
  this, the first refusal failed the whole watch, as on w-c66f3217 and
  w-b5c463bc, where a 96-vCPU g7.24xlarge hit a 64-vCPU quota in eu-north-1
  while us-west-2 had 768 vCPU free.
  - A refused candidate is skipped for 30 minutes (the same re-probe interval
    as the fleet quota cap from #153). After that it is tried again, so a
    granted quota increase is picked up without any action from you.
  - The watch fails only when **every** candidate has been refused. It then
    sends the quota notification once, listing each candidate with the vCPUs
    it needs and, when the poller can read Service Quotas, the quota limit and
    usage, plus the command to raise it. A watch with a single candidate still
    fails on its first refusal, as before, because there is nothing else to
    try. For spot watches, a candidate priced above `--max-price` has never
    been tried, so it keeps the watch alive.
  - `lagotto status` lists the candidates being skipped and when each will be
    re-probed. `lagotto history` records each refusal as a `quota_infeasible`
    row with its detail, instead of a generic `spawn_failed`.
  - Fleet watches (`--maintain`) are unchanged.
  - The new watch field `quota_infeasible` and history field `quota_detail`
    are optional. A poller older than this release ignores them and keeps the
    old fail-on-first-quota-refusal behaviour, so the hosted pollers need a
    redeploy before the fix takes effect.

### Security

- **golang.org/x/net v0.62.0 (from v0.60.0)** in both modules, for
  CVE-2026-78661 (HIGH, a denial of service in `x/net/html`). It pulls in
  x/crypto v0.58.0, x/sync v0.24.0, x/sys v0.49.0, x/term v0.47.0,
  x/text v0.43.0 and x/tools v0.51.0.

## [0.66.0] - 2026-11-02

### Added

- **The IAM actions each CLI command needs from your own credentials are now
  documented, generated from the code** (#206). Before, lagotto documented IAM only
  for the hosted poller. Nothing said, for example, that `lagotto cancel` of a fired
  hold needs `ec2:CancelCapacityReservation` and `ec2:DescribeCapacityReservations`,
  or that `watch` and `extend` need `scheduler:UpdateSchedule` and `iam:PassRole` on
  the invoke role to re-enable a self-disabled poller.
  [docs/reference/cli-iam-actions.md](docs/reference/cli-iam-actions.md) lists every
  action by command and by action, and has a table of what each everyday command
  needs beyond lagotto's DynamoDB tables. The page is discovered, not hand-written:
  `internal/iamcoverage` (ported from spawn) type-checks the CLI for each shipped
  OS, builds its call graph, and records every AWS SDK operation reachable from
  each command, including through spawn and truffle. CI fails when the page is
  stale (`make cli-iam-actions` regenerates it), or when the README's permissions
  summary names an action the code does not call. lagotto does not provision a
  CLI-user policy yet (#232).

### Fixed

- **`lagotto watch` and `lagotto extend` no longer create a watch that is never
  polled when you lack EventBridge Scheduler permissions** (#231). The hosted
  poller switches its schedule off when no watches are active, and arming a
  watch has to switch it back on. Both commands used to treat any `GetSchedule`
  error, AccessDenied included, as "poller not deployed", and reported an
  `UpdateSchedule` failure only under `--verbose`: the watch was stored active,
  the poller stayed off, and the watch expired without ever being polled. Now
  only a missing schedule (ResourceNotFound) means "not deployed" and proceeds
  as before. Any other `GetSchedule` error refuses the command before anything
  is stored; an `UpdateSchedule` failure rolls the write back (a new watch is
  rewritten as cancelled, a reactivated one is set back to expired) and refuses.
  An AccessDenied refusal names the missing permissions (`scheduler:GetSchedule`
  and `scheduler:UpdateSchedule` on `lagotto-capacity-poller`, and `iam:PassRole`
  on `lagotto-capacity-poller-scheduler-invoke`). Re-enabling the schedule also
  keeps its description, group and other optional fields instead of dropping
  them.
- **`lagotto deploy --migrate-from-cloudformation` can no longer delete live
  resources** (#226). It used to retain a fixed three logical IDs and run
  `UpdateStack` with lagotto's *current* template and no preview. On a legacy
  stack that also owned the account-wide IAM roles
  (`lagotto-capacity-poller-role`, `lagotto-capacity-poller-scheduler-invoke`)
  or the DynamoDB tables, that update dropped them from the template, so
  CloudFormation deleted them, breaking every region's poller. Any property
  that differed between the old and current templates was also applied to the
  live poller unreviewed. The migration now:
  - lists what the stack really owns;
  - adds only `DeletionPolicy`/`UpdateReplacePolicy: Retain` to **every**
    resource in the stack's **own** deployed template (YAML or JSON, SAM or
    not), and proves nothing else changed;
  - previews the change in a change set (all parameters `UsePreviousValue`)
    and refuses unless every change is a policy-only Modify with no
    replacement;
  - runs CloudFormation drift detection on any resource the change set will
    re-evaluate dynamically, and refuses unless its stored properties match
    the live resource;
  - confirms Retain on the processed (post-transform) template before
    executing and again before deleting;
  - verifies that every formerly owned resource, IAM roles and tables
    included, still exists unchanged.

  Every refusal leaves the stack untouched and prints the manual procedure.
  `lagotto deploy`'s and `lagotto doctor`'s legacy-stack warning now says the
  stack may own the shared IAM roles. `deploy --teardown`'s warning no longer
  calls a leftover stack an "empty shell" safe to delete; it says to check
  `list-stack-resources` first.
  The migration's administrator needs the CloudFormation change-set and drift
  actions, plus read access to the roles it verifies. They are listed under
  `lagotto deploy` in `docs/reference/cli-iam-actions.md`.
- **lagotto's unit tests can no longer reach a live AWS account** (#214). On
  2026-11-01 a test reached `runWatch`, built a real AWS config from the ambient
  `AWS_PROFILE`, and wrote a real watch record. Three layers now stop that:
  `awscfg.Load`, which every CLI command loads its AWS config through, panics
  under `go test` unless the test installs a fake with `awscfg.SetLoaderForTest`;
  every test package that can reach an AWS config loader has a `TestMain` that
  replaces the ambient credentials, profile and config with fakes and fails the
  binary on any request that would leave the machine; and `make test` and CI run
  both modules' tests under a `go test -exec` wrapper (`test/hermetic`, ported
  from spawn) that does the same from before package init. CI's test job also
  runs with no AWS config at all. A gate (`internal/hermetic`) discovers every
  function in both modules that can build an AWS config, directly or through
  spawn/truffle, and fails if one bypasses `awscfg.Load`, runs at package init,
  or is linked into a test binary without the hermetic `TestMain`. The
  capacity-poller Lambda now builds its clients from `main` instead of `init`
  (the only way its tests could be hermetic); its runtime behaviour is unchanged.
- **A SageMaker watch's email no longer says the job launched when it did not**
  (#212). The match email always read "SageMaker job launched for ..." and
  "Your SageMaker job ... has been provisioned", including when SageMaker had
  rejected the submit (`CreateTrainingJob` failed: a bad `RoleArn`, a validation
  error, a quota limit) or the job failed for a reason other than capacity, and
  the watch had stopped as failed. The only clue was the `Action Taken:` line.
  Now only a job that got capacity is reported as launched; any other final
  outcome sends "SageMaker job NOT launched ...: watch stopped", says why, and
  points at the job definition. The hosted poller sends this email, so the
  capacity-poller Lambda must be redeployed (`lagotto deploy`) to pick it up.

### Documentation

- **DEPLOYMENT.md's CloudFormation path no longer says `lagotto setup` creates
  everything the stack references** (#220). `setup` creates the watches and
  match-history tables and the poller's execution role and runtime policy, but not
  the EventBridge Scheduler invoke role `lagotto-capacity-poller-scheduler-invoke`
  or the `lagotto-scheduled-launches` table. Only `lagotto deploy` creates those.
  In an account that has never run `deploy`, the stack's schedule referenced a
  role that did not exist, so the poller was never invoked. The guide now shows
  what `setup` and `deploy` each create, and explains that the missing table is
  harmless and the missing role is not. For an account that has never run
  `deploy`, it gives a CLI-only way to create the role: one `deploy` and then a
  `deploy --teardown`, which keeps the roles. lagotto#229 tracks having `setup`
  create the role. The "What gets deployed" table now names which command
  creates each role and table.

## [0.65.1] - 2026-11-01

### Fixed

- **In an account with pollers in more than one region, deploying one region
  no longer breaks every other region's poller** (#218). The EventBridge
  Scheduler invoke role `lagotto-capacity-poller-scheduler-invoke` is IAM-global
  and shared by every region's poller schedule and every per-launch
  (`lagotto launch`) schedule, but `lagotto deploy` wrote its `InvokeLambda`
  policy for the deploying region's poller only. The last region deployed won:
  in every other region each scheduled run failed with a Scheduler target error,
  the poller was never invoked, its watches were never polled, and nothing
  reported it. `deploy` now writes the same policy from every region:
  `lambda:InvokeFunction` on `arn:aws:lambda:*:<account>:function:lagotto-capacity-poller`
  (still only lagotto's own poller, in your account). **To repair an affected
  account, run `lagotto deploy` with v0.65.1 in any one region**: it rewrites the
  shared policy for all regions. Re-deploying the others is not needed.
  `lagotto deploy --teardown` still never touches the shared IAM roles, so
  tearing down one region cannot cut off another. The runtime policy that
  `lagotto setup` writes has the same fix (#219, below), so after upgrading,
  one `deploy` plus one `setup` in any region repairs every region.
- **`lagotto doctor` catches it** (#218). The new `invoke-policy` check evaluates
  the invoke role's policy locally against this region's poller and FAILs when
  Scheduler is not allowed to invoke it. It WARNs when the policy covers this
  region only, which is what an older `deploy` wrote. Previously `iam-roles`
  passed because the role existed. A new best-effort `scheduler-errors` check
  reports EventBridge Scheduler target errors over the last 24 hours (CloudWatch
  `AWS/Scheduler` `TargetErrorCount` for the `default` schedule group, which
  also counts any other schedule in that group). It needs
  `cloudwatch:GetMetricStatistics` on your credentials; without it the check
  warns and doctor still passes.
- **`lagotto setup` in one region no longer revokes the poller's permissions in
  every other region** (#219). The poller's execution role
  `lagotto-capacity-poller-role` is IAM-global and shared by every region's
  poller, but `setup` wrote its `lagotto-runtime-policy` with the region it ran
  in. Each `setup` cut every other region's poller off from its own DynamoDB
  tables, SNS alerts topic and schedules, and `lagotto doctor` there then
  prescribed `setup`, which broke the first region again. The policy now names
  lagotto's tables, topic and schedules in any region (for example
  `arn:aws:dynamodb:*:<account>:table/lagotto-watches`). It is still scoped to
  lagotto's own resource names in your account, no action was added, and
  `--instance-role` authorizations are carried over exactly as before. **After
  deploying v0.65.1, run `lagotto setup` once, in any one region**: it rewrites
  the shared policy for every region.
- **`lagotto doctor`'s `runtime-policy` check evaluates the policy for this
  region** (#219). It FAILs when this region's tables, topic or schedules are
  not covered, and names the cause. It WARNs when they are covered for this
  region only, which is what an older `setup` wrote. Previously it reported that
  shape as missing grants plus extra grants "from a newer lagotto".

## [0.65.0] - 2026-11-01

### Added

- **The `status` table and every match notification show a fired hold's
  capacity reservation** (#203). `lagotto status` prints `Reservation: cr-… (AZ)`
  under "Last match" (it was only in `-o json`). The email body names the
  reservation, its AZ and how to launch into it. SNS and webhook payloads gain
  `reservation_id` and `reservation_availability_zone`, and the SNS payload
  also gains `availability_zone`. The hosted poller sends these too once its
  Lambda is rebuilt from this release.

### Changed

- **`--spawn-config` rejects unknown keys** (#202). `lagotto watch` and
  `lagotto launch` used to accept a key they didn't know and then ignore it, so
  `az:`, `s3_read:`, `completion_delay:` or a typo like `idle_timout:` looked
  like it worked and the instance launched without it. Such a key is now an
  error at watch/launch creation that names the key and the nearest accepted one
  (or, for `az:`/`s3_read:`/`instance_profile:`, where that setting actually
  lives). snake_case, kebab-case and CamelCase spellings of real keys are still
  accepted. Watches already stored are not affected: the poller re-reads a
  stored config tolerantly, so a watch created before this change still fires.

- **Go API: `watcher.Holder.Release` returns `(ReleaseOutcome, error)`**
  (#203). It used to return only an error. The outcome reports whether the call
  released the reservation or found it already ended. No other spore.host
  module calls it.

### Fixed

- **`--action hold` no longer reserves in an AZ you can't launch into** (#209).
  A hold took the first AZ truffle offered the type in, which could be an
  opted-in Local Zone such as `us-west-2-lax-1b`. A default VPC has no subnet
  there, so the printed `spawn launch --reservation-id …` failed with
  `InvalidInput` while the reservation billed On-Demand. A hold now reserves in
  the first offered AZ that has a default subnet, the same check `--action
  spawn` already makes (#150), and the match reports that AZ. If none of the
  offered AZs has one, nothing is reserved and the hold fails with a message
  saying so (retried next cycle, up to the watch's failure cap). If the subnets
  can't be listed (for example `ec2:DescribeSubnets` is denied, or the account
  has no default VPC), the hold behaves as before. The hosted poller gets this
  once its Lambda is rebuilt from this release.
- **`lagotto launch --reservation-id` now launches into the reservation**
  (#210). The flag only timed and gated the launch: unless the spawn config
  also set `reservation_id`, the instance launched On-Demand outside the
  reservation, for example outside a paid Capacity Block. lagotto now reads the
  reservation when the launch is scheduled and sets the launch's
  `reservation_id`, sets `capacity_block` for a Capacity Block, and takes the
  instance type and AZ from the reservation when the spawn config and `--az`
  leave them out. A value that could not launch into the reservation is refused
  then, naming the conflict: a different `reservation_id`, `instance_type` or
  `--az`, `spot: true`, or `capacity_block: true` on a reservation that isn't a
  Capacity Block. The flag's help said "to launch into" all along and now
  describes this. Launches already scheduled this way also launch into their
  reservation once the hosted poller's Lambda is rebuilt from this release.
- **Durations no longer truncate silently** (#201). `--ttl` on `watch` and
  `extend`, `launch --after`, and a `--spawn-config` `ttl:` read the number
  with a parser that stopped at the first non-digit, so `1.5d` meant 24h,
  `2d12h` meant 2h and `5abcd` meant 5d, with no warning. The whole value must
  now parse: fractions and compound values mean what they say (`1.5d` is 36h,
  `2d12h` is 60h, as `1.5h` and `1h30m` already were), and anything else
  (trailing text, a missing or unknown unit, a sign, overflow) is an error
  naming the value. Watches and scheduled launches already stored are not
  broken by this: a stored `ttl:` only the old parser accepted still launches,
  with the duration it was accepted as and a warning in the poller log.
- **A `--spawn-config` `ttl:` in weeks no longer fails every launch** (#201).
  lagotto accepted `ttl: 1w`, but spawn's launcher has no week unit and refused
  to launch it at match time. lagotto now hands spawn the TTL in a form it
  reads (`168h0m0s`), for new and already-stored watches alike.
- **A notify-only SageMaker watch is refused instead of never notifying**
  (#195). SageMaker has no capacity signal short of submitting a job, so
  `lagotto watch ml.* --service sagemaker` with `--action notify` (the default)
  was accepted and then sat silently until its TTL; the missing
  `--sagemaker-config` error even recommended `--action notify`. `watch` now
  refuses notify for SageMaker and names the mode that works (`--action spawn
  --sagemaker-config job.yaml`, plus `--notify` to be told when SageMaker
  provisions the job), and that error no longer recommends notify. A
  notify-only SageMaker watch already stored is retired as `failed` on its next
  poll, with one notification saying why; nothing is submitted. The hosted
  poller does this once its Lambda is rebuilt from this release.
- **The hold launch command in emails and the README now launches as
  written** (#208). It was `spawn launch --region … --az … --reservation-id …`,
  which spawn rejects: it needs a spore name and doesn't take the instance type
  from the reservation. The command is now `spawn launch <watch-id> --region …
  --az … --instance-type … --reservation-id …`. The email, the README and a new
  `Launch into it:` line in `lagotto status` (shown while the hold is live) all
  print it from one builder.

- **`lagotto cancel` can release a fired `--action hold` reservation early**
  (#203). A hold that fired leaves the watch `matched`, and `cancel` refused
  any watch that wasn't active. That left the targeted reservation billing at
  the On-Demand rate until its 30-minute window ended, unless you cancelled it
  by hand with the EC2 CLI. `cancel` now releases it; the watch stays
  `matched`. Releasing a reservation that already expired or was already
  released reports that and succeeds, so running it twice is safe. It needs
  `ec2:CancelCapacityReservation` on your own credentials; without it, cancel
  names the permission and when the reservation will expire on its own.

### Documentation

- **README: `--spawn-config` is lagotto's own schema, not `spawn launch`'s**
  (#202). The README said the file was "the same shape `spawn launch` reads";
  `spawn launch --config` accepts only a `plugins:` list. The README now lists
  every accepted key, and a test keeps that list equal to the parser's fields.
  The `--spawn-config` flag help says the same.

## [0.64.1] - 2026-10-20

### Documentation
- **DEPLOYMENT.md: verify with `lagotto doctor`, not `lagotto status`**
  (spore-host#585). `status` takes a watch ID and shows one watch; `doctor` is
  the read-only deployment check. The deploy, upgrade and CloudFormation examples
  no longer pin `--version 0.60.0`/`0.61.0`: `deploy` already defaults to the
  running binary's version, and the CloudFormation path uses a `VERSION`
  placeholder. Removed the "AWS Account Reference" section, which named
  maintainer accounts and wrongly said the poller only reads EC2 metadata, and
  the monorepo-era `cd lagotto` from the integration-test command.
- **README: the goal-driven fleet example runs as written** (spore-host#585). It
  put `# comments` after the line-continuation backslashes, which ends the
  command at the first one. The comment is now above the command.

### Security
- **Built with Go 1.26.10** for GO-2026-6809 (disclosed 2026-10-21), which
  makes `crypto/x509` distrust compromised intermediate certificates. The CI
  Go pin moves 1.26.9 → 1.26.10.

## [0.64.0] - 2026-10-17

### Security

- **Rebuilt against Go 1.26.9 and `golang.org/x/net` v0.60.0** for the Go
  vulnerabilities disclosed 2026-10-09: GO-2026-6605, 6607, 6608, 6610, 6611,
  6612, 6613 and 6617, across `net/http`, `crypto/tls`, `mime/multipart`,
  `net/http/httputil`, `net/textproto` and x/net's HTTP/2 implementation. The CI
  Go pin moves 1.26.8 → 1.26.9 in all workflows, and the nested `lambda/capacity-poller` module is re-tidied
  (it has its own go.mod with a `replace ../..`, so it pins its own copy of the
  root's dependencies and otherwise fails with "updates to go.mod needed").

### Added

- **A gate that fails on any flag nothing reads** (`cmd/inert_flags_test.go`,
  ported from truffle). It parses the `cmd` package and requires every
  registered flag's variable to be read somewhere, so a flag can't again parse,
  show in `--help` and do nothing. Unlike truffle's version it also sees flags
  registered through a local alias (`f := cmd.Flags(); f.StringVar(...)`), which
  is how `deploy` and `launch` register theirs.

### Changed

- **lagotto now builds against spawn v0.131.0, truffle v0.59.0 and libs
  v0.51.1** (from spawn v0.111.1, truffle v0.56.0 and libs v0.49.0), in the CLI
  and in the `lambda/capacity-poller` module. It picks up twenty spawn
  releases of launcher and AWS-client fixes; lagotto's own code needed no
  changes.

- **`watcher.Store.ListMatchHistory` now requires a watch ID** (#184). An
  empty ID used to Scan the whole match-history table across every user and
  return up to 100 of their records in arbitrary order. It now returns
  `watcher.ErrHistoryWatchIDRequired` without reading anything; use
  `ListMatchHistoryByUser` for a user's history. No lagotto command is affected
  (`lagotto history` always passes an ownership-checked watch ID, or uses the
  per-user query), and no other spore.host module calls it. This is a breaking
  change to the exported Go API, so the next release is a MINOR bump.

- **The inert-flag gate's "found no flags" check is now tested** (#185). If
  the gate's parser ever stopped recognising how flags are registered, it
  would find nothing and could report the package clean; it fails instead, and
  a test now proves that. Ported from truffle#215 so the two copies stay in
  step, including its fixture for an unread flag registered through a
  `PersistentFlags()` alias. Test-only; no change to the CLI.

- **`make lint` is clean and CI now runs it** (#183). The issue counted 8
  errcheck findings, but golangci-lint's default `max-same-issues=3` was hiding
  repeats: the real count was 105, almost all unchecked `fmt.Fprint*` writes to
  a command's output. Those now discard the result explicitly (`_, _ =`, as
  truffle does), and two deferred `Body.Close()` calls use the
  `defer func() { _ = x.Close() }()` form the rest of the repo already uses. No
  output changes. A new CI `Lint` job runs `make lint` with golangci-lint pinned
  to 2.14.0 and the download checked against a pinned SHA256; `make lint` now
  shows every finding (`--max-same-issues=0`), also lints the
  `lambda/capacity-poller` module, and warns when the local golangci-lint
  version differs from CI's.

### Fixed

- **CI action pins now say which version they actually run** (#189, #190).
  `actions/checkout@3d3c42e…` was commented `# v6` in every workflow but is
  v7.0.1, and `goreleaser-action` `# v7` is v7.2.3. The SHAs were right and are
  unchanged; only the comments were wrong — but those comments are what
  reviewers read and what Dependabot rewrites. A new `workflow_pins_test.go` now
  fails on any `uses:` not pinned to a full SHA with a `# vX.Y.Z` comment, and
  checks each comment against `scripts/action-pins.txt`, a table of what every
  pinned SHA really is, regenerated from GitHub by
  `scripts/refresh-action-pins.sh` (run it, and commit the table, whenever a pin
  changes — including on a Dependabot bump).
- **`lagotto list` and `lagotto history` no longer drop watches past the first
  page** (#181). DynamoDB returns a query in pages of at most 1 MB, and lagotto
  read only the first, so a user with enough watches (or match records) saw a
  silently truncated list. Every DynamoDB query and scan now follows all pages.
  That also fixes two decisions the capacity-poller Lambda makes from a scan:
  whether any scheduled launch is still pending (a pending launch past the first
  page could let the poller disable its schedule under it) and, with
  `AUTO_DELETE_TABLES=true`, whether the tables are empty before deleting them.
- **`lagotto --account <id>` now actually guards the account** (#179). The flag
  ("Expected AWS account ID (optional guard)") was parsed and ignored, so a
  command ran against whatever account your credentials belonged to. That
  includes `watch --action spawn`, which launches billable instances. Now, when
  you pass it, lagotto checks the resolved credentials with STS
  `GetCallerIdentity` before doing anything else, and refuses on a mismatch or
  if the account can't be determined. Only the explicit flag is enforced.
  `SPORE_ACCOUNT` and the shared config's `account` are not, matching truffle
  (see spore-host/truffle#206).

## [0.63.1] - 2026-10-04

### Added

- **The changelog policy is now enforced in CI rather than by habit.** A PR that changes
  non-test Go source without touching `CHANGELOG.md` fails, and `changelog_test.go`
  checks `[Unreleased]` for duplicate group headings, unknown group names, entries
  outside a group, and releases missing a compare link.
  This policy has been suite-wide for a while but only `spawn` enforced it — where it
  immediately earned its keep, catching a duplicate `### Fixed` **four times in one
  session** and a PR that had merged with no entry at all (found only at the next
  release, against an empty `[Unreleased]`, with the entries reconstructed from the diff
  at tag time).
  `scripts/changelog-consolidate.py` (and `make changelog-fix` where there's a Makefile)
  merges duplicate groups mechanically, because two PRs each adding their own
  `### Fixed` is a routine conflict that merges cleanly for git and badly for the format
  — not a mistake worth hand-fixing each time.

### Fixed

- **The capacity-poller Lambda is now built as a package, not a file list** (#172). The
  GoReleaser `before` hook ran `go build ... main.go`, which compiles **only** the named
  files. The module is single-file today, which is the only reason that worked.
  The loud failure is a second source file that `main.go` references: the build breaks,
  which at least stops the release — spawn's reaper hit exactly that. The failure worth
  gating is the quiet one: a new file `main.go` does *not* reference (a `func init()`
  registration, a side-effect import) compiles fine and produces a binary **silently
  missing that code**. The release succeeds, the asset publishes, and `lagotto deploy`
  installs a poller that is subtly wrong.
  Fixed in both places it appeared — the GoReleaser hook **and**
  `lambda/capacity-poller/Makefile`, which had the same bug and would have kept a local
  build diverging from the released one. A test now scans every Makefile, YAML and shell
  script for a `go build` naming `.go` files, so this can't come back.

## [0.63.0] - 2026-10-01

### Added
- **`lagotto setup --instance-role NAME`** authorizes the hosted poller to launch
  with an IAM instance role your watch names in `spawn_config.iam_role` (#170).
  Repeatable; `--revoke-instance-role NAME` removes one. Previously there was no
  way to do this at all: the poller's policy covers only `spawn-instance*` and
  `spored*`, so a watch naming any other role waited for capacity, **matched**, and
  then died at launch with `AccessDenied: iam:GetRole`. Three real `g6.8xlarge`
  watches were lost that way after matching in under a minute.
  **What you are granting:** the poller may read that role, write its inline
  policy, attach managed policies to it, and pass it to EC2 — spawn adds its
  spored/SSM baseline to every role it launches with. That is why roles must be
  named explicitly; a wildcard is refused.
- **`lagotto watch` refuses up front** when a watch names an `iam_role` the hosted
  poller cannot use, naming the missing permissions and the exact `lagotto setup`
  command to fix it (#170). It refuses only on evidence from the deployed policy:
  with no hosted poller it stays silent (a local `poll --daemon` runs under your own
  credentials), and if the policy can't be read it warns rather than blocking. The
  verdict is in `-o json` as `instance_role_check`.

### Fixed
- **`lagotto setup` no longer silently revokes instance-role authorizations.** The
  runtime policy is written with a wholesale `PutRolePolicy`, so a plain re-run used
  to drop anything the running binary didn't know about. It now carries existing
  authorizations forward, and `lagotto doctor` no longer reports them as drift.
- **`lagotto doctor` no longer claims a missing grant kills your watches when it
  doesn't** (#170). It asserted, for *every* missing grant, that "the poller fails
  these calls with AccessDenied, which classifies TERMINAL — the watch dies". That
  is false for the `servicequotas:*` reads, whose lookup **fails open** and only
  enriches quota-cap reports with real vCPU numbers. #170's reporter had exactly
  that gap, believed the stated consequence, and spent a day investigating their
  spawn config while an unrelated `iam:GetRole` denial was actually killing their
  watches. Missing grants are now reported as **required** or **reporting-only**,
  with the real consequence of each, and a gap that is reporting-only is a WARN
  rather than a FAIL.
- **Errors are no longer printed twice.** Cobra printed `Error: <err>` and lagotto
  printed the error again. Unnoticeable for a one-line message; it made longer,
  actionable messages read as noise.

## [0.62.0] - 2026-09-25

### Fixed
- **Extending a watch no longer causes its record to be hard-deleted the moment it
  expires** (#161). `lagotto extend` set the record's DynamoDB TTL to the watch's
  *new expiry*, so DynamoDB deleted the item at the exact instant the watch ran
  out — destroying the terminal `status=expired` marker lagotto writes on that same
  transition. The record's TTL is a 90-day **retention** horizon, deliberately not
  the watch's own TTL, and creating a watch made the same mistake, which is why an
  expired watch vanished even if you never ran `extend`. An expired watch now
  leaves a `status=expired` tombstone, so you can tell "expired without ever
  acquiring" from "that watch ID never existed" — and `lagotto status <id>` answers
  instead of printing nothing. Watches created before this fix are repaired in
  place when they expire, so you don't have to re-arm them to get a tombstone.
- The terminal status transitions (`expired`, `cancelled`, `completed`, `failed`)
  now all push the record's retention horizon out, so no terminal outcome can be
  deleted out from under itself (#161).

### Added
- **Expired watches are now surfaced instead of disappearing** (#161). `lagotto
  history` gained a "Gave up without acquiring" section listing every watch that
  ended without ever getting capacity, with the elapsed watch duration reported as
  *waited* — the other half of the wait measurement added in #139, which until now
  only reported successful acquisitions. "We watched g7e for 48h across 5 regions
  and never got it" is arguably the most useful thing a capacity watcher can tell
  you, and it used to be silently dropped. `lagotto list --all` shows them too; it
  needed no new flag, since `--all` already meant "every status" — there was simply
  no record left to find. In `-o json`, each history row now carries an `event` key
  (`match` or `gave_up`); match rows are otherwise unchanged, and a gave-up row is
  deliberately its own shape so nothing can mistake it for an acquisition.
- **A watch that expires without acquiring now notifies you**, on the same
  `--notify` channels as a match (#161) — the mirror image of "capacity found", and
  the moment you most want to hear from an unattended hunt, especially with
  `--action spawn` where nothing was ever launched. The message says what was
  hunted, for how long, that nothing was launched, and how to re-arm with a longer
  TTL. Sent once, at the expiry transition. Webhook and raw-SNS channels get an
  `"event": "expired"` payload with `"acquired": false`; the existing match payload
  is untouched.

## [0.61.0] - 2026-09-25

### Added
- **`lagotto doctor`** (#156) — a read-only health check that tells you whether the
  hosted poller in your account still matches the lagotto you're running. The
  headline is the poller's runtime IAM policy: `lagotto setup` replaces it
  *wholesale* with the permissions compiled into whichever binary ran it, so
  upgrading lagotto and forgetting to re-run `setup` silently leaves the poller on
  the old permissions — and running an *older* lagotto's `setup` reverts newer
  ones. Neither shows up anywhere until a watch dies, and for the hosted poller
  that failure is only visible in CloudWatch Logs. That is what made #149, #151
  and #153 fail silently, and `doctor` now reports it as drift, naming the exact
  actions and resources that are missing (or extra) instead of dumping a diff of
  two JSON documents. It also reports a poller Lambda older than your CLI, a
  missing table/role/Lambda/topic/schedule, a schedule that is switched off while
  watches are active — the case where your watches look armed but nothing is
  polling them — and a leftover CloudFormation stack. Every finding comes with the
  command that fixes it. It exits non-zero only on failures (warnings don't count),
  so you can run it in CI, and `-o json` gives the machine-readable form. Nothing
  about it writes: there is deliberately no `--fix`.
- `lagotto deploy` now stamps the poller Lambda with a `Version` tag recording the
  release its code came from, which is what makes "is my deployed poller stale?"
  answerable. A poller deployed by an earlier lagotto has no such tag; `doctor`
  reports its version as unknown rather than guessing.

## [0.60.0] - 2026-09-19

### Added
- **`lagotto deploy --migrate-from-cloudformation`** (#154) detaches an existing
  CloudFormation stack from your live poller and then deletes it, leaving the
  poller, topic and schedule running. You do not need it for things to work — the
  resources all have fixed names, so `lagotto deploy` adopts whatever a previous
  stack created, and nothing is duplicated or recreated. You need it to be rid of
  the stack *safely*, because the stack still thinks it owns those resources and a
  plain `aws cloudformation delete-stack` would take your poller down with it.
  `deploy` warns you whenever it sees such a stack. The migration puts
  `DeletionPolicy: Retain` on the three resources, **reads the deployed template
  back to confirm the policy actually landed**, and only then deletes the stack —
  if the verification fails it aborts without deleting anything and prints the
  manual commands.

### Changed
- **`lagotto deploy` provisions the poller with direct AWS API calls instead of
  CloudFormation** (#154). Every deploy outage lagotto has had was
  CloudFormation-specific — a circular dependency between the function, its role
  and its schedule (#67/#68), an `AlreadyExists` collision between `watch` and
  `deploy` (#59), a missing-capability refusal that wedged the stack in
  `ROLLBACK_COMPLETE` (#143), and a post-transform validation failure that took
  change-set bisection to even read (#145). Two of those four were fixed by taking
  resources *out* of CloudFormation; by the end the stack held three. So now the
  SNS topic, the poller Lambda and its schedule are each created or converged
  directly. What you get: re-running `deploy` is genuinely incremental and can't
  strand anything, a `--version` bump is a fast code-only update (no more
  `aws lambda update-function-code` by hand), and a failure reports the actual API
  error at the call that failed instead of an aggregate "Validation failed with 2
  error(s)". The printed output keys are unchanged. The CloudFormation template is
  still shipped as an optional declarative path for IaC/enterprise use — see
  DEPLOYMENT.md.
- **`lagotto deploy --teardown` deletes the three resources explicitly** (#154), in
  the order schedule → function → topic so nothing can fire into a half-deleted
  poller, and is safe to re-run. The confirmation prompt now spells out everything
  that is *retained*: your DynamoDB tables (as before), **both IAM roles** — they're
  CLI-owned and the Scheduler invoke role is shared with per-launch scheduled
  launches — and **the artifact bucket**, which was never stack-owned so no previous
  teardown removed it either. Two of those three were previously unmentioned, and
  the docs claimed a stack delete removed the IAM roles. It never did.
- **`lagotto deploy --stack-name` now means "the legacy stack"** (#154). `deploy`
  doesn't create a stack any more, so the flag only names one to detect and
  optionally migrate away from. The poller's own resources have fixed names.
- **The topic-only deploy mode is gone from the CLI** (#154). Deploying the SNS
  topic without the Lambda was reachable only by setting an empty
  `LambdaCodeBucket`, which the CLI never did. The template still supports it for
  anyone using the raw CloudFormation path.
- **`lagotto launch` no longer needs a CloudFormation stack to find the poller**
  (#154). It used to read the poller Lambda's ARN and the scheduler role's ARN out
  of the stack's outputs; now it builds both from your account and region — those
  resources have fixed names, and the CloudFormation template constructed the very
  same ARNs itself — and then confirms the poller is actually there with a direct
  Lambda lookup. Two practical differences: you get a clear *"the hosted poller
  isn't deployed in account … / … — run 'lagotto deploy' first"* instead of a
  stack-outputs error, and a poller that was deleted out from under its stack is
  now caught (the old check was satisfied by the stack still reporting outputs,
  even when the function was gone). A permissions failure on that lookup is
  reported as a permissions failure, not as a missing deployment. This works
  whether the poller was created by `lagotto deploy` or by the CloudFormation
  template, so existing deployments keep working untouched.

### Deprecated
- **`lagotto launch --stack-name` is ignored** (#154). The poller has a fixed name,
  so there is no stack to point at. The flag still parses (scripts that pass it
  won't break) and stays visible in `--help`, now labelled as ignored, and `launch`
  prints a one-line note when you use it. `lagotto deploy --stack-name` is
  rescoped rather than deprecated (see Changed).

### Fixed
- **`lagotto deploy` can no longer switch off a running poller** (#154). The
  schedule's on/off state belongs to `lagotto watch` (which enables it) and to the
  poller itself (which disables it when it goes idle) — but the CloudFormation
  template declared `State: DISABLED`, so it was permanently out of step with
  reality and any deploy that reasserted it would have stopped an active watch's
  polling. The new path sets `DISABLED` only when *creating* the schedule, and
  passes the current state through untouched on every update.
- **The `aws cloudformation deploy` command in DEPLOYMENT.md failed as documented**
  (#155). It passed `--capabilities CAPABILITY_IAM`; since #146 the template needs
  `CAPABILITY_AUTO_EXPAND` and no IAM capability at all. Fixed, and a test now
  fails if it drifts again.

### Documentation
- **DEPLOYMENT.md is rewritten around `lagotto deploy`** (#155), with
  CloudFormation as a clearly-labelled alternate path for IaC/enterprise use. Gone:
  the claim that the stack creates the poller's execution role and that a stack
  delete removes the IAM roles (both untrue since #146), the manual
  `aws lambda update-function-code` recipe (use `lagotto deploy --version X`), and
  `deployment/cloudformation/template.go`'s reference to a `sam deploy` path that
  never existed. Added: what each resource is and who owns it, exactly what
  teardown retains, how to migrate off a stack, and a warning that `lagotto setup`
  **replaces** `lagotto-runtime-policy` wholesale — so re-run `setup` after
  upgrading, and never run an older lagotto's `setup` against a
  newer-deployed poller.

## [0.59.0] - 2026-09-18

### Fixed
- **A `--maintain` fleet that exceeds your EC2 vCPU quota now tells you, instead
  of silently re-failing a launch every poll for the whole TTL** (#153). Ask for
  `--maintain 5` on a `g6e.*` pattern when your G-family quota only fits 2 and you
  got 2 instances, one wasted failed `RunInstances` per poll cycle, and **no
  user-visible signal at all** — the evidence was buried in the poller's
  CloudWatch logs. Now the ceiling is reported once: a notification (when the
  watch has `--notify`) and a `Quota cap: fleet capped at 2/5 since <time>` line
  in `lagotto status`, with the family, the numbers, and the
  `aws service-quotas request-service-quota-increase` command to raise it. The
  watch deliberately **stays active and keeps its running workers** (failing it
  would throw away healthy instances, and a quota can be raised mid-watch), makes
  at most one top-up attempt per 30 minutes while capped, still replaces a worker
  that dies *below* the ceiling on the very next cycle, still retires on `--until`,
  and **picks up a granted quota increase automatically** with no user action.
  `lagotto poll` gained a `quota-capped` count in its cycle summary.

### Added
- **A create-time feasibility warning for fleet watches** (#153): `lagotto watch
  --maintain 5` now checks the family's vCPU quota up front and warns (to stderr,
  so it survives `-o json`) when the goal doesn't fit — "this fleet may not reach
  --maintain 5 … fits at best 2 × g6e.xlarge (4 vCPU each)", or the louder "your
  quota is 0 — this fleet cannot launch a single worker" — together with the
  quota-increase command. It is **non-fatal**: the watch is still created, and a
  quota that can't be read produces no warning at all (a missing read permission
  must never look like a zero quota).
- **Read-only `servicequotas:GetServiceQuota` / `ListServiceQuotas` in the hosted
  poller's runtime policy**, so a quota-cap report can carry your account's actual
  limit and usage. Re-run `lagotto setup` (or `lagotto deploy`) to get exact quota
  numbers in the report — **the report itself works without it**: the ceiling is
  detected from the `RunInstances` error, and the quota lookup is optional and
  fails open.

## [0.58.3] - 2026-09-17

### Fixed
- **The launcher no longer wastes an attempt on an availability zone the account
  can't launch in** (#150). When a watch spans AZs, the AZ sweep dropped into any
  AZ that offered the instance type — including ones with **no default subnet**
  (e.g. `us-west-2d` in many accounts), which fail `RunInstances` with
  `InvalidInput: No default subnet for availability zone`. The sweep now filters
  to AZs that have a default subnet (via `ec2:DescribeSubnets`, already in the
  poller's policy; memoized per region) before trying them, so a launch skips
  un-launchable AZs instead of burning an attempt and logging a confusing error.
  Fails open — if the launchable set can't be determined, all AZs are tried as
  before.

## [0.58.2] - 2026-09-17

### Fixed
- **Hosted-poller auto-spawn can now enforce `--cost-limit`, so cost-capped GPU
  watches reach a real capacity check instead of dying** (#148, second layer).
  With the #149 instance-profile fix in place, watches still went terminal
  because the poller's runtime role lacked **`pricing:GetProducts`**: spawn looks
  up the on-demand price to enforce `--cost-limit` before launching, and for
  newer GPU types (g7/g7e/p5) there's no static-fallback price, so the launch
  failed with `AccessDenied` — a terminal error — before any capacity check
  (observed: 11/12 GPU watches). The runtime policy now grants
  `pricing:GetProducts`/`GetAttributeValues`, so a genuine capacity miss
  classifies `FailureCapacity` and retries to TTL as intended. **Re-run `lagotto
  setup` (or `lagotto deploy`) after upgrading to apply it to an existing poller.**

## [0.58.1] - 2026-09-17

### Fixed
- **Hosted-poller auto-spawn no longer dies instantly on every watch** (#148,
  the retry-to-TTL half of #140). The poller's runtime IAM policy granted the
  instance-profile setup actions (`iam:GetRole`/`CreateRole`/…/`PassRole`) only on
  `spored*`, but spawn's launcher names the per-launch role/profile it creates
  `spawn-instance-<hash>`. So every hosted `--action spawn` failed at "set up IAM
  instance profile" with `AccessDenied` — which is (correctly) a *terminal* error,
  so the watch went `failed` in ~46s and never retried, defeating the wait-out-
  scarce-capacity use case (observed: 12 GPU watches all `failed` on the first
  poll despite 48h TTLs). The policy now authorizes those actions on
  `spawn-instance*` (roles + instance-profiles) as well as `spored*`, so the
  poller can provision the instance profile and a genuine capacity miss classifies
  as `FailureCapacity` and retries to TTL as intended. **After upgrading, re-run
  `lagotto setup` (or `lagotto deploy`) to re-apply the poller's runtime policy.**

### Added
- **`lagotto list` now shows each watch's project and owner, and can filter by
  them.** New `PROJECT` and `OWNER` columns surface the `--project` label and a
  short form of the creator's caller ARN (e.g. `user/alice`,
  `assumed-role/Dev/alice-session`); `--project <name>` narrows the list to one
  project and `--mine` limits it to watches you created. `lagotto status` now
  prints the watch's `Project:` and `Owner:` too. Both `list` and `status` include
  `project` and `user_id` in `-o json`. (#1)
- **Instances launched by a watch are now tagged with their origin.** Every
  `--action spawn` launch stamps `lagotto:watch-id=<watch-id>` (and
  `lagotto:project=<project>` when the watch has one) on the instance and its
  volumes, so a running box is traceable back to the watch/project that created it
  — handy for cost attribution and cleanup with a tag filter. (#1)
- **`lagotto status` and `lagotto history` now report how long capacity took to
  acquire.** For a watch that matched/spawned, they show `wait to acquire`
  (matched-at minus created-at); for a watch that gave up, `status` shows
  `time to give up` (failed-at minus created-at). Durations render human-readably
  in tables (e.g. `4m12s`) and as raw `wait_to_acquire_seconds` /
  `time_to_give_up_seconds` in `-o json`. (#139)

### Changed
- **Homebrew and Scoop packages no longer pull in `truffle` as a runtime
  dependency.** lagotto links truffle in as a compiled-in Go library and never
  runs the `truffle` CLI, so installing lagotto no longer force-installs a second
  binary you don't need.

## [0.57.2] - 2026-09-17

### Fixed
- **`lagotto deploy` no longer rolls back with a CloudFormation Early Validation
  `ResourceExistenceCheck` failure.** The stack used to create the two poller IAM
  roles (`lagotto-capacity-poller-role`, `lagotto-capacity-poller-scheduler-invoke`)
  by explicit name; if either already existed — from a prior deploy, a rollback
  that retained it, or `lagotto setup` — CloudFormation's Early Validation refused
  the create and rolled the whole stack back, blocking the hosted poller (#145).
  The roles are now **CLI-owned**: `lagotto deploy`/`setup` create them (idempotent,
  exactly as the DynamoDB tables are — #59) and the stack only **references** them
  by ARN. Deploy now succeeds whether or not the roles already exist, and no manual
  role cleanup is needed. As a result the stack defines no IAM resources, so deploy
  no longer requests `CAPABILITY_NAMED_IAM` (only `CAPABILITY_AUTO_EXPAND` for the
  SAM transform). `lagotto setup` also now works on a fresh account without a prior
  `lagotto deploy` (it creates the role instead of reporting it missing).

## [0.57.1] - 2026-09-17

### Fixed
- **`lagotto deploy` no longer fails immediately with a CloudFormation rollback.**
  The stack creates IAM roles with explicit names
  (`lagotto-capacity-poller-role`, `lagotto-capacity-poller-scheduler-invoke`),
  which CloudFormation accepts only under `CAPABILITY_NAMED_IAM` — but `deploy`
  passed only `CAPABILITY_IAM`, so every `CreateStack` was refused
  ("Requires capabilities: [CAPABILITY_NAMED_IAM]") and rolled back to
  `ROLLBACK_COMPLETE`, blocking the hosted poller entirely (#143). `deploy` now
  passes `CAPABILITY_NAMED_IAM`. A stack already stranded in `ROLLBACK_COMPLETE`
  by the old bug is cleaned up automatically on the next `deploy` (it is deleted
  and recreated), so re-running `lagotto deploy` is all that's needed.

## [0.57.0] - 2026-09-16

### Added
- `lagotto watch --action spawn --spawn-config` (and `lagotto launch
  --spawn-config`) now accept the config from a **local path, an
  `s3://bucket/key` URI, or `-` (stdin)**, not just a local file. This lets a
  watch be created from a config that lives in S3 or is piped in, so you don't
  have to stage a local file first (#140).
- Spawn configs are now made **self-contained at watch-creation**: any
  `user_data_file` / `iam_policy_file` (`user_data: "@path"` too) it references
  is read *now*, on the machine that has the files and credentials, and stored
  inline in the watch. A hosted poller (from `lagotto deploy`) — which has no
  access to the creating machine's filesystem — can then service an
  `--action spawn` watch entirely from the stored config, so unattended
  auto-spawn no longer requires a foreground daemon on the creating machine
  (#132). The IAM policy is passed to spawn via its inline-policy path; user data
  is stored as inline text. Older watches stored with a file path still launch as
  before when the poller can see the file.

### Fixed
- **A spawn watch that hits `InsufficientInstanceCapacity` now reliably keeps
  retrying to its TTL instead of giving up early** (#140). Capacity failures
  already classify as `FailureCapacity` (uncapped retry) when the AWS
  `smithy.APIError` is intact — but if a capacity error reached the classifier
  with its API-error type flattened out in wrapping (a `%v`/`%s`-formatted layer,
  an aggregation), it fell through to the capped unknown-failure bucket and the
  watch could go `failed` well before its TTL — defeating the flagship "arm it
  and wait out scarce GPU capacity" use case. `ClassifyFailure` now also matches
  the specific capacity phrasing in the raw error message as a fallback, so a
  capacity wait is honored regardless of how the error was wrapped. (The generic
  word "capacity" is deliberately not matched.)

## [0.56.0] - 2026-09-15

### Added
- `lagotto watch` now accepts a **comma-separated list** of instance-type
  sub-patterns (e.g. `"g6.8xlarge,g6.4xlarge,g6.2xlarge,g6.xlarge"`); a candidate
  type matches if it matches ANY listed rung (OR semantics). Each sub-pattern may
  itself be a wildcard (`p5.*`) or exact type. This is the natural "watch any of
  these interchangeable sizes" request (#135).

### Documentation
- Corrected the README Actions table for `hold` (#136). It previously read
  "Record availability without acting", which was doubly misleading: `hold`
  **does** act (it creates an On-Demand Capacity Reservation via
  `CreateCapacityReservation`, billable from the moment it's held), and
  capacity-reservation admission is **stricter than launchability** — so `hold`
  can report `InsufficientInstanceCapacity` for a type that `--action spawn`
  (`RunInstances`) would launch immediately. The table and new prose now
  distinguish the three actions (`notify` = signal only, `hold` = reserve
  capacity, `spawn` = launch) and point at `--action spawn` as the launchability
  test.

### Fixed
- A comma-separated instance-type pattern is no longer silently treated as one
  literal (nonexistent) type that can never match. Previously such a watch was
  accepted, went active, and polled forever with `match_count=0` while capacity
  was in fact abundant — a silent, costly false negative. Comma-lists now match
  per-rung (see Added), and if a pattern matches **no instance type your
  region(s) offer**, the first poll prints a clear warning ("pattern %q matches
  no known instance type — check for typos") instead of failing silently. The
  warning does not stop the watch, so a not-yet-offered type isn't blocked (#135).
- On a `hold` capacity failure, the poller now logs a clarifying note that a
  capacity reservation being refused does not mean the type can't be launched
  (it may still be launchable via `--action spawn`), so the log no longer reads
  as a launchability failure (#136). Retry logic is unchanged.

## [0.55.2] - 2026-09-09

### Security
- Bumped `google.golang.org/grpc` v1.83.1 → v1.83.2 (indirect, root module +
  `lambda/capacity-poller`), fixing CVE-2026-84445 (HIGH) — a gRPC-Go xDS-server
  DoS via crash. Trivy's gate flagged v1.83.1 (itself the fix for a prior grpc
  CVE); v1.83.2 supersedes it.

### Fixed
- Adapted the managed-table safety tests to the Substrate emulator bump
  (v0.107.0 → v0.109.0, in the Dependabot minor-and-patch group). Substrate now
  emulates DynamoDB `ListTagsOfResource`, so `DeleteManagedTables`' tag guard can
  finally confirm the `lagotto:managed=cli` tag it always relied on. Production
  behavior is unchanged; the "never delete a table whose managed tag can't be
  confirmed" invariant is now exercised with a table that genuinely lacks the tag
  (plus a new positive-path test that a tagged, CLI-owned table is deleted).
- Tidied `lambda/capacity-poller/go.sum` so its Substrate checksum tracks the
  root module's bump (recurring nested-module drift, lagotto#43).

## [0.55.1] - 2026-09-05

## [0.55.0] - 2026-09-04

### Added
- Six previously-missing `--spawn-config` YAML keys, closing the gap between
  what `lagotto watch --action spawn` / `lagotto launch` accept and what
  `spawn launch` itself supports (#129):
  - `user_data` (inline string, or `@path` to read a script from disk) and
    `user_data_file` (a path) — a custom bootstrap payload appended after
    spored installs. `user_data_file` takes precedence when both are set,
    matching spawn CLI's own behavior. Previously there was no way to express
    this at all.
  - `iam_role` (a role name to create/reuse) and `iam_policy_file` (a path to
    a custom IAM policy JSON document), composing with the existing
    `iam_policy` shorthands into one instance profile — mirroring `spawn
    launch --iam-role/--iam-policy/--iam-policy-file`.
  - `tags` — extra EC2 tags on the launched instance, as either a YAML map
    (`{env: prod}`) or a list of `key=value` strings (mirroring spawn's
    repeatable `--tag` flag).
  - `volume_size` — override the root EBS volume size in GiB.
  - `spot_max_price` — cap the Spot bid price (accepts a quoted string or a
    bare YAML number); previously only the `spot` bool was carried through,
    with no price ceiling.
  - `completion_file` — override the path spored watches for the on-complete
    signal.

### Security
- Bumped `golang.org/x/crypto` v0.53.0 → v0.55.0 and `google.golang.org/grpc`
  v1.82.1 → v1.83.1 (both indirect, root module + `lambda/capacity-poller`'s
  own nested module), fixing CVE-2026-56854 (CRITICAL) and CVE-2026-84304
  (HIGH) per Trivy — Dependabot's own bump landed one patch version short of
  the versions that actually fix these.

### Changed
- Dependency maintenance: 18 Go module updates across the root module and
  `lambda/capacity-poller`'s nested module (#128). No API or behavior change.

## [0.54.1] - 2026-08-29

### Security
- Bumped `golang.org/x/mod` (indirect) v0.37.0 → v0.40.0 in both the root
  module and `lambda/capacity-poller`'s own nested module, fixing
  CVE-2026-56864/CVE-2026-56865 (HIGH) — a malicious GOSUMDB/GOPROXY could
  forge module checksums. Trivy's security gate was failing on every PR
  against this dependency graph, independent of any code change.

## [0.54.0] - 2026-08-19

### Added
- **Release-time guard verifying the published binary reports its own tag**
  (#98). `.goreleaser.yaml` injects the version via a `-X
  github.com/spore-host/lagotto/cmd.Version={{.Version}}` ldflag, and the Go
  linker accepts `-X` for a symbol that no longer exists with no error or
  warning — a renamed variable, a moved package, or a renamed module path
  would build fine, release fine, and publish binaries that report `dev`
  forever, with no signal anywhere until a user pastes `lagotto version`
  into a bug report. `scripts/check-release-version.sh` (new, run
  automatically by the release workflow, or by hand via `make
  check-release-version TAG=vX.Y.Z`) builds with the real release ldflag —
  read out of `.goreleaser.yaml` rather than restated, so it exercises the
  actual wiring — and fails the tag unless the binary reports back the same
  version. It also refuses to publish while `CHANGELOG.md` still says
  `[Unreleased]`. Two companion unit tests
  (`cmd/release_version_guard_test.go`) catch the same drift earlier, in
  ordinary PR CI: one asserts `.goreleaser.yaml`'s ldflag still names the
  `cmd.Version` variable this package actually declares, the other asserts
  the release workflow still invokes the guard script.
- **`pkg/snipe.Options.ClientFor` — inject a pre-configured `*spawnaws.Client`
  builder, e.g. one pointed at a test emulator like Substrate** (#113).
  `Snipe` previously always resolved its AWS client via
  `spawnaws.NewClientWithRegion`, which loads config through the default AWS
  credential chain with no way to point it at a custom endpoint — so a
  consumer wanting to exercise `Snipe`'s real request-building/response-
  parsing/error-classification code against a fake EC2 endpoint (as calque's
  own `Acquirer` could, via `github.com/scttfrdmn/substrate/emulator`, before
  migrating to `Snipe`) had no way to do so. `Options.ClientFor`, if set,
  overrides the client-resolution function Snipe calls once per distinct
  region (cached for the run, same as the default); a caller sets it to
  something built via `spawnaws.NewClientFromConfig` with
  `config.WithBaseEndpoint` pointed at an emulator. Zero value is unchanged:
  the default AWS credential chain, same as before this field existed.

### Fixed
- **`pkg/snipe.Result.AvailabilityZone` now reports the AZ the instance
  actually landed in, not the requested placement AZ** (#114). When a
  `Target` has no `Placements` (letting EC2 choose the AZ, per
  `Target.Placements`'s own doc), `Result.AvailabilityZone` came back empty
  even though the instance genuinely launched into a specific AZ —
  `launchAcrossPlacements` was returning the loop's requested-placement `AZ`
  instead of `spawnaws.LaunchResult.AvailabilityZone`, which already carries
  the actual landed AZ from the `RunInstances` response. Invisible whenever a
  `Placement` pins a specific AZ (the request forces the two to match), so it
  only surfaced for the common unpinned case. Any caller logging or reporting
  on `Result.AvailabilityZone` for a non-placement-pinned `Snipe` now gets the
  real value instead of `""`.

### Changed
- Test-only: `pkg/deploy` (the `lagotto deploy`/`teardown` stack manager)
  gains a Substrate-backed test suite covering `Deploy`, `Teardown`,
  `StackOutputs`, `ensureBucket`, and `uploadArtifact` — previously 4.1%
  covered, now 75.5% (#1). No behavior change; exercises the real
  CloudFormation create/update/delete and S3 bucket/upload paths against
  `github.com/scttfrdmn/substrate/emulator` instead of only the four pure
  helper functions (`LambdaArtifactURL`, `DefaultBucketName`,
  `LambdaObjectKey`, `failedCreateStates`) that had tests before.

## [0.53.2] - 2026-08-17

## [0.53.1] - 2026-08-11

### Changed
- Dependency maintenance: bumped 15 Go module dependencies (across 2
  directories) and 5 GitHub Actions to their current releases (no API or
  behavior change).

## [0.53.0] - 2026-08-10

### Added
- **`pkg/failure.IsQuotaExceeded` — an exhausted account quota/limit is now
  distinguishable from the rest of the `FailureTerminal` bucket** (#116).
  Previously every terminal error (bad AMI, IAM denial, malformed request,
  *and* an exhausted quota) looked identical to a caller: an immediate
  give-up with no further signal. `pkg/snipe.Status` gains a
  `QuotaExceeded bool` field (set via `Options.Progress`), and
  `pkg/watcher.IsQuotaExceeded` aliases the same function. Does NOT change
  any default retry behavior — a quota error still classifies as
  `FailureTerminal` and `Snipe` still returns immediately on it, exactly as
  before. This is a purely additive signal for a caller building its own
  backoff/reduce-concurrency logic on top: e.g. several concurrent
  `pkg/snipe.Snipe` calls against the same account, where a quota ceiling
  saturated by the caller's OWN other in-flight requests may free up shortly
  — unlike a hard account wall that genuinely needs a support ticket, and
  which now reports `QuotaExceeded=false` instead of looking identical.
- Bumped `spore-host/truffle` v0.48.1 → v0.49.0 (root + `lambda/capacity-poller`),
  picking up truffle's latest fixes.

## [0.52.1] - 2026-08-09

### Fixed
- **`pkg/snipe.Snipe` now resolves its AWS client pinned to each target's
  region, instead of the ambient default-credential-chain region** (#111).
  `spawnaws.NewClient`'s default region (env/profile/shared-config) can
  differ from the region a `Target` actually asks to launch in — exactly the
  bug spawn#276 fixed for the CLI (`spawn launch --region`), which `Snipe`
  had silently regressed on. `Target.Region` is already required, so there
  was never a reason to consult the ambient chain instead. A `Fallback`
  target (#76) in a *different* region now gets its own region-pinned
  client too — one client per distinct region, cached for the run, not one
  client shared across every target regardless of region.

## [0.52.0] - 2026-08-07

### Added
- **`pkg/snipe` — a dependency-light leaf package for block-and-wait,
  single-target capacity acquire** (#106), extracted from `pkg/watcher.Snipe`.
  Its import graph is `spawn/pkg/aws` + `spawn/pkg/launcher` + this repo's
  `pkg/failure` only — no DynamoDB/S3/SageMaker/SNS, so a consumer that just
  wants "acquire this instance type here, blocking, and hand me the result"
  (calque's original #73 ask) no longer needs the persisted-Watch/poll-cycle
  machinery to get it. Adds three things `watcher.Snipe` didn't have:
  - **Per-AZ subnet placement** (`Placement{AZ, Subnet}`) — some AZs have no
    default subnet (observed: `us-west-2d` for g7e), which `watcher.Snipe`'s
    AZ-only list can't express and would regress into.
  - **A live progress callback** (`Options.Progress`), so a caller can drive a
    "waiting for capacity..." status line without polling anything itself.
  - **A bounded-unknown-failure fail-fast** (`Options.MaxConsecutiveUnknown`,
    default 3): a persistent but unrecognized error no longer retries silently
    for the whole deadline, masquerading as a capacity wait — it gives up after
    a bounded number of consecutive `FailureUnknown` results. A recognized
    capacity failure resets the counter, so genuine capacity waits stay
    uncapped.

  `watcher.Snipe` / `watcher.Spawner.Snipe` are unchanged — there were no
  callers of them in this repo, so they're kept as-is rather than rewired to
  delegate; new code should call `pkg/snipe.Snipe` directly.

### Fixed
- **`pkg/failure`'s terminal-error taxonomy was missing 5 misconfig codes**
  (#105): `ParameterNotFound`, `AccessDenied`, `AccessDeniedException`,
  `ValidationError`, `ValidationException`. These are surfaced by a pre-launch
  step (SSM param lookup, IAM check) rather than by `RunInstances` itself, and
  will never resolve by waiting — without this fix they classified as
  `FailureUnknown` (retry, capped) rather than `FailureTerminal` (stop), so a
  real misconfiguration (observed: a GPU AL2023 SSM parameter that doesn't
  exist) retried silently instead of failing fast with a clear signal.

### Changed
- CI moved off the self-hosted orion runner fleet onto `ubuntu-latest`. The
  fleet (colima/Docker on orion.local) is being decommissioned org-wide; no
  behavior change to the tool.

### Security
- **Added a Dependabot config, so the SHA-pinned actions and Go deps get bumped (#101).**
  Every action here is pinned to a commit SHA, which closes the mutable-tag hole
  but opens a staleness one: a SHA never moves — including past a security fix —
  and unlike `@v6` nothing updates it for you. Pinning is only safe if something
  bumps the pins, and nothing did. `actions/checkout@v6` had already moved
  upstream while this repo went on pinning the older commit, silently.

  This matters most for `release.yaml`, which pins the release-signing actions
  (`goreleaser-action`, `cosign-installer`, `attest-build-provenance`) — the
  supply-chain machinery. A frozen `cosign-installer` means releases keep getting
  signed by an old cosign, and cosign 3.x already changed its CLI surface.

  Weekly, grouped, with a 7-day cooldown so a freshly-published tag isn't proposed
  the day it ships. The actions group pattern is `*` rather than `actions/*`
  precisely because those signing actions aren't under `actions/`. The `gomod`
  entry watches **both** modules — root and `lambda/capacity-poller` — since the
  Lambda module pins its own deps; that split is exactly what #43 was about, where
  a stale nested `go.mod` failed govulncheck with an error naming neither the
  module nor the cause. Tests enforce both kinds of coverage. CI-only; no change
  to the tool.

### Changed
- **CI now fails on unformatted code (#99).** Nothing did before: CI had no
  formatting step, and `make fmt` rewrites files and always succeeds — convenient
  locally, but it cannot fail a build, so it could only ever report success. This
  tree happened to be clean; the gate is what keeps it that way. (The sibling
  repos drifted for months under the same configuration — spawn had 3 files
  unformatted on `main`, truffle 7.)

  `make check-fmt` reports drift instead of fixing it, with offenders listed and
  diffed, and now runs in CI. One invocation also covers
  `lambda/capacity-poller`, since `gofmt` walks paths rather than modules.
- Bumped the `substrate` test dependency v0.71.0 → v0.85.0 (root +
  `lambda/capacity-poller`). Test-only; no runtime or API change. Most relevant to
  lagotto: EC2 `describe_*` calls naming an explicit ID that does not exist now
  raise `Invalid*ID.NotFound` instead of returning an empty list (substrate#391),
  and each service serializes errors in its own wire protocol so `Error.Code` is
  the symbolic AWS code rather than an HTTP status (substrate#392) — the watcher
  classifies terminal-vs-retryable failures on exactly those codes, so those
  branches were previously unreachable offline. `RunInstances` also now rejects a
  launch that resolves no AMI (substrate#412), which is the failure mode a
  `--action spawn` watch must not silently succeed on.

  The v0.82.0–v0.85.0 span adds two more paths a `--action spawn` watch depends
  on: `RunInstances` now merges a launch template instead of ignoring it whenever
  the request also passes `ImageId` (substrate#453 — previously the launch
  "succeeded" with an instance that was not the one asked for), and EC2 rejects
  reserved `aws:`-prefixed tag keys (substrate#452). Lagotto writes only
  `lagotto:`-prefixed tags, so the latter is coverage gained, not behavior changed.
- Bumped `spore-host/libs` v0.43.2 → v0.43.3, `spore-host/spawn` v0.75.0 →
  v0.98.0, and `spore-host/truffle` v0.38.1 → v0.48.1 (root +
  `lambda/capacity-poller`), picking up each tool's latest fixes/features.

### Security
- Bumped `golang.org/x/text` to v0.39.0 (root + `lambda/capacity-poller`) to clear
  CVE-2026-56852 (a `norm.Iter` infinite loop on crafted input; HIGH). Indirect
  dependency; no API change.

### Documentation
- Added the project hero image to the top of the README.

## [0.51.0] - 2026-07-22

### Security
- **Bump `google.golang.org/grpc` → 1.82.1** (indirect) — resolves
  GHSA-hrxh-6v49-42gf (gRPC-Go xDS RBAC / HTTP/2, HIGH).
- **Release artifacts are now signed** with keyless cosign (Sigstore) + SLSA build
  provenance (#93). The release signs `checksums.txt` (which lists every
  archive/package hash) with the workflow's GitHub OIDC identity — no long-lived
  key — publishing `checksums.txt.bundle`, and attests build provenance. Verify a
  download with `cosign verify-blob --bundle` (see docs: "Verify a download").
  Takes effect from the next tagged release.

### Added
- **`CITATION.cff`** — machine-readable citation metadata so the repo is citable
  (GitHub "Cite this repository"); base for Zenodo DOI minting.

## [0.50.0] - 2026-07-19

### Documentation
- **`lagotto setup` help now describes that it also grants the hosted poller's
  runtime IAM policy** (#16), not just the DynamoDB tables — so the generated
  command reference reflects what `setup` actually does after `lagotto deploy`.

### Added
- **Command/flag reference is now generated from the CLI and drift-gated.** A
  hidden `lagotto gen-docs` command (via `libs/docgen`) emits the exhaustive
  per-command reference to `docs-gen/`; `make gen-docs` regenerates it and a CI
  `check-docs` gate fails if the committed reference drifts from the code, so the
  docs site's reference can no longer go stale (2026-07 docs audit). The
  previously-missing `setup`/`deploy`/`teardown` commands and the goal-driven
  `--maintain`/`--until` fleet flags are now covered automatically. Run `make
  gen-docs` after changing a command or flag. Uses `libs/docgen` v0.43.2, which
  HTML-escapes bare `<…>` and Vue `{{ … }}` tokens (e.g. `shell: <cmd>`) so the
  reference renders on the VitePress docs site.

### Changed
- **The hosted poller's runtime IAM permissions are now owned by `lagotto setup`,
  not the CloudFormation template** (#16). lagotto codifies the poller's policy in
  Go (`pkg/runtimeiam`) as the single source of truth — the same way it already
  owns its DynamoDB tables — so adding a permission is a one-file code change
  instead of a template edit, and code (what it calls) and grant (what it may
  call) no longer drift apart. The stack now creates only a **minimal** execution
  role (Lambda trust + CloudWatch Logs); `setup` attaches the permissions policy.
  Crucially, the privilege-escalation surface (`iam:CreateRole`/`PutRolePolicy`)
  stays with the human/CI identity running `setup` — the runtime Lambda never
  self-escalates. **After a fresh `lagotto deploy`, run `lagotto setup`** to grant
  the poller its spawn/hold/SageMaker/scheduler permissions; until then it can
  only send notifications.

### Added
- **Goal-driven fleet mode: `lagotto watch --maintain N --until <condition>`** (#70).
  Instead of firing an action once and retiring, a fleet watch **maintains ~N
  running workers until an external completion condition is true** — relaunching
  toward the goal each poll, **including from zero** after a correlated total
  reclaim. This is the missing primitive for idempotent, spot-friendly batch:
  pull-model workers + scale-out handle independent loss, and this durable
  supervisor handles the correlated-total-loss tail, so a spot batch job becomes
  genuinely fire-and-forget. Requires `--action spawn`. The `--until` condition
  is one of: `s3-empty: s3://b/wanted minus s3://b/done/` (done when every wanted
  key has a done key), `http-200: <url>`, or `shell: <cmd>` (shell runs only on a
  local `poll --daemon` — the hosted poller has no sandbox). Workers are tracked
  by a `lagotto:watch=<id>` tag and counted live each poll (reflecting real
  reclaim). Runs on both `poll --daemon` and the hosted Lambda poller; `list`
  shows `spawn×N` and `status` shows the fleet target + condition.
- **`watcher.Snipe` supports optional multi-region fallback** (#76). The
  block-and-wait acquire loop (`SnipeOptions.Fallbacks`) can now take an ordered
  list of additional targets — each a full `SnipeTarget` with its own region, AZs,
  and launch config — tried in order within each round after the primary region's
  AZ sweep hits `InsufficientInstanceCapacity`, before backing off. GPU capacity is
  bursty and region-uneven, so the zone with capacity is often in a different
  region than the one you picked. Opt-in and off by default: cross-region is not
  free like AZ-breadth (each region needs its own AMI id, in-region launch
  artifacts, SG/subnet/IAM), so the caller supplies a complete target per region.
  A terminal failure on any target still stops immediately.
- **Shared spore.host config base.** lagotto now honors the suite-wide
  `libs/sporeconfig` settings: new persistent `--profile`, `--region`, and
  `--account` flags, the `SPORE_PROFILE`/`SPORE_REGION`/`SPORE_ACCOUNT` env vars,
  and the `[spore]` table of `~/.config/spore/config.toml`, resolved
  flag > env > file > default. Every command's AWS config now loads through a
  shared `pkg/awscfg` helper, so a suite-wide profile/region applies consistently
  (previously lagotto used the bare ambient chain with no profile concept — and
  could point at a different account than spawn when reading spawn's infra tables).
  Unset = unchanged (ambient AWS chain); `launch`/`deploy` keep their own
  `--region` which still wins for that command.

### Fixed
- **`parseDuration` now accepts `w` (weeks) and `s` (seconds)** in addition to
  d/h/m, and gives clearer errors naming the offending input and the valid units
  (#41). `1w` / `45s` now work anywhere a short duration is accepted (watch TTL,
  extend).
- **A persistently-broken watch now stops after a bounded number of polls**
  instead of retrying every cycle until its TTL (#41). Launch/hold errors are now
  classified three ways: genuine capacity failures retry uncapped (a watch may
  wait days for scarce GPUs), deterministic serialization errors (a malformed
  stored config) fail immediately, and any other unrecognized error retries but
  counts toward a per-watch cap of 10 consecutive failures — so a single blip
  never kills a watch, but a sustained bad-IAM/region fault does.
- **A matched or launched watch record is now retained for 90 days** rather than
  being deleted at its original watch-expiry TTL (#41). Its `ttl_timestamp` is
  reset to the retention window on the matched/failed transition, mirroring the
  match-history retention, so a resolved watch and its history age out together.

### Changed
- Internal: renamed the SageMaker config loader to `loadSageMakerConfig` and
  documented why it intentionally skips the EC2 spawn-config key-normalization/TTL
  defaulting — a SageMaker job is a `CreateTrainingJobInput`-shaped document with a
  different schema, validated server-side at submit (#41).

## [0.49.0] - 2026-07-15

### Added
- New leaf package **`pkg/failure`** holds the launch-failure classifier
  (`FailureKind`, `ClassifyFailure`, the capacity/terminal code sets), importing
  only `errors`/`strings`/`smithy-go` + spawn's dependency-free `launchererr`
  sentinel — **zero AWS service SDKs**. `pkg/watcher` now aliases these, so every
  existing caller is unchanged, but a stateless consumer that only wants "is this
  launch error retryable?" (e.g. a block-and-wait acquire loop) can import the
  classifier without pulling the poller's 70+ AWS-SDK dependency tree (#75).
  Requires spawn ≥ v0.75.0.
- **`watcher.Snipe(ctx, target, opts)`** — a stateless, blocking, single-target
  capacity acquire for library consumers (#73). It wraps spawn's
  `launcher.Provision` with the capacity classify + backoff-retry loop
  (`ClassifyFailure`), so an embedding consumer that just wants "acquire this
  type here, block until it lands or the deadline passes" no longer needs the
  persisted-`Watch` / DynamoDB machinery or a reimplementation of the retry
  policy. Terminal failures (bad AMI/IAM, quota, post-launch teardown) return
  immediately; capacity failures back off (capped exponential, deadline-bounded).
  Returns a `MatchResult` with the launched instance id/region/AZ. The stateful
  `pkg/watcher` poller remains the path for multi-candidate, scheduled watches.

### Security
- **Pinned the CI/release Go toolchain to 1.26.5** to clear GO-2026-5856, a
  `crypto/tls` standard-library advisory present in go1.26.4. Builds now link the
  patched stdlib and govulncheck is green.
- **Pinned all GitHub Actions to commit SHAs** (with version comments) across
  the CI/security/release workflows, and pinned `trivy-action` from `@master`
  to a release. Clears the Semgrep `github-actions-mutable-action-tag` finding
  and hardens the CI supply chain.

## [0.48.1] - 2026-06-24

### Fixed
- **`lagotto deploy` no longer rolls back with a CloudFormation circular
  dependency** (#67). The v0.48.0 (#62) additions made the poller Lambda (and its
  auto-generated role) reference `SchedulerInvokeRole` (via the
  `SCHEDULER_INVOKE_ROLE_ARN` env var and the `iam:PassRole` grant), while that
  role still referenced the function by `!GetAtt` — closing a cycle among
  `[CapacityPollerFunction, CapacityPollerFunctionRole, SchedulerInvokeRole,
  CapacityPollerSchedule]` so the stack couldn't create in a clean account.
  `SchedulerInvokeRole` now references the poller by its **constructed ARN**
  (`!Sub`, the function name is fixed) instead of `!GetAtt`, breaking the cycle.
  Unblocks the hosted/autonomous poller (`lagotto deploy`).

## [0.48.0] - 2026-06-24

### Added
- **`lagotto launch --reservation-id cr-… --at-reservation-start`** fires a launch
  reliably at a Capacity Block for ML's window open (#62). It derives the start
  time from the reservation itself (no transcription), fires a touch early
  (`--fire-early`, default 2m) so EventBridge latency doesn't burn paid GPU time,
  and **retries through the boundary** (`--retry-interval`, default 30s) until the
  instance is running — absorbing the transient `InsufficientInstanceCapacity` /
  not-yet-active conditions right at the open. It gates on the reservation state
  (a dead/expired/payment-failed reservation fails fast; payment-pending retries),
  pins the launch to the reservation's AZ, and bounds the retry to end ~1h before
  the block closes (all Capacity Blocks end 11:30 UTC). Since EventBridge one-shots
  don't retry themselves, the hosted poller **self-reschedules** a fresh
  tight-interval schedule per attempt until success or the deadline. Requires
  `lagotto deploy`; bumps the spawn dependency to v0.65.0.

## [0.47.2] - 2026-06-24

### Security
- `cancel`, `extend`, `status`, and `history --watch-id` now authorize the caller
  as the watch's owner (#41). Watches are addressable by a guessable ID and the
  tables can be shared across an account, so previously anyone could cancel,
  extend, or read any watch by ID. The caller ARN is compared to the watch's
  recorded owner; a mismatch returns the same "not found" as a missing watch (no
  existence oracle).
- Closed an SSRF hole in webhook delivery (#40). `ValidateWebhookURL` vetted the
  resolved IPs, but `http.Client` re-resolved the host at connect time, so a
  DNS-rebinding attacker could pass validation with a public IP and then have the
  Lambda connect to `169.254.169.254` (the metadata endpoint holding the
  function's credentials). The notifier's HTTP client now re-validates the actual
  dial IP in a custom `DialContext` and pins the connection to it. Also,
  `ValidateWebhookURL` no longer fails open when a host can't be resolved — an
  unresolvable host is now rejected.

### Fixed
- Cancelling a `--action hold` watch now releases its capacity reservation
  (#41). A held ODCR previously billed until its 30-minute auto-expiry even after
  the watch was cancelled; `cancel` now calls `CancelCapacityReservation`
  (best-effort) for the recorded reservation.
- The deployed (hosted) poller can now actually service `--action hold` and
  `--service sagemaker` watches (#39). The poller never wired a `Holder`, so
  `hold` silently degraded to notify; and the CloudFormation policy lacked the
  IAM for both paths. Wire the `Holder`, and grant the poller
  `ec2:CreateCapacityReservation` (+ cancel/describe) for holds and
  `sagemaker:CreateTrainingJob` (+ a SageMaker-scoped `iam:PassRole`) for
  SageMaker jobs. (`--action spawn` IAM was already granted in #51.)
- **A post-launch failure no longer makes the AZ sweep launch (and orphan) an
  instance per AZ** (spawn#220). When a `--action spawn` launch succeeded at
  `RunInstances` but a downstream step failed, spawn now tears the instance down
  and reports it as a post-launch failure; lagotto's per-AZ retry (`launchAcrossAZs`)
  now classifies that as **terminal** and stops, instead of marching to the next
  AZ and launching another instance (each leaking an instance + ephemeral FSx).
  Bumps the spawn dependency to v0.63.1 (which adds the `ErrPostLaunch` sentinel
  and the instance-teardown-on-post-launch-failure fix).

## [0.47.1] - 2026-06-17

### Fixed
- **`lagotto deploy` no longer fails when the DynamoDB tables already exist** from
  prior CLI use (#59). The CLI auto-creates the tables on first `watch`/`launch`
  (#12) and the stack used to *also* create them, so the natural watch-then-deploy
  flow collided on `AlreadyExists` and rolled the whole stack back. The tables are
  now unambiguously **CLI-owned**: `lagotto deploy` ensures they exist, and the
  stack only *references* them by name (env vars + IAM) — it never creates or
  deletes them. The poller's IAM now also covers the **scheduled-launches** table
  (#49), which was previously missing (silent `AccessDenied` on scheduled launches
  from the hosted poller).
- **`lagotto deploy` recovers from a wedged stack**: a previous deploy left in a
  `ROLLBACK_COMPLETE` (or other failed-create) state — which can never be updated —
  is now deleted and recreated automatically instead of failing every retry (#59).

### Changed
- The hosted poller no longer auto-deletes the DynamoDB tables when they idle to
  empty unless `AUTO_DELETE_TABLES=true` (#59). A deployed poller is deliberate,
  persistent infrastructure that references the CLI-owned tables, so it must not
  delete them out from under itself; tear down explicitly with `lagotto deploy
  --teardown` or `lagotto teardown`. (The infra-free `poll --daemon` path is
  unaffected — it never deleted tables.) `lagotto deploy --teardown` now states
  that your data tables are retained.

## [0.47.0] - 2026-06-17

### Added
- **`poll --daemon` can be scoped to your own watches** in a shared account
  (#47): `--project NAME` (or `$LAGOTTO_PROJECT`), `--mine` (only watches you
  created), or `--watch w-aaa,w-bbb`. Previously a local daemon polled and acted
  on *every* watch in the account, so one operator's daemon could launch another
  project's `--action spawn` instance. A scoped daemon also exits when *its*
  watches are done, not the whole account's. `lagotto watch --project NAME` tags
  a watch for this scoping (also from `$LAGOTTO_PROJECT`).

### Fixed
- **Two pollers can no longer double-fire the same watch** (#47): before acting
  on a match, a poller now claims a short processing lease, so a second daemon —
  or a local daemon racing the hosted Lambda — skips a watch already being
  handled instead of duplicating the `RunInstances`/FSx-create. A crashed
  poller's lease ages out (it never blocks the watch). Disable with
  `poll --no-lease`. The hosted poller leases too.

## [0.46.0] - 2026-06-17

### Added
- **`lagotto launch`** schedules a future or recurring instance launch — fire once
  at a clock time (`--at`), after a delay (`--after 6h`), or on a recurring cron
  (`--cron`) — as opposed to `watch`, which fires on *capacity* appearing (#49).
  The motivating case is launching into an EC2 Capacity Block for ML at its
  reserved start time. It's driven by EventBridge Scheduler in the hosted poller
  stack, so it requires `lagotto deploy` first; the launched instance always
  carries a TTL (#38). One-shots self-delete their schedule after firing.
- **Overlap policy** for scheduled launches: when an instance with the same `Name`
  tag already exists at fire time, `--if-exists` decides what happens — `skip`
  (don't double-launch; the default for `--at`/`--after`, so a Capacity Block
  can't double-book), `launch` (launch anyway; the default for `--cron`, so each
  fire is a fresh box), or `replace` (terminate the existing instance, then
  launch). `--name` overrides the dedup key (defaults to the spawn config's name).

### Changed
- The hosted poller's self-teardown now reference-counts **pending scheduled
  launches** alongside active watches: the poller schedule and CLI-managed tables
  are only torn down when there are neither active watches nor pending scheduled
  launches, so a `lagotto launch --at next-week` can't have its infrastructure
  removed out from under it (#49).

## [0.45.0] - 2026-06-17

### Added
- A watch's `--spawn-config` can now target an **EC2 Capacity Reservation /
  Capacity Block for ML**: `reservation_id` launches into an existing reservation
  and `capacity_block: true` consumes a Capacity Block (forwarded to spawn's
  `--reservation-id`/`--capacity-block`, spawn#216). Bumps the spawn dependency to
  v0.62.0. Groundwork for scheduling a launch into a block at its start time (#49).
- **`lagotto deploy`** stands up the hosted capacity-poller stack (DynamoDB, SNS,
  Lambda, EventBridge Scheduler) **in your own AWS account** (#48), so watches are
  serviced server-side — armed once, then hands-off — instead of relying on a
  foreground `poll --daemon` that dies when your laptop sleeps. It downloads the
  published poller Lambda artifact for `--version`, uploads it to a bucket in your
  account (auto-named/created, or `--lambda-bucket`), and deploys the embedded
  CloudFormation template; the poller schedule deploys disabled and the first
  `lagotto watch` arms it. `lagotto deploy --teardown` deletes the stack. Both
  prompt for confirmation (`--yes` to skip).

## [0.44.0] - 2026-06-17

### Added
- The capacity-poller Lambda is now published as a release asset
  (`capacity-poller_lambda_linux_arm64.zip`) attached to each GitHub Release (#29).
  Previously the zip lived only in the `spore-host-infra` S3 bucket, so an end
  user couldn't obtain it — groundwork for a future `lagotto deploy` that stands
  up the poller stack in your own account (#48).

### Fixed
- **The hosted capacity-poller can now actually launch `--action spawn` watches.**
  Its Lambda execution role had Describe-only EC2 permissions, so a deployed
  `--action spawn` watch matched capacity but **silently failed to launch** (no
  `ec2:RunInstances` / `iam:PassRole`). The CloudFormation stack now grants the
  launch permissions spawn's headless launcher needs — `RunInstances`,
  `CreateTags`, security-group create/authorize, the AMI/VPC/subnet/keypair
  describe reads + `ssm:GetParameter`, and scoped `spored*` role/instance-profile
  management with `iam:PassRole` (conditioned to `ec2.amazonaws.com`). Redeploy
  the stack to apply.
- **lagotto now guarantees every spawned instance carries a TTL** (#38). The
  `--spawn-config` `ttl` field had no default or validation, so an `--action
  spawn` watch whose config omitted `ttl` could launch an instance with **no
  death clock** — running unbounded and billing forever, contrary to the
  "everything dies" invariant. lagotto now defaults a missing instance TTL to
  `24h` (matching the watch's own `--ttl` default) and rejects a malformed one at
  watch-create; the spawner also enforces a non-empty TTL as a hard floor at
  launch time, so no path (CLI daemon, hosted poller, or configs written by an
  older CLI) can launch a TTL-less instance.

## [0.43.0] - 2026-06-17

### Changed
- Bumped the `spawn` dependency to **v0.61.0**, which carries the FSx hardening
  arc for `--action spawn` watches that auto-create ephemeral FSx (#45): `--az` is
  honored when placing the filesystem (spawn#208), a capacity-failed launch no
  longer orphans the FSx (spawn#210), and — the key one for lagotto's
  retry-on-capacity loop — the ephemeral FSx is now created **only after**
  `RunInstances` succeeds (spawn#213), so a multi-AZ capacity-fail poll creates
  **zero** filesystems instead of one-per-AZ-attempt-per-poll.

### Fixed
- The `--action spawn` AZ retry sweep is now covered by tests (#45): a capacity
  failure tries every candidate AZ in preference order within a poll, stops at the
  first AZ that launches, and a terminal error (bad config/IAM) stops the sweep
  immediately. This is the lagotto-side guard that, paired with spawn#213, keeps a
  capacity-scarce FSx watch from churning filesystems.

### Added
- A watch's `--spawn-config` can now **auto-create ephemeral FSx Lustre storage**
  for the launched instance (#43): `fsx_create: true`, `fsx_lifecycle: ephemeral`,
  `fsx_s3_bucket`, `fsx_import_path`, `fsx_export_path`, `fsx_mount_point`,
  `fsx_storage_capacity`. The filesystem is created asynchronously and mounted by
  spored once AVAILABLE, so the poller never blocks (spawn#194/#202), and it's
  reaped when the instance terminates (spawn#192). Only `ephemeral` is supported
  here — durable storage must be pre-created and mounted via `fsxid`. Requires
  spawn ≥ 0.57.0. See spawn's `docs/durable-storage-fsx.md`.

### Security
- Semgrep SAST is now **enforcing** in CI (`--config=auto --error`) rather than
  report-only (#368). The scan was already clean — no findings to triage.

## [0.41.0] - 2026-06-14

### Added
- `lagotto watch --azs us-west-2b,us-west-2c` pins/orders the availability zones
  eligible within the watched region(s). Empty = all AZs. Widening across AZs is
  free (same-region data locality), so by design all eligible AZs are tried each
  poll (#34).

### Changed
- On an `--action spawn` match, lagotto now tries **every** offered AZ (in
  preference order) within a poll, retrying the next on `InsufficientInstance
  Capacity`, instead of attempting only one AZ and giving up until the next poll.
  For capacity-scarce types this materially shortens the wait at zero extra cost,
  since each AZ is an independent capacity pool reachable with no cross-AZ data
  charge. A terminal failure (bad AMI/IAM/quota) still stops immediately (#34).

### Documentation
- `--regions` help now notes that widening across regions can break data
  co-location (cross-region egress) and to prefer `--azs` within the data's
  region first (#34).

## [0.40.0] - 2026-06-13

### Added
- `lagotto poll --daemon [--interval 5m]`: a built-in foreground polling loop, so
  `watch --action spawn` works hands-off in your own account with **no Lambda /
  EventBridge / CloudFormation** (#30). It runs the same poll path as the hosted
  Lambda, polls immediately then every interval, and exits cleanly when no active
  watches remain or on Ctrl-C / SIGTERM. The hosted multi-tenant Lambda poller
  stays as the team option. Addresses the "nothing polls the watch in an
  end-user account" gap (#29).

## [0.39.2] - 2026-06-12

### Fixed
- Bump spawn to v0.44.2, which stops sending an empty `KeyName` to
  `RunInstances` (spawn#130). This was the second blocker on `--action spawn`
  launches (after the user-data fix in 0.39.1) — with both fixed, the
  watch→launch→run flow completes a headless, SSM-only launch end to end.

## [0.39.1] - 2026-06-12

### Fixed
- Bump spawn to v0.44.1, which fixes user-data base64 encoding in
  `launcher.Provision` (spawn#127). Before this, `--action spawn` launches failed
  at `RunInstances` with "Invalid BASE64 encoding of user data" — so the
  watch→launch→run flow now actually completes its first launch.

## [0.39.0] - 2026-06-12

### Added
- `lagotto version` now reports whether a newer release is available (an
  explicit, on-demand check) (#21).

### Fixed
- `--action spawn` now launches a fully-functional spore via spawn's headless
  launcher (`launcher.Provision`): the AMI is auto-detected, the spored bootstrap
  is installed, and the workload command + on-complete/pre-stop/idle actually
  run. A new `SpawnConfigFile` maps snake_case / kebab-case / CamelCase keys (so
  `on_complete`, `pre_stop`, `idle_timeout`, `iam_policy`, `command` are no longer
  silently dropped) and validates the config at watch-creation. Bumps the spawn
  dependency to v0.41.0 (#19).

## [0.38.1] - 2026-06

### Added
- `lagotto cancel --yes/-y` to skip the confirmation prompt (non-interactive use).

## [0.38.0] - 2026-06

### Changed
- SageMaker watches launch the job directly instead of the EC2-proxy approach,
  retrying on capacity error until provisioned (#14).

## [0.37.0] - 2026-06

### Added
- Auto-create the DynamoDB tables on first use and auto-tear-them-down when empty
  (#12).

## [0.36.0 – 0.36.2] - 2026-06

The 0.36.x series after the move to the standalone repo. Highlights:

### Added
- SageMaker EC2-proxy watch with retry-until-launch-or-TTL (#7).
- Standardized `version` subcommand output.

### Fixed
- Makefile LDFLAGS module path.

## [0.35.0] - 2026-06

Initial tagged release from the standalone `spore-host/lagotto` repository.

---

Older releases are summarized in the
[GitHub Releases](https://github.com/spore-host/lagotto/releases) for this repo.

[Unreleased]: https://github.com/spore-host/lagotto/compare/v0.70.0...HEAD
[0.70.0]: https://github.com/spore-host/lagotto/compare/v0.69.2...v0.70.0
[0.69.2]: https://github.com/spore-host/lagotto/compare/v0.69.1...v0.69.2
[0.69.1]: https://github.com/spore-host/lagotto/compare/v0.69.0...v0.69.1
[0.69.0]: https://github.com/spore-host/lagotto/compare/v0.68.0...v0.69.0
[0.68.0]: https://github.com/spore-host/lagotto/compare/v0.67.0...v0.68.0
[0.67.0]: https://github.com/spore-host/lagotto/compare/v0.66.0...v0.67.0
[0.66.0]: https://github.com/spore-host/lagotto/compare/v0.65.1...v0.66.0
[0.65.1]: https://github.com/spore-host/lagotto/compare/v0.65.0...v0.65.1
[0.65.0]: https://github.com/spore-host/lagotto/compare/v0.64.1...v0.65.0
[0.64.1]: https://github.com/spore-host/lagotto/compare/v0.64.0...v0.64.1
[0.64.0]: https://github.com/spore-host/lagotto/compare/v0.63.1...v0.64.0
[0.63.1]: https://github.com/spore-host/lagotto/compare/v0.63.0...v0.63.1
[0.63.0]: https://github.com/spore-host/lagotto/compare/v0.62.0...v0.63.0
[0.62.0]: https://github.com/spore-host/lagotto/compare/v0.61.0...v0.62.0
[0.61.0]: https://github.com/spore-host/lagotto/compare/v0.60.0...v0.61.0
[0.60.0]: https://github.com/spore-host/lagotto/compare/v0.59.0...v0.60.0
[0.59.0]: https://github.com/spore-host/lagotto/compare/v0.58.3...v0.59.0
[0.58.3]: https://github.com/spore-host/lagotto/compare/v0.58.2...v0.58.3
[0.58.2]: https://github.com/spore-host/lagotto/compare/v0.58.1...v0.58.2
[0.58.1]: https://github.com/spore-host/lagotto/compare/v0.58.0...v0.58.1
[0.58.0]: https://github.com/spore-host/lagotto/compare/v0.57.2...v0.58.0
[0.57.2]: https://github.com/spore-host/lagotto/compare/v0.57.1...v0.57.2
[0.57.1]: https://github.com/spore-host/lagotto/compare/v0.57.0...v0.57.1
[0.57.0]: https://github.com/spore-host/lagotto/compare/v0.56.0...v0.57.0
[0.56.0]: https://github.com/spore-host/lagotto/compare/v0.55.2...v0.56.0
[0.55.2]: https://github.com/spore-host/lagotto/compare/v0.55.1...v0.55.2
[0.55.1]: https://github.com/spore-host/lagotto/compare/v0.55.0...v0.55.1
[0.55.0]: https://github.com/spore-host/lagotto/compare/v0.54.1...v0.55.0
[0.54.1]: https://github.com/spore-host/lagotto/compare/v0.54.0...v0.54.1
[0.54.0]: https://github.com/spore-host/lagotto/compare/v0.53.2...v0.54.0
[0.53.2]: https://github.com/spore-host/lagotto/compare/v0.53.1...v0.53.2
[0.53.1]: https://github.com/spore-host/lagotto/compare/v0.53.0...v0.53.1
[0.53.0]: https://github.com/spore-host/lagotto/compare/v0.52.1...v0.53.0
[0.52.1]: https://github.com/spore-host/lagotto/compare/v0.52.0...v0.52.1
[0.52.0]: https://github.com/spore-host/lagotto/compare/v0.51.0...v0.52.0
[0.51.0]: https://github.com/spore-host/lagotto/compare/v0.50.0...v0.51.0
[0.50.0]: https://github.com/spore-host/lagotto/compare/v0.49.0...v0.50.0
[0.49.0]: https://github.com/spore-host/lagotto/compare/v0.48.1...v0.49.0
[0.48.1]: https://github.com/spore-host/lagotto/compare/v0.48.0...v0.48.1
[0.48.0]: https://github.com/spore-host/lagotto/compare/v0.47.2...v0.48.0
[0.47.2]: https://github.com/spore-host/lagotto/compare/v0.47.1...v0.47.2
[0.47.1]: https://github.com/spore-host/lagotto/compare/v0.47.0...v0.47.1
[0.47.0]: https://github.com/spore-host/lagotto/compare/v0.46.0...v0.47.0
[0.46.0]: https://github.com/spore-host/lagotto/compare/v0.45.0...v0.46.0
[0.45.0]: https://github.com/spore-host/lagotto/compare/v0.44.0...v0.45.0
[0.44.0]: https://github.com/spore-host/lagotto/compare/v0.43.0...v0.44.0
[0.43.0]: https://github.com/spore-host/lagotto/compare/v0.42.0...v0.43.0
[0.42.0]: https://github.com/spore-host/lagotto/compare/v0.41.0...v0.42.0
[0.41.0]: https://github.com/spore-host/lagotto/compare/v0.40.0...v0.41.0
[0.40.0]: https://github.com/spore-host/lagotto/compare/v0.39.2...v0.40.0
[0.39.2]: https://github.com/spore-host/lagotto/compare/v0.39.1...v0.39.2
[0.39.1]: https://github.com/spore-host/lagotto/compare/v0.39.0...v0.39.1
[0.39.0]: https://github.com/spore-host/lagotto/compare/v0.38.1...v0.39.0
[0.38.1]: https://github.com/spore-host/lagotto/compare/v0.38.0...v0.38.1
[0.38.0]: https://github.com/spore-host/lagotto/compare/v0.37.0...v0.38.0
[0.37.0]: https://github.com/spore-host/lagotto/compare/v0.36.2...v0.37.0
[0.35.0]: https://github.com/spore-host/lagotto/releases/tag/v0.35.0
