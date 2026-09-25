// Package doctor implements `lagotto doctor`: a strictly READ-ONLY health check
// that reports drift between the hosted capacity poller deployed in an account
// and what the running lagotto binary expects (#156).
//
// # The class of bug this exists for
//
// `lagotto setup` applies the poller's runtime permissions with PutRolePolicy,
// which REPLACES lagotto-runtime-policy wholesale with whatever grants are
// compiled into that binary's runtimeiam.PolicyDocument. So:
//
//   - upgrade lagotto and forget `setup` → the deployed poller keeps the OLD
//     policy, and new code that needs a new permission degrades silently;
//   - run an OLDER lagotto's `setup` → it REVERTS the newer grants.
//
// Neither produces a signal until a watch fails, and for the hosted poller that
// failure is only visible in /aws/lambda/lagotto-capacity-poller's CloudWatch
// Logs. That has bitten four times: #149 (iam:* on spawn-instance*, every hosted
// auto-spawn terminal in ~46s), #151 (pricing:GetProducts, 11/12 GPU watches
// terminal before any capacity check), #150, #153 (servicequotas). doctor is the
// structural answer: report the drift instead of letting it be discovered as a
// silent degradation.
//
// # Shape
//
// The command is split in two so the logic is testable without AWS at all:
//
//	Gather(ctx, Options) State   — the ONLY part that talks to AWS; all reads.
//	Evaluate(State) *Report      — pure; every PASS/WARN/FAIL decision lives here.
//
// Gather never fails as a whole: each read's error is recorded on its own State
// field, so one AccessDenied degrades one check to WARN instead of denying the
// user the other eight. And every AWS dependency arrives through an interface or
// a function field, so Gather itself is unit-testable against fakes.
//
// There is deliberately NO --fix. doctor is the command you run when you already
// suspect the account is wrong; it must never be the thing that changed it.
package doctor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// Status is a check outcome. Only FAIL makes the command exit non-zero: a WARN is
// something to know about, not something that is broken, and a doctor that exits
// non-zero on warnings can't be used as a CI gate (people just stop running it).
type Status string

const (
	// StatusPass — checked, and correct.
	StatusPass Status = "PASS"
	// StatusWarn — worth knowing, nothing is broken, exit code unaffected.
	StatusWarn Status = "WARN"
	// StatusFail — the account is wrong in a way that degrades the poller.
	StatusFail Status = "FAIL"
)

// Check names, fixed so `lagotto doctor -o json` output can be asserted against
// and grepped for.
const (
	CheckRuntimePolicy = "runtime-policy"
	CheckPollerVersion = "poller-version"
	CheckTables        = "tables"
	CheckIAMRoles      = "iam-roles"
	CheckPollerLambda  = "poller-lambda"
	CheckAlertsTopic   = "alerts-topic"
	CheckSchedule      = "schedule"
	CheckScheduleState = "schedule-state"
	CheckLegacyStack   = "legacy-stack"
)

// Check is one diagnostic line.
type Check struct {
	Name    string   `json:"name"`
	Status  Status   `json:"status"`
	Summary string   `json:"summary"`
	Detail  []string `json:"detail,omitempty"`
	// Fix holds the command(s) that resolve the finding, verbatim, so the report
	// ends with something copy-pasteable rather than advice.
	Fix []string `json:"fix,omitempty"`
}

// Report is the whole diagnosis.
type Report struct {
	Region     string  `json:"region"`
	AccountID  string  `json:"account_id"`
	CLIVersion string  `json:"cli_version"`
	Checks     []Check `json:"checks"`
	// PolicyDrift is the machine-readable form of the headline check, for
	// `-o json` consumers that want the exact grants rather than the prose.
	PolicyDrift *runtimeiam.PolicyDiff `json:"policy_drift,omitempty"`
}

// Failed reports whether any check FAILed — the command's exit-code condition.
func (r *Report) Failed() bool {
	for _, c := range r.Checks {
		if c.Status == StatusFail {
			return true
		}
	}
	return false
}

// Counts returns the number of passing, warning and failing checks.
func (r *Report) Counts() (pass, warn, fail int) {
	for _, c := range r.Checks {
		switch c.Status {
		case StatusPass:
			pass++
		case StatusWarn:
			warn++
		case StatusFail:
			fail++
		}
	}
	return
}

// --- gathering --------------------------------------------------------------

// TableAPI is the read-only slice of DynamoDB doctor needs: does the table exist.
// Narrow on purpose — doctor must not be able to touch a table's contents.
type TableAPI interface {
	DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// Infra is the read-only slice of pkg/deploy doctor needs. *deploy.Deployer
// satisfies it; a test supplies a fake.
type Infra interface {
	InspectPoller(ctx context.Context) (deploy.PollerInfo, error)
	InspectPollerSchedule(ctx context.Context) (deploy.ScheduleInfo, error)
	AlertsTopicPresent(ctx context.Context, region, accountID string) (bool, error)
	LegacyStackState(ctx context.Context, stackName string) (bool, string, error)
}

// Options is everything Gather needs. Every AWS dependency is an interface or a
// function field so the whole command runs offline in tests.
type Options struct {
	Region     string
	AccountID  string
	CLIVersion string
	// LegacyStackName is the CloudFormation stack to look for (`--stack-name`).
	LegacyStackName string
	// Tables are the three CLI-owned DynamoDB tables, in the order
	// watches/history/scheduled.
	Tables []string

	IAM      runtimeiam.IAMAPI
	DynamoDB TableAPI
	Infra    Infra
	// ActiveWatches counts the watches currently in the active state. A function
	// field rather than an interface because watcher.Store wraps a concrete
	// DynamoDB client; the CLI passes a closure over store.ListActiveWatches.
	ActiveWatches func(ctx context.Context) (int, error)
}

// State is the raw, read-only observation Evaluate turns into a Report. Each
// field carries its own error so a single unreadable resource costs one check,
// not the whole run.
type State struct {
	Region     string
	AccountID  string
	CLIVersion string
	// LegacyStackName is echoed through so remediation text can name it.
	LegacyStackName string

	// PolicyDoc is the deployed inline policy document (decoded JSON).
	// PolicyErr is runtimeiam.ErrRoleNotFound / ErrPolicyNotFound when it's
	// definitively absent, or a read failure.
	PolicyDoc string
	PolicyErr error

	// Roles maps role name → present. RoleErrs maps role name → read failure.
	Roles    map[string]bool
	RoleErrs map[string]error

	// Tables maps table name → present. TableErrs maps table name → read failure.
	Tables     map[string]bool
	TableOrder []string
	TableErrs  map[string]error

	Poller    deploy.PollerInfo
	PollerErr error

	Schedule    deploy.ScheduleInfo
	ScheduleErr error

	TopicPresent bool
	TopicErr     error

	ActiveWatches    int
	ActiveWatchesErr error

	LegacyStack       bool
	LegacyStackStatus string
	LegacyStackErr    error
}

// Gather performs every read. It makes no mutating call of any kind.
func Gather(ctx context.Context, o Options) State {
	st := State{
		Region:          o.Region,
		AccountID:       o.AccountID,
		CLIVersion:      o.CLIVersion,
		LegacyStackName: o.LegacyStackName,
		Roles:           map[string]bool{},
		RoleErrs:        map[string]error{},
		Tables:          map[string]bool{},
		TableErrs:       map[string]error{},
	}

	// 1. The headline: the deployed runtime policy.
	if o.IAM != nil {
		st.PolicyDoc, st.PolicyErr = runtimeiam.ReadRuntimePolicy(ctx, o.IAM)

		// 2. Both CLI-owned roles.
		for _, name := range []string{runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName} {
			present, err := runtimeiam.RoleExists(ctx, o.IAM, name)
			if err != nil {
				st.RoleErrs[name] = err
				continue
			}
			st.Roles[name] = present
		}
	}

	// 3. The three CLI-owned tables.
	for _, name := range o.Tables {
		if name == "" {
			continue
		}
		st.TableOrder = append(st.TableOrder, name)
		if o.DynamoDB == nil {
			continue
		}
		_, err := o.DynamoDB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)})
		if err == nil {
			st.Tables[name] = true
			continue
		}
		var notFound *dynamodbtypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			st.Tables[name] = false
			continue
		}
		st.TableErrs[name] = err
	}

	// 4. The poller, its schedule, the topic, and the legacy stack.
	if o.Infra != nil {
		st.Poller, st.PollerErr = o.Infra.InspectPoller(ctx)
		st.Schedule, st.ScheduleErr = o.Infra.InspectPollerSchedule(ctx)
		st.TopicPresent, st.TopicErr = o.Infra.AlertsTopicPresent(ctx, o.Region, o.AccountID)
		if o.LegacyStackName != "" {
			st.LegacyStack, st.LegacyStackStatus, st.LegacyStackErr = o.Infra.LegacyStackState(ctx, o.LegacyStackName)
		}
	}

	// 5. Are any watches actually armed?
	if o.ActiveWatches != nil {
		st.ActiveWatches, st.ActiveWatchesErr = o.ActiveWatches(ctx)
	}
	return st
}

// --- evaluation (pure) ------------------------------------------------------

// Evaluate turns an observed State into a Report. Pure: no AWS, no clock, no
// environment — so every PASS/WARN/FAIL rule is directly unit-testable.
func Evaluate(st State) *Report {
	r := &Report{Region: st.Region, AccountID: st.AccountID, CLIVersion: st.CLIVersion}

	policyCheck, drift := evalRuntimePolicy(st)
	r.Checks = append(r.Checks, policyCheck)
	r.PolicyDrift = drift

	if c, ok := evalPollerVersion(st); ok {
		r.Checks = append(r.Checks, c)
	}
	r.Checks = append(r.Checks,
		evalTables(st),
		evalRoles(st),
		evalPollerLambda(st),
		evalAlertsTopic(st),
		evalSchedule(st),
	)
	if c, ok := evalScheduleState(st); ok {
		r.Checks = append(r.Checks, c)
	}
	if c, ok := evalLegacyStack(st); ok {
		r.Checks = append(r.Checks, c)
	}
	return r
}

// evalRuntimePolicy is the check this command exists for.
//
// The asymmetry between the two halves of the drift is deliberate:
//
//   - grants this binary expects but the deployed policy LACKS → FAIL. The
//     poller cannot do something the code assumes it can, and the failure mode is
//     a watch dying with an AccessDenied that classifies terminal.
//   - grants the deployed policy has that this binary does not KNOW → WARN. A
//     newer lagotto applied that policy; nothing is broken, but running THIS
//     binary's `setup` would revert them.
//
// A read failure that isn't a definitive NoSuchEntity is a WARN, never a FAIL: we
// don't know whether there's drift, and "your policy is wrong" is the wrong thing
// to tell someone whose real problem is a missing iam:GetRolePolicy permission.
func evalRuntimePolicy(st State) (Check, *runtimeiam.PolicyDiff) {
	c := Check{Name: CheckRuntimePolicy}
	switch {
	case errors.Is(st.PolicyErr, runtimeiam.ErrRoleNotFound):
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("the poller execution role %s does not exist — the hosted poller was never deployed in this account/region", runtimeiam.RoleName)
		c.Fix = []string{"lagotto deploy --version <release>", "lagotto setup"}
		return c, nil
	case errors.Is(st.PolicyErr, runtimeiam.ErrPolicyNotFound):
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("role %s exists but carries no %s — the poller can only NOTIFY; it cannot spawn, hold or submit", runtimeiam.RoleName, runtimeiam.PolicyName)
		c.Fix = []string{"lagotto setup"}
		return c, nil
	case st.PolicyErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not read %s: %v", runtimeiam.PolicyName, st.PolicyErr)
		c.Detail = []string{"this is not a drift finding — doctor could not tell either way (iam:GetRolePolicy / iam:GetRole are needed)"}
		return c, nil
	}

	diff, err := runtimeiam.DiffRuntimePolicy(st.PolicyDoc, st.Region, st.AccountID)
	if err != nil {
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not compare the deployed %s: %v", runtimeiam.PolicyName, err)
		return c, nil
	}
	if diff.Empty() {
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("deployed %s matches this lagotto (%s)", runtimeiam.PolicyName, displayVersion(st.CLIVersion))
		return c, diff
	}
	if len(diff.Missing) > 0 {
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("the deployed %s is BEHIND this lagotto: %d grant(s) this binary expects are missing",
			runtimeiam.PolicyName, len(diff.Missing))
		c.Detail = append(c.Detail, "missing actions: "+strings.Join(diff.MissingActions(), ", "))
		for _, line := range diff.MissingSummary() {
			c.Detail = append(c.Detail, "  - missing: "+line)
		}
		c.Detail = append(c.Detail,
			"until this is applied the poller fails these calls with AccessDenied, which classifies TERMINAL — the watch dies instead of waiting for capacity")
		c.Fix = []string{"lagotto setup"}
	}
	if len(diff.Extra) > 0 {
		if c.Status == "" {
			c.Status = StatusWarn
			c.Summary = fmt.Sprintf("the deployed %s is AHEAD of this lagotto: %d grant(s) this binary does not know about",
				runtimeiam.PolicyName, len(diff.Extra))
			c.Detail = append(c.Detail,
				fmt.Sprintf("a NEWER lagotto applied this policy; running THIS binary's 'lagotto setup' (%s) would REVERT those grants",
					displayVersion(st.CLIVersion)))
			c.Fix = []string{"upgrade lagotto before running 'lagotto setup' again"}
		} else {
			c.Detail = append(c.Detail,
				fmt.Sprintf("and %d grant(s) this binary does not know about (a newer lagotto applied them; this binary's 'setup' would revert them)", len(diff.Extra)))
		}
		c.Detail = append(c.Detail, "extra actions: "+strings.Join(diff.ExtraActions(), ", "))
		for _, line := range diff.ExtraSummary() {
			c.Detail = append(c.Detail, "  + extra: "+line)
		}
	}
	return c, diff
}

// evalPollerVersion compares the deployed poller artifact's version with this
// CLI's. Reported as WARN at worst: a stale poller is real drift, but it is the
// kind you fix with a deploy, and it does not mean anything is broken right now.
//
// ok is false when there is no function to talk about — the poller-lambda check
// already reports that, and a second line saying the same thing is noise.
func evalPollerVersion(st State) (Check, bool) {
	if st.PollerErr != nil || !st.Poller.Exists {
		return Check{}, false
	}
	c := Check{Name: CheckPollerVersion}
	deployed := st.Poller.Version
	if deployed == "" {
		c.Status = StatusWarn
		c.Summary = "the deployed poller's version is UNKNOWN (no Version tag on the Lambda)"
		c.Detail = []string{
			"a poller deployed by lagotto before the Version tag existed carries no version stamp, and nothing else on a Lambda reveals which release its code came from (Code.Location is an internal presigned URL, not the artifact key)",
			"doctor will not guess: a wrong version claim is worse than none",
			"re-deploying stamps the tag, after which this check becomes meaningful",
		}
		c.Fix = []string{fmt.Sprintf("lagotto deploy --version %s", strings.TrimPrefix(st.CLIVersion, "v"))}
		return c, true
	}
	cmp, ok := compareVersions(deployed, st.CLIVersion)
	if !ok {
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("cannot compare the deployed poller (v%s) with this CLI (%s)", deployed, displayVersion(st.CLIVersion))
		c.Detail = []string{"one of the two versions is not a release version (a dev build, say), so there is nothing meaningful to compare"}
		return c, true
	}
	switch {
	case cmp < 0:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("the deployed poller is OLDER than this CLI (poller v%s, CLI v%s)", deployed, strings.TrimPrefix(st.CLIVersion, "v"))
		c.Detail = []string{"the hosted poller runs its own copy of lagotto's code; a CLI upgrade does not update it"}
		c.Fix = []string{fmt.Sprintf("lagotto deploy --version %s", strings.TrimPrefix(st.CLIVersion, "v"))}
	case cmp > 0:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("the deployed poller is NEWER than this CLI (poller v%s, CLI v%s)", deployed, strings.TrimPrefix(st.CLIVersion, "v"))
		c.Detail = []string{"upgrade this CLI rather than deploying it backwards — and do not run 'lagotto setup' from this binary, which could revert the newer poller's permissions"}
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("the deployed poller matches this CLI (v%s)", deployed)
	}
	return c, true
}

func evalTables(st State) Check {
	c := Check{Name: CheckTables}
	var missing, unreadable []string
	for _, name := range st.TableOrder {
		if err, bad := st.TableErrs[name]; bad {
			unreadable = append(unreadable, fmt.Sprintf("%s (%v)", name, err))
			continue
		}
		if !st.Tables[name] {
			missing = append(missing, name)
		}
	}
	switch {
	case len(missing) > 0:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("%d of %d CLI-owned DynamoDB table(s) missing: %s", len(missing), len(st.TableOrder), strings.Join(missing, ", "))
		c.Fix = []string{"lagotto setup"}
	case len(unreadable) > 0:
		c.Status = StatusWarn
		c.Summary = "could not check table(s): " + strings.Join(unreadable, "; ")
	case len(st.TableOrder) == 0:
		c.Status = StatusWarn
		c.Summary = "no table names were supplied to check"
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("all %d CLI-owned DynamoDB tables exist (%s)", len(st.TableOrder), strings.Join(st.TableOrder, ", "))
	}
	if len(unreadable) > 0 && c.Status == StatusFail {
		c.Detail = append(c.Detail, "also could not check: "+strings.Join(unreadable, "; "))
	}
	return c
}

func evalRoles(st State) Check {
	c := Check{Name: CheckIAMRoles}
	want := []string{runtimeiam.RoleName, runtimeiam.SchedulerInvokeRoleName}
	var missing, unreadable []string
	for _, name := range want {
		if err, bad := st.RoleErrs[name]; bad {
			unreadable = append(unreadable, fmt.Sprintf("%s (%v)", name, err))
			continue
		}
		if !st.Roles[name] {
			missing = append(missing, name)
		}
	}
	switch {
	case len(missing) > 0:
		c.Status = StatusFail
		c.Summary = "CLI-owned IAM role(s) missing: " + strings.Join(missing, ", ")
		c.Detail = []string{"both roles are created by the CLI, not by the stack; the Scheduler invoke role is also what per-launch scheduled launches use"}
		c.Fix = []string{"lagotto deploy --version <release>"}
	case len(unreadable) > 0:
		c.Status = StatusWarn
		c.Summary = "could not check IAM role(s): " + strings.Join(unreadable, "; ")
	default:
		c.Status = StatusPass
		c.Summary = "both CLI-owned IAM roles exist (" + strings.Join(want, ", ") + ")"
	}
	return c
}

func evalPollerLambda(st State) Check {
	c := Check{Name: CheckPollerLambda}
	switch {
	case st.PollerErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not check the poller Lambda: %v", st.PollerErr)
	case !st.Poller.Exists:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("the poller Lambda %s is not deployed — no watch is serviced server-side", deploy.PollerFunctionName)
		c.Fix = []string{"lagotto deploy --version <release>"}
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("the poller Lambda %s is deployed", deploy.PollerFunctionName)
		if st.Poller.RoleARN != "" {
			c.Detail = []string{"execution role: " + st.Poller.RoleARN}
		}
	}
	return c
}

func evalAlertsTopic(st State) Check {
	c := Check{Name: CheckAlertsTopic}
	switch {
	case st.TopicErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not check the SNS alerts topic: %v", st.TopicErr)
	case !st.TopicPresent:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("the SNS alerts topic %s does not exist — a matching watch cannot notify anyone", deploy.AlertsTopicName)
		c.Fix = []string{"lagotto deploy --version <release>"}
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("the SNS alerts topic %s exists", deploy.AlertsTopicName)
	}
	return c
}

func evalSchedule(st State) Check {
	c := Check{Name: CheckSchedule}
	switch {
	case st.ScheduleErr != nil:
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not check the EventBridge schedule: %v", st.ScheduleErr)
	case !st.Schedule.Exists:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("the EventBridge schedule %s does not exist — nothing ever invokes the poller", deploy.PollerScheduleName)
		c.Fix = []string{"lagotto deploy --version <release>"}
	default:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("the EventBridge schedule %s exists (%s, %s)", deploy.PollerScheduleName, st.Schedule.State, st.Schedule.Expression)
	}
	return c
}

// evalScheduleState is the silent-death check.
//
// The schedule's state is owned out of band: `lagotto watch`/`extend` enable it,
// and the poller disables it when the last active watch goes away. So the
// invariant is "ENABLED iff at least one watch is active", and the two ways to
// break it are not equally serious:
//
//   - ENABLED with no active watches → WARN. Harmless: the poller wakes every
//     five minutes, finds nothing, and disables itself. Costs a little billing and
//     log noise.
//   - DISABLED with active watches → FAIL. This is the silent death: watches
//     exist, the user believes they're armed, and NOTHING IS POLLING THEM. No
//     error is produced anywhere, ever — the watches simply expire unserviced.
//
// ok is false when there's no schedule to reason about (evalSchedule covers that)
// or the active-watch count is unknown.
func evalScheduleState(st State) (Check, bool) {
	if st.ScheduleErr != nil || !st.Schedule.Exists {
		return Check{}, false
	}
	c := Check{Name: CheckScheduleState}
	if st.ActiveWatchesErr != nil {
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("schedule is %s, but the active-watch count could not be read: %v", st.Schedule.State, st.ActiveWatchesErr)
		return c, true
	}
	enabled := st.Schedule.Enabled()
	switch {
	case enabled && st.ActiveWatches > 0:
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("schedule is ENABLED and %d watch(es) are active", st.ActiveWatches)
	case enabled && st.ActiveWatches == 0:
		c.Status = StatusWarn
		c.Summary = "schedule is ENABLED but no watches are active"
		c.Detail = []string{"harmless — the poller wakes every 5 minutes, finds nothing to do and disables itself; until then it's billing and log noise"}
	case !enabled && st.ActiveWatches > 0:
		c.Status = StatusFail
		c.Summary = fmt.Sprintf("%d watch(es) are ACTIVE but the schedule is DISABLED — nothing is polling them", st.ActiveWatches)
		c.Detail = []string{
			"this is the silent-death case: the watches look armed in 'lagotto list', no error is reported anywhere, and they will simply expire unserviced",
			"a re-created schedule comes back DISABLED, so this is the state to check after a redeploy or a CloudFormation migration",
		}
		c.Fix = []string{
			"lagotto extend <watch-id> --ttl <duration>   # re-enables the schedule",
			"lagotto watch …                              # arming any watch also enables it",
		}
	default:
		c.Status = StatusPass
		c.Summary = "schedule is DISABLED and no watches are active (the poller self-disables when idle)"
	}
	return c, true
}

// evalLegacyStack reports a leftover CloudFormation stack, reusing pkg/deploy's
// #154 warning text rather than inventing a second wording for it.
//
// ok is false when there is nothing to say — no stack, or no stack name to look
// for. A detection FAILURE is also silent here by design: it's advisory, exactly
// as in `lagotto deploy`, and it must not be what a user reads instead of the
// checks they came for.
func evalLegacyStack(st State) (Check, bool) {
	if st.LegacyStackName == "" || st.LegacyStackErr != nil || !st.LegacyStack {
		return Check{}, false
	}
	c := Check{
		Name:    CheckLegacyStack,
		Status:  StatusWarn,
		Summary: fmt.Sprintf("a legacy CloudFormation stack %q still exists (%s)", st.LegacyStackName, st.LegacyStackStatus),
		Detail: strings.Split(
			deploy.LegacyStackAdoptionWarning(st.LegacyStackName, st.AccountID, st.Region), "\n"),
		Fix: []string{"lagotto deploy --migrate-from-cloudformation"},
	}
	return c, true
}

// --- version comparison -----------------------------------------------------

// compareVersions compares two dotted release versions, returning -1/0/1 and
// whether the comparison was possible at all. ok is false for "dev", an empty
// string, or anything whose first component isn't a number — in which case the
// caller must say "unknown" rather than pick a side.
//
// Deliberately local and tiny: the only versions involved are lagotto release
// tags (X.Y.Z, optionally v-prefixed, optionally with a pre-release suffix), and
// libs' equivalent helper is unexported.
func compareVersions(a, b string) (int, bool) {
	av, aok := parseVersion(a)
	bv, bok := parseVersion(b)
	if !aok || !bok {
		return 0, false
	}
	for i := 0; i < 3; i++ {
		if av[i] != bv[i] {
			if av[i] < bv[i] {
				return -1, true
			}
			return 1, true
		}
	}
	return 0, true
}

func parseVersion(v string) ([3]int, bool) {
	var out [3]int
	v = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), "v"))
	if v == "" || v == "dev" {
		return out, false
	}
	parts := strings.SplitN(v, ".", 3)
	for i := 0; i < len(parts) && i < 3; i++ {
		// Drop any pre-release/build suffix on the last component ("1-rc1" → "1").
		num := strings.SplitN(strings.SplitN(parts[i], "-", 2)[0], "+", 2)[0]
		n, err := strconv.Atoi(num)
		if err != nil {
			return [3]int{}, false
		}
		out[i] = n
	}
	return out, true
}

// displayVersion renders a CLI version for prose, making a dev build explicit
// rather than printing a bare "dev" that reads like a version number.
func displayVersion(v string) string {
	if v == "" || v == "dev" {
		return "dev build"
	}
	return "v" + strings.TrimPrefix(v, "v")
}
