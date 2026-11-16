package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/spf13/cobra"
	fp "github.com/spore-host/libs/footprint"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/inventory"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// The plan-driven teardown (lagotto#325): `lagotto teardown --plan <file|->`
// executes a `lagotto footprint -o json` report through the shared executor
// (libs/footprint.Execute). It is a second path beside the fixed one
// (runTeardown), which it does not change; the two share flags only where
// they mean the same thing, and none of --force and --iam-roles applies here.
var (
	teardownPlan    string
	teardownMode    string
	teardownYes     bool
	teardownNoLease bool
)

// noLeaseFlag is the hidden flag reap mode needs until the teardown lease
// (lagotto#326) exists.
const noLeaseFlag = "i-understand-no-lease"

func init() {
	teardownCmd.Flags().StringVar(&teardownPlan, "plan", "",
		"Execute a footprint plan: the JSON 'lagotto footprint -o json' writes, from a file or - for stdin")
	teardownCmd.Flags().StringVar(&teardownMode, "mode", string(fp.ModeReport),
		"With --plan: report (say what would be deleted; the default) or reap (delete; needs --yes)")
	teardownCmd.Flags().BoolVar(&teardownYes, "yes", false, "With --plan --mode reap: confirm the deletion")
	teardownCmd.Flags().BoolVar(&teardownNoLease, noLeaseFlag, false,
		"With --plan --mode reap: delete without a teardown lease (lagotto#326), so nothing stops a concurrent setup or watch")
	if err := teardownCmd.Flags().MarkHidden(noLeaseFlag); err != nil {
		panic(err)
	}
}

// checkTeardownFlags rejects flags of one teardown path given to the other.
func checkTeardownFlags(cmd *cobra.Command) error {
	changed := func(names ...string) []string {
		var out []string
		for _, n := range names {
			if cmd.Flags().Changed(n) {
				out = append(out, "--"+n)
			}
		}
		return out
	}
	if teardownPlan == "" {
		if f := changed("mode", "yes", noLeaseFlag); len(f) > 0 {
			return fmt.Errorf("%s apply only with --plan", strings.Join(f, ", "))
		}
		return nil
	}
	if f := changed("force", "iam-roles"); len(f) > 0 {
		return fmt.Errorf("%s do not apply with --plan: the plan says what is deleted, and its classes decide what may be", strings.Join(f, ", "))
	}
	return nil
}

func runTeardownPlan(cmd *cobra.Command) error {
	mode := fp.Mode(teardownMode)
	switch {
	case mode != fp.ModeReport && mode != fp.ModeReap:
		return fmt.Errorf("--mode %q: use report or reap", teardownMode)
	case mode == fp.ModeReap && !teardownYes:
		return fmt.Errorf("--mode reap deletes what the plan lists; pass --yes to confirm (run --mode report first to see what)")
	case mode == fp.ModeReap && !teardownNoLease:
		return fmt.Errorf("refusing --mode reap: there is no teardown lease yet (lagotto#326), so nothing would stop " +
			"another user's 'lagotto setup' or 'watch' racing this deletion in a shared account. Run --mode report")
	}
	planned, err := readPlan(cmd.InOrStdin(), teardownPlan)
	if err != nil {
		return err
	}
	ctx := context.Background()
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	acct, err := setupCallerAccount(ctx, cfg)
	if err != nil {
		return fmt.Errorf("not deleting anything: resolve the AWS account ID: %w", err)
	}
	return executePlan(ctx, cmd.OutOrStdout(), cfg, acct, planned, mode)
}

// readPlan decodes the plan from path, or stdin for "-".
func readPlan(stdin io.Reader, path string) (*fp.Report, error) {
	r := stdin
	if path != "-" {
		f, err := os.Open(path) // #nosec G304 -- the user names the plan file to read
		if err != nil {
			return nil, fmt.Errorf("read the plan: %w", err)
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	rep, err := fp.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("read the plan %s: %w", path, err)
	}
	return rep, nil
}

// planScope is the footprint a plan is re-checked against: the regions it
// names, and every region when it names an account-wide resource (which only
// a footprint of every region can classify).
func planScope(planned *fp.Report) inventory.Scope {
	var s inventory.Scope
	seen := map[string]bool{}
	for _, r := range planned.Resources {
		if r.Region == inventory.Global {
			s.AllRegions = true
			continue
		}
		if r.Region != "" && !seen[r.Region] {
			seen[r.Region] = true
			s.Regions = append(s.Regions, r.Region)
		}
	}
	sort.Strings(s.Regions)
	if s.AllRegions {
		s.Regions = nil
	}
	return s
}

// executePlan re-takes the footprint, keeps what is in use, and runs the rest
// of the plan through footprint.Execute.
func executePlan(ctx context.Context, out io.Writer, cfg aws.Config, acct string, planned *fp.Report, mode fp.Mode) error {
	if len(planned.Resources) == 0 {
		_, _ = fmt.Fprintln(out, "The plan lists nothing; nothing to do.")
		return nil
	}
	if planned.Account != "" && planned.Account != acct {
		return fmt.Errorf("not deleting anything: the plan is for account %s, but these credentials are for %s", planned.Account, acct)
	}
	live, err := takeFootprint(ctx, cfg, acct, planScope(planned))
	if err != nil {
		return fmt.Errorf("not deleting anything: re-check the plan against the account: %w", err)
	}
	plan, kept, err := inventory.PlanTeardown(planned, live, footprintOptions())
	if err != nil {
		return fmt.Errorf("not deleting anything: %w", err)
	}
	var lease *lifecycle.Lease
	if mode == fp.ModeReap {
		if lease, err = unfencedLease(ctx); err != nil {
			return fmt.Errorf("not deleting anything: %w", err)
		}
	}
	res, execErr := fp.Execute(ctx, plan, mode, inventory.NewDeleter(live, newTeardownDeleters(cfg, acct)), lease)
	if res == nil {
		return execErr
	}
	if err := writeTeardownResult(out, plan, kept, res, live.Warnings, getOutputFormat()); err != nil {
		return err
	}
	if execErr != nil {
		return fmt.Errorf("teardown --plan: %d step(s) failed: %w", res.Count(fp.OutcomeFailed)+res.Count(fp.OutcomeStopped), execErr)
	}
	return nil
}

// unfencedLease is the stand-in for the teardown lease (lagotto#326) that
// --i-understand-no-lease accepts. footprint.Execute takes a lease in reap
// mode and heartbeats it before every delete, so that a teardown that lost
// the account to someone else's setup stops. This one is held on an
// in-memory table no other lagotto can see: its heartbeats always succeed and
// fence nothing, which is exactly what the flag acknowledges.
func unfencedLease(ctx context.Context) (*lifecycle.Lease, error) {
	table := lifecycletest.New()
	c, err := lifecycle.New(lifecycle.Options{
		DynamoDB:      func(string) (lifecycle.API, error) { return table, nil },
		ControlRegion: "local",
		Owner:         "lagotto teardown --plan (no lease)",
	})
	if err != nil {
		return nil, err
	}
	if _, _, err := c.Observe(ctx, inventory.Tool, lifecycle.ScopeGlobal, false); err != nil {
		return nil, fmt.Errorf("take the local stand-in lease: %w", err)
	}
	lease, err := c.Claim(ctx, inventory.Tool, lifecycle.ScopeGlobal, 0)
	if err != nil {
		return nil, fmt.Errorf("take the local stand-in lease: %w", err)
	}
	return lease, nil
}

// regionDeleter is one region's deleters: deploy --teardown's (the Deployer)
// and teardown's tables (the Store).
type regionDeleter struct {
	*deploy.Deployer
	*watcher.Store
}

// iamAccountDeleter is the account-wide deleters: `teardown --iam-roles`'s.
type iamAccountDeleter struct {
	client    *iam.Client
	accountID string
}

func (d iamAccountDeleter) DeleteRole(ctx context.Context, name string) (bool, error) {
	return runtimeiam.DeleteRole(ctx, d.client, name)
}

func (d iamAccountDeleter) DeleteInstanceProfile(ctx context.Context, name string) (bool, error) {
	return runtimeiam.DeleteInstanceProfile(ctx, d.client, name)
}

func (d iamAccountDeleter) DeletePolicy(ctx context.Context) (cliiam.Deletion, error) {
	return cliiam.DeletePolicy(ctx, d.client, d.accountID)
}

// newTeardownDeleters builds the deleters a plan is executed with. A seam for
// tests.
var newTeardownDeleters = func(cfg aws.Config, accountID string) inventory.Deleters {
	var mu sync.Mutex
	regions := map[string]inventory.RegionDeleter{}
	return inventory.Deleters{
		AccountID: accountID,
		Region: func(region string) inventory.RegionDeleter {
			mu.Lock()
			defer mu.Unlock()
			if d, ok := regions[region]; ok {
				return d
			}
			c := cfg.Copy()
			c.Region = region
			d := regionDeleter{Deployer: deploy.New(c), Store: watcher.NewStore(c, watchesTable, historyTable)}
			regions[region] = d
			return d
		},
		Account: iamAccountDeleter{client: iam.NewFromConfig(cfg), accountID: accountID},
	}
}

// teardownStep is one line of the plan's outcome.
type teardownStep struct {
	ARN     string `json:"arn"`
	Type    string `json:"type"`
	Region  string `json:"region"`
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
	Error   string `json:"error,omitempty"`
}

// teardownReport is `teardown --plan -o json`.
type teardownReport struct {
	Mode     fp.Mode        `json:"mode"`
	Kept     []teardownStep `json:"kept"`
	Steps    []teardownStep `json:"steps"`
	Warnings []string       `json:"warnings"`
}

func writeTeardownResult(out io.Writer, plan *fp.Plan, kept []inventory.Kept, res *fp.Result, warnings []string, format string) error {
	rep := teardownReport{Mode: res.Mode, Kept: []teardownStep{}, Steps: []teardownStep{}, Warnings: append([]string{}, warnings...)}
	for _, k := range kept {
		r := k.Resource
		rep.Kept = append(rep.Kept, teardownStep{ARN: r.ARN, Type: r.Type, Region: r.Region, Name: r.Name, Outcome: "kept", Reason: k.Reason})
	}
	for i, a := range res.Actions {
		st := teardownStep{ARN: a.ARN, Type: a.Type, Name: a.Name, Outcome: string(a.Outcome), Reason: a.Reason}
		if i < len(plan.Steps) {
			st.Region = plan.Steps[i].Resource.Region
		}
		if a.Err != nil {
			st.Error = a.Err.Error()
			if errors.Is(a.Err, inventory.ErrInUse) {
				st.Reason = "in use since the plan was made"
			}
		}
		rep.Steps = append(rep.Steps, st)
	}
	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "OUTCOME\tREGION\tTYPE\tNAME\tREASON")
	line := func(s teardownStep) {
		reason := s.Reason
		if s.Error != "" {
			reason = strings.TrimSpace(reason + ": " + s.Error)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", s.Outcome, s.Region, strings.TrimPrefix(s.Type, "AWS::"), s.Name, reason)
	}
	for _, s := range rep.Kept {
		line(s)
	}
	for _, s := range rep.Steps {
		line(s)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	verb := "would delete"
	n := res.Count(fp.OutcomeWouldDelete)
	if res.Mode == fp.ModeReap {
		verb, n = "deleted", res.Count(fp.OutcomeDeleted)
	}
	_, _ = fmt.Fprintf(out, "\n%s: %s %d, kept %d in use, %d not deletable, %d failed.\n", res.Mode, verb, n,
		len(rep.Kept), res.Count(fp.OutcomeKept)+res.Count(fp.OutcomeRefused)+res.Count(fp.OutcomeSkipped),
		res.Count(fp.OutcomeFailed)+res.Count(fp.OutcomeStopped))
	for _, wn := range rep.Warnings {
		_, _ = fmt.Fprintf(out, "warning: %s\n", wn)
	}
	return nil
}
