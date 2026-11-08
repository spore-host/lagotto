package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	"github.com/spore-host/lagotto/pkg/watcher"
	"gopkg.in/yaml.v3"
)

var (
	watchRegions         []string
	watchAZs             []string
	watchSpot            bool
	watchMaxPrice        float64
	watchAction          string
	watchTTL             string
	watchNotify          []string
	watchSpawnConfig     string
	watchSageMakerConfig string
	watchService         string
	watchProject         string
	watchMaintain        int
	watchUntil           string
	watchUntilLanded     bool
	watchRemind          string
	watchYes             bool
	watchRequireQuota    bool
	watchGroup           string
	watchGroupOnExtra    string
)

var watchCmd = &cobra.Command{
	Use:   "watch <instance-type-pattern>",
	Short: "Create a capacity watch for an instance type",
	Long: `Watch for instance availability across regions and AZs.

The pattern supports wildcards ("p5.*" matches all p5 sizes), exact types
("g5.xlarge"), and comma-separated lists ("g6.4xlarge,g6.2xlarge,g6.xlarge") that
match ANY of the listed rungs. lagotto attempts to launch the requested instance
and retries until it succeeds or the watch TTL expires — the launch itself is the
capacity test (neither EC2 nor SageMaker exposes a capacity API).

With --until-landed (--action spawn or hold) there is no TTL: the watch hunts
until the launch is running or the reservation is active. It is still bounded:
it pauses itself unless you run 'lagotto ack <watch-id>' within 3 × --remind
(21 days by default) of creation or the last ack, and 'lagotto ack' resumes it.

A --maintain fleet whose goal exceeds the account's EC2 vCPU quota gets a
warning, and is still created. With --require-quota it is refused instead (exit
status 3), but only when the quota was read in every --regions region and
provably can't hold the goal; an unreadable quota is unknown, not zero, so the
watch is then created with a note saying it could not be checked.

--action notify is deprecated (lagotto#283) and will be removed in a later
release, and so is the notify default when no --action is given. A notify
watch only checks that the type is offered in the region at or under
--max-price, not a capacity check: it launches and reserves nothing. It still
works, with a warning on stderr. Use --action hold (reserve the capacity) or
--action spawn (launch it) to wait for real capacity.

If the pattern matches no instance type your region(s) offer, the first poll
prints a warning (a likely typo) but keeps watching in case the type appears.

With --service sagemaker, lagotto submits your SageMaker job (--sagemaker-config)
directly and retries it on CapacityError until SageMaker provisions it. SageMaker
has its own AWS-managed compute pool, so the attempt targets SageMaker itself.
It needs --action spawn: SageMaker has no capacity signal short of submitting the
job, so there is nothing for --action notify to fire on. Add --notify to be told
when the job is provisioned, or that it was not (a rejected submit or a
non-capacity job failure stops the watch as failed).`,
	Args: cobra.ExactArgs(1),
	// A refused watch is a finding, not a usage error (same reasoning as
	// doctor's SilenceUsage). The #170 instance-role refusal is a multi-line
	// explanation with a remediation command in it; printing the full flag block
	// above that buries the one thing the user needs to read. Cobra still prints
	// usage for genuine flag misuse, which is parsed before RunE.
	SilenceUsage: true,
	RunE:         runWatch,
}

func init() {
	rootCmd.AddCommand(watchCmd)

	watchCmd.Flags().StringSliceVarP(&watchRegions, "regions", "r", nil, "Regions to watch (comma-separated; empty = all enabled). Widening across regions can break data co-location (cross-region egress) — prefer --azs within your data's region first.")
	watchCmd.Flags().StringSliceVar(&watchAZs, "azs", nil, "Availability zones to pin/order within the region(s), comma-separated (e.g. us-west-2b,us-west-2c). Empty = all AZs. AZ breadth is free (same-region data locality), so all AZs are tried each poll.")
	watchCmd.Flags().BoolVar(&watchSpot, "spot", false, "Watch for Spot capacity (default: On-Demand)")
	watchCmd.Flags().Float64Var(&watchMaxPrice, "max-price", 0, "Maximum acceptable price per hour (0 = any)")
	watchCmd.Flags().StringVar(&watchAction, "action", "notify", "Action on match: spawn (launch it), hold (reserve it), or notify (deprecated: offering only, not a capacity check). The notify default is deprecated too: pass --action explicitly")
	watchCmd.Flags().StringVar(&watchTTL, "ttl", "24h", "How long to keep watching (e.g., 24h, 7d)")
	watchCmd.Flags().StringSliceVar(&watchNotify, "notify", nil, "Notification channels (e.g., email:user@example.com, webhook:https://..., sns:<topic-arn>). An email address is subscribed to the alerts topic and must confirm by clicking the link in the AWS Notification email before anything is delivered.")
	watchCmd.Flags().StringVar(&watchSpawnConfig, "spawn-config", "", "lagotto spawn-config YAML (required for --action spawn; not spawn launch --config, see README for its keys — unknown keys are rejected): a local path, an s3://bucket/key URI, or '-' for stdin. Any user_data_file / iam_policy_file it references is read now and stored inline, so a hosted poller can launch it with no access to this machine.")
	watchCmd.Flags().StringVar(&watchSageMakerConfig, "sagemaker-config", "", "YAML/JSON file with the SageMaker job definition (required for --service sagemaker)")
	watchCmd.Flags().StringVar(&watchService, "service", "ec2", "Capacity service: ec2, or sagemaker (submits your SageMaker job for ml.* types)")
	watchCmd.Flags().StringVar(&watchProject, "project", "", "Project label for scoping a local 'poll --daemon --project' in a shared account (default: $LAGOTTO_PROJECT)")
	watchCmd.Flags().IntVar(&watchMaintain, "maintain", 0, "Goal-driven fleet: maintain this many workers (relaunching toward the goal, even from zero) until --until holds. Requires --action spawn and --regions. 0 = single-shot (default).")
	watchCmd.Flags().BoolVar(&watchUntilLanded, "until-landed", false, "Keep hunting with no TTL until the launch (--action spawn) or hold lands. The watch pauses itself unless you run 'lagotto ack <id>' within (2+1) × --remind (21d by default). Needs a --notify channel; not with --ttl, --maintain, --until, or --action notify.")
	watchCmd.Flags().StringVar(&watchRemind, "remind", "7d", "With --until-landed: the reminder interval (1h to 30d). You are reminded after each of the first 2 intervals and warned 24h before the pause; the watch pauses 3 × this after creation or the last ack.")
	watchCmd.Flags().BoolVarP(&watchYes, "yes", "y", false, "Skip the confirmation prompt that --until-landed --action spawn/hold shows (required when stdin is not a terminal)")
	watchCmd.Flags().BoolVar(&watchRequireQuota, "require-quota", false, "With --maintain: refuse to create the watch (exit status 3) when the account's EC2 vCPU quota, read in every --regions region, provably can't hold --maintain N workers. A quota that can't be read is unknown, not zero: the watch is then created anyway, with a note.")
	watchCmd.Flags().StringVar(&watchGroup, "group", "", "Join (or create) a watch group: the first member to land wins and every other member is cancelled. Members may differ in pattern, regions, Spot and spawn config; --action spawn or hold only, not with --maintain, --until-landed or --service sagemaker. 'lagotto status <group>' and 'lagotto cancel <group>' act on the whole group.")
	watchCmd.Flags().StringVar(&watchGroupOnExtra, "group-on-extra", "", "With --group: what happens to this member's launch if it lands after the group was already won: terminate (default; a hold's reservation is released) or keep")
	watchCmd.Flags().StringVar(&watchUntil, "until", "", "Fleet completion condition, re-checked each poll: 's3-empty: s3://b/manifest minus s3://b/done/', 'http-200: https://…', or 'shell: <cmd>' (shell = local daemon only). When true the fleet retires.")
}

func runWatch(cmd *cobra.Command, args []string) error {
	pattern := args[0]
	ctx := context.Background()

	// Parse TTL
	ttl, err := parseTTLFlag(watchTTL)
	if err != nil {
		return err
	}
	ul, err := parseUntilLandedFlags(watchUntilLanded, watchRemind, cmd.Flags().Changed("ttl"),
		cmd.Flags().Changed("remind"), cmd.Flags().Changed("maintain"), cmd.Flags().Changed("until"))
	if err != nil {
		return err
	}

	// Validate service and that the pattern fits it (ml.* iff sagemaker).
	service := watcher.Service(watchService)
	if !service.Valid() {
		return fmt.Errorf("invalid service %q: must be ec2 or sagemaker", watchService)
	}
	if err := watcher.ValidateWatchPattern(service, pattern); err != nil {
		return err
	}

	// Validate action
	action := watcher.ActionMode(watchAction)
	switch action {
	case watcher.ActionNotify, watcher.ActionSpawn, watcher.ActionHold:
	default:
		return fmt.Errorf("invalid action %q: must be notify, spawn, or hold", watchAction)
	}

	if err := validateServiceFlags(service, action, cmd.Flags().Changed("action"), watchSageMakerConfig); err != nil {
		return err
	}

	// Load spawn config if action is spawn (EC2 watches).
	var launchConfigJSON []byte
	if service == watcher.ServiceEC2 && action == watcher.ActionSpawn {
		if watchSpawnConfig == "" {
			return errSpawnConfigRequired
		}
		launchConfigJSON, err = readSpawnConfigFlag(ctx, watchSpawnConfig)
		if err != nil {
			return err
		}
	}

	// Load the SageMaker job definition. lagotto submits this job on each attempt
	// and retries on CapacityError until SageMaker provisions it. Always required:
	// submitting the job is the only thing a SageMaker watch can do (#195).
	var sageMakerJobJSON []byte
	if service == watcher.ServiceSageMaker {
		sageMakerJobJSON, err = loadSageMakerConfig(watchSageMakerConfig)
		if err != nil {
			return fmt.Errorf("load sagemaker config: %w", err)
		}
	}

	// Goal-driven fleet validation (#70): --maintain/--until make this a fleet
	// watch, which only makes sense for the spawn action (it launches workers).
	if err := validateFleetFlags(action, watchMaintain, watchUntil); err != nil {
		return err
	}
	// Also checked by validateWatchSearch below; here too so the refusal comes
	// before any AWS call (#275).
	if err := validateFleetRegions(watchMaintain, watchRegions); err != nil {
		return err
	}
	if watchRequireQuota && watchMaintain <= 0 {
		return errRequireQuotaNeedsFleet
	}
	if watchUntil != "" && watcher.IsShellCondition(watchUntil) {
		fmt.Fprintln(os.Stderr, "note: shell completion conditions run only on the local 'poll --daemon' (the hosted poller has no shell sandbox).")
	}

	// Watch groups (#287): refused before any AWS call when the combination
	// can't be a member. Checked again on the built watch below.
	groupOnExtra, err := validateGroupFlags(watchGroup, watchGroupOnExtra, cmd.Flags().Changed("group-on-extra"),
		&watcher.Watch{Group: watchGroup, Service: service, Action: action, DesiredCount: watchMaintain, UntilLanded: ul.enabled})
	if err != nil {
		return err
	}

	// Parse notify channels
	channels, err := parseNotifyChannels(watchNotify)
	if err != nil {
		return err
	}

	// Until-landed (#239): no TTL, a rolling acknowledgement deadline instead.
	// Validated and confirmed before any AWS call, so a refusal costs nothing.
	if ul.enabled {
		if err := watcher.ValidateUntilLanded(action, watchMaintain, channels, ul.remindEvery, watcher.DefaultRemindPauseAfter); err != nil {
			return err
		}
		exp := cliExposure(&watcher.Watch{Service: service, Action: action, InstanceTypePattern: pattern,
			Regions: watchRegions, Spot: watchSpot, MaxPrice: watchMaxPrice, DesiredCount: watchMaintain})
		if err := confirmUntilLanded(os.Stdin, os.Stderr, stdinIsTerminal(), watchYes, action, pattern, ul, exp); err != nil {
			return err
		}
	}

	// Get caller identity for user_id
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	stsClient := sts.NewFromConfig(cfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("get caller identity: %w", err)
	}

	// The ID, timestamps and expiry (an until-landed watch's first
	// acknowledgement deadline) come from watcher.NewWatch, the one place
	// ExpiresAt and the record's retention TTL are set together (#161, #266).
	life := watcher.Lifetime{TTL: ttl}
	if ul.enabled {
		life = watcher.Lifetime{UntilLanded: true, RemindEvery: ul.remindEvery, RemindPauseAfter: ul.pauseAfter}
	}
	w, err := watcher.NewWatch(time.Now(), life)
	if err != nil {
		return err
	}

	project := watchProject
	if project == "" {
		project = os.Getenv("LAGOTTO_PROJECT")
	}

	w.UserID = *identity.Arn
	w.Project = project
	w.Service = service
	w.InstanceTypePattern = pattern
	w.Regions = watchRegions
	w.AvailabilityZones = watchAZs
	w.Spot = watchSpot
	w.MaxPrice = watchMaxPrice
	w.Action = action
	w.NotifyChannels = channels
	w.LaunchConfigJSON = launchConfigJSON
	w.SageMakerJobJSON = sageMakerJobJSON
	w.DesiredCount = watchMaintain
	w.CompletionCondition = watchUntil
	w.Group = watchGroup
	w.GroupOnExtra = groupOnExtra
	if err := watcher.ValidateGroupMember(w); err != nil {
		return err
	}

	// Everything `update` re-checks (#257) goes through these two, so an update
	// cannot store a watch that this command would refuse.
	if err := validateWatchSearch(w); err != nil {
		return err
	}
	// Default-subnet coverage (#256), the fleet quota warning (#153) and its
	// opt-in --require-quota gate (#157), and the instance-role check (#170),
	// before anything is created.
	pre, err := preflightWatch(ctx, cfg, os.Stderr, w, watchRequireQuota)
	if err != nil {
		return err
	}
	quotaReport := pre.Quota

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// Auto-create the backing tables on first use so the tool is zero-setup (#12).
	created, err := store.EnsureTables(ctx)
	if err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}
	for _, name := range created {
		fmt.Fprintf(os.Stderr, "Created DynamoDB table %s\n", name)
	}

	// Store the watch and make sure the hosted poller will poll it (#231): a
	// caller who cannot read or switch on the poller's schedule is refused, and
	// no active watch is left behind for nothing to poll. See pollschedule.go.
	sched := scheduler.NewFromConfig(cfg)
	armAction := "create watch " + w.WatchID
	pollSched, err := pollSchedulePreflight(ctx, sched, armAction)
	if err != nil {
		return err
	}
	// Subscribe each email address to the alerts topic (#240), before the watch
	// is stored, so an address that cannot be subscribed refuses the watch
	// instead of arming one whose emails reach no one.
	if err := subscribeForNewWatch(ctx, os.Stderr, cfg, os.Getenv, aws.ToString(identity.Account), deploy.New(cfg), w); err != nil {
		return err
	}
	warnUndeliverableUntilLanded(os.Stderr, w)
	warnIfPollerPredatesReminders(ctx, cfg, os.Stderr, w)
	// --action notify is deprecated (#283). Stderr, after every refusal, so a
	// refused watch is not also lectured and stdout / -o json stay clean.
	deprecations := watcher.ActionDeprecations(w.Action, cmd.Flags().Changed("action"))
	writeDeprecations(cmd.ErrOrStderr(), deprecations)
	// Join the group before the watch is stored (#287), so a member is never
	// polled without its group: a refused join stores nothing.
	var group *watcher.WatchGroup
	if w.Group != "" {
		group, err = joinWatchGroup(ctx, store, w)
		if err != nil {
			return err
		}
		warnIfPollerPredatesGroups(ctx, cfg, os.Stderr, w)
	}
	if err := store.PutWatch(ctx, w); err != nil {
		return fmt.Errorf("create watch: %w", err)
	}
	if err := enablePollScheduleIfOff(ctx, sched, os.Stderr, pollSched); err != nil {
		// Roll back through PutWatch, the call (and dynamodb:PutItem permission)
		// that just created it, so the rollback needs nothing the caller lacks.
		w.Status = watcher.StatusCancelled
		return pollScheduleUpdateRefusal(armAction, "no watch was created", store.PutWatch(ctx, w), err)
	}

	cliExposure(w)
	if getOutputFormat() == "json" {
		return encodeWatchJSON(cmd.OutOrStdout(), w, pre, deprecations)
	}

	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Created watch %s\n", w.WatchID)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Pattern:  %s\n", w.InstanceTypePattern)
	if w.Service == watcher.ServiceSageMaker {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Service:  sagemaker (submits your SageMaker job, retries on CapacityError)\n")
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Regions:  %v\n", displayRegions(w.Regions))
	if len(w.AvailabilityZones) > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  AZs:      %v\n", w.AvailabilityZones)
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  AZs:      all in region\n")
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Spot:     %v\n", w.Spot)
	if w.MaxPrice > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Max price: $%.4f/hr\n", w.MaxPrice)
	}
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Action:   %s\n", w.Action)
	if group != nil {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Group:    %s\n", groupJoinedLine(group, w))
	}
	writeExposure(cmd.OutOrStdout(), "  Exposure: ", w.Exposure)
	if action == watcher.ActionSpawn && w.Service == watcher.ServiceEC2 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Spawn config: %s\n", watchSpawnConfig)
	}
	if w.Service == watcher.ServiceSageMaker && len(w.SageMakerJobJSON) > 0 {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  SageMaker config: %s\n", watchSageMakerConfig)
	}
	if quotaReport != nil && quotaReport.Known {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Quota:    %s\n", quotaSummaryRow(quotaReport, w))
	}
	if pre.Gate != nil {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Quota gate: %s (--require-quota)\n", pre.Gate.Verdict)
	}
	if w.UntilLanded {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Expires:  until landed; pauses %s unless acknowledged (lagotto ack %s)\n",
			w.ExpiresAt.Format(time.RFC3339), w.WatchID)
	} else {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Expires:  %s\n", w.ExpiresAt.Format(time.RFC3339))
	}
	writeEmailStates(cmd.OutOrStdout(), "  Notify:   ", w)
	return nil
}

// encodeWatchJSON is `watch -o json`'s output: the watch, with the quota
// report, the quota gate, the instance-role verdict and any deprecations riding
// alongside it (their own struct tags do the rest), so a scripted caller can
// see what was printed to stderr.
func encodeWatchJSON(out io.Writer, w *watcher.Watch, pre preflightResult, deprecations []watcher.Deprecation) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if pre.Quota == nil && pre.Role == nil && pre.Gate == nil && len(deprecations) == 0 {
		return enc.Encode(w)
	}
	return enc.Encode(struct {
		*watcher.Watch
		QuotaWarning *quotacheck.Report    `json:"quota_warning,omitempty"`
		QuotaGate    *quotacheck.Gate      `json:"quota_gate,omitempty"`
		InstanceRole *InstanceRoleCheck    `json:"instance_role_check,omitempty"`
		Deprecations []watcher.Deprecation `json:"deprecations,omitempty"`
	}{Watch: w, QuotaWarning: pre.Quota, QuotaGate: pre.Gate, InstanceRole: pre.Role, Deprecations: deprecations})
}

// writeDeprecations prints each deprecation as a warning, to errOut (stderr):
// stdout and -o json carry none of it, the JSON output has a "deprecations"
// field instead.
func writeDeprecations(errOut io.Writer, deprecations []watcher.Deprecation) {
	for _, d := range deprecations {
		_, _ = fmt.Fprintf(errOut, "warning: %s\n", strings.ReplaceAll(d.Message, "\n", "\n  "))
	}
}

// quotaSummaryRow is the one-line "Quota:" row echoed in the human watch-create
// summary, so the feasibility answer appears next to the fleet size even when the
// stderr warning scrolls past.
func quotaSummaryRow(rep *quotacheck.Report, w *watcher.Watch) string {
	lifecycle := "on-demand"
	if w.Spot {
		lifecycle = "spot"
	}
	head := fmt.Sprintf("%s-family %s: %d vCPU limit, %d in use", rep.Family, lifecycle, rep.Limit, rep.Used)
	if rep.MaxWorkers < 0 {
		return head
	}
	verdict := "fits the fleet"
	if !rep.Feasible {
		verdict = fmt.Sprintf("short of --maintain %d", w.DesiredCount)
	}
	return fmt.Sprintf("%s → room for %d × %s (%s)", head, rep.MaxWorkers, rep.SmallestRung, verdict)
}

// maxQuotaCheckRegions bounds how many regions the create-time check queries. Each
// GetQuotas is ~16 GetServiceQuota calls plus 2 DescribeInstances, and an empty
// --regions means ALL enabled regions — sweeping those at watch-create would turn
// a convenience warning into a ~500-call stall.
const maxQuotaCheckRegions = 3

// warnFleetQuotaFeasibility warns (to stderr, non-fatally) when a --maintain
// fleet's goal doesn't fit the account's EC2 vCPU quota (#153), and returns the
// report for the JSON output. Returns nil when there's nothing to say: not a
// fleet watch, no lookup, or a lookup that produced no usable numbers.
//
// Output goes to STDERR on purpose so it survives `-o json`. fallbackRegion is the
// caller's configured region, used when --regions is empty (which means "all
// enabled regions" at poll time — never swept here).
func warnFleetQuotaFeasibility(ctx context.Context, l quotacheck.Lookup, out io.Writer, w *watcher.Watch, fallbackRegion string) *quotacheck.Report {
	if w == nil || w.DesiredCount <= 0 || l == nil {
		return nil
	}
	regions := w.Regions
	if len(regions) == 0 && fallbackRegion != "" {
		regions = []string{fallbackRegion}
	}
	if len(regions) == 0 {
		return nil
	}
	truncated := false
	if len(regions) > maxQuotaCheckRegions {
		regions = regions[:maxQuotaCheckRegions]
		truncated = true
	}

	rungs := watcher.SplitInstanceTypePatterns(w.InstanceTypePattern)
	if len(rungs) == 0 {
		return nil
	}

	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var first *quotacheck.Report
	for _, region := range regions {
		rep, err := quotacheck.Feasibility(cctx, l, quotacheck.Request{
			Region:  region,
			Rungs:   rungs,
			Spot:    w.Spot,
			Desired: w.DesiredCount,
		})
		if err != nil {
			// A failed lookup is NOT a quota problem — stay quiet (verbose-only) so a
			// missing read permission never reads as "your quota is zero".
			if verbose {
				_, _ = fmt.Fprintf(out, "note: could not read EC2 service quotas in %s: %v\n", region, err)
			}
			continue
		}
		if !rep.Known {
			if verbose {
				_, _ = fmt.Fprintf(out, "note: no EC2 %s-family quota figure available in %s "+
					"(needs servicequotas:GetServiceQuota); the watch still reports a quota ceiling if it hits one.\n",
					rep.Family, region)
			}
			continue
		}
		if first == nil {
			r := rep
			first = &r
		}
		printQuotaFeasibility(out, w, region, &rep)
	}
	if truncated && first != nil {
		_, _ = fmt.Fprintf(out, "note: quota checked in the first %d of %d region(s) only.\n", maxQuotaCheckRegions, len(w.Regions))
	}
	return first
}

// printQuotaFeasibility writes the human warning for one region's report. The
// zero-quota case is the loudest; an infeasible-but-nonzero quota is a warning
// that also says what the watch WILL do; a feasible fleet says nothing.
func printQuotaFeasibility(out io.Writer, w *watcher.Watch, region string, rep *quotacheck.Report) {
	lifecycle := "on-demand"
	if w.Spot {
		lifecycle = "spot"
	}
	switch {
	case rep.ZeroQuota:
		_, _ = fmt.Fprintf(out, "warning: your EC2 %s-family %s vCPU quota in %s is 0 — this fleet cannot launch a single worker until it's raised.\n",
			rep.Family, lifecycle, region)
	case !rep.Feasible:
		_, _ = fmt.Fprintf(out, "warning: this fleet may not reach --maintain %d: your EC2 %s-family %s vCPU quota in %s is %d vCPU with %d in use",
			w.DesiredCount, rep.Family, lifecycle, region, rep.Limit, rep.Used)
		if rep.MaxWorkers >= 0 {
			qualifier := ""
			if rep.MixedRungs {
				// Which rung matches isn't fixed at create time (#135 comma-list), so the
				// count is the most optimistic reading — name the rung it assumes.
				qualifier = "at best "
			}
			_, _ = fmt.Fprintf(out, ", which fits %s%d × %s (%d vCPU each).\n",
				qualifier, rep.MaxWorkers, rep.SmallestRung, rep.VCPUsPerWorker)
		} else {
			// No literal rung in the pattern (a pure wildcard) — report the vCPU
			// headroom rather than invent a worker count.
			_, _ = fmt.Fprintf(out, " (%d vCPU free), and %q has no literal size to count workers against.\n",
				rep.Limit-rep.Used, w.InstanceTypePattern)
		}
		_, _ = fmt.Fprintf(out, "  The watch will still run: it maintains as many workers as quota allows, tells you when it hits the ceiling, and picks up a granted increase automatically.\n")
	default:
		return
	}
	if rep.Remediation != "" {
		_, _ = fmt.Fprintf(out, "%s\n", rep.Remediation)
	}
}

// loadEC2SpawnConfig reads an EC2 --spawn-config YAML file, normalizes its keys
// (so snake_case / kebab-case / CamelCase all map), validates it parses into a
// SpawnConfigFile, and stores it as that struct's JSON so the spawner reads back
// exactly the fields it will launch with. Normalization fixed the "settings
// silently dropped" gap for real keys in another spelling (lagotto#19 issue #3);
// ParseSpawnConfigYAML also rejects a key that matches no field, naming it and
// the nearest known key (#202), so a mistyped or unsupported setting fails here
// at watch-creation rather than being ignored at launch. Stored configs are
// re-read tolerantly at match time (see watcher.Spawner.Spawn).
func loadEC2SpawnConfig(ctx context.Context, read watcher.ConfigReader, source string) ([]byte, error) {
	cfg, err := loadEC2SpawnConfigFile(ctx, read, source)
	if err != nil {
		return nil, err
	}
	jsonBytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal spawn config: %w", err)
	}
	return jsonBytes, nil
}

// loadEC2SpawnConfigFile is loadEC2SpawnConfig before the final marshal, for a
// caller that adjusts the config first (`lagotto launch --reservation-id`, #210).
func loadEC2SpawnConfigFile(ctx context.Context, read watcher.ConfigReader, source string) (*watcher.SpawnConfigFile, error) {
	data, err := read(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", source, err)
	}
	cfg, err := watcher.ParseSpawnConfigYAML(data)
	if err != nil {
		return nil, err
	}
	if cfg.InstanceType == "" && cfg.AMI == "" {
		// A spawn config with neither an instance type nor an AMI is almost
		// certainly a mis-keyed file (e.g. the user wrote `instancetype:` under a
		// nested block). The instance type is overridden by the matched type at
		// launch, so it's not strictly required — but flag the empty-shell case.
		return nil, fmt.Errorf("spawn config %s has no recognized fields (check key names: instance_type, on_complete, pre_stop, command, …)", source)
	}
	// Guarantee a TTL on the eventual launch (#38): default an empty one to 24h
	// and reject a malformed one now, at watch-create, so the stored config can
	// never produce an instance with no death clock.
	if err := cfg.ValidateAndDefaultTTL(); err != nil {
		return nil, err
	}
	// fsx_create's backing bucket must be one the hosted poller is allowed to
	// create and tag (#242): refuse it here, before the watch is stored.
	if err := cfg.ValidateFSxBucket(); err != nil {
		return nil, err
	}
	// Resolve any user_data_file / iam_policy_file references to inline content
	// now, on the creating machine, so the stored watch is self-contained and a
	// hosted poller with no access to this filesystem can launch it (#132, #140).
	if err := cfg.MakeSelfContained(ctx, read); err != nil {
		return nil, fmt.Errorf("resolve spawn config references: %w", err)
	}
	return cfg, nil
}

// validateServiceFlags is the service/action part of `watch`'s validation,
// pure so it can be tested without reaching runWatch's AWS calls. SageMaker
// supports only --action spawn (submit the job): hold has no SageMaker
// equivalent, and notify has no capacity signal to fire on (#195). notify is
// the default --action, so the refusal says so when the user didn't pick it.
// The job definition is always required for SageMaker; the old error here
// recommended --action notify, the one mode that never tells anyone.
func validateServiceFlags(service watcher.Service, action watcher.ActionMode, actionSet bool, sageMakerConfig string) error {
	if err := watcher.ValidateServiceAction(service, action); err != nil {
		if action == watcher.ActionNotify && !actionSet {
			return fmt.Errorf("%w (notify is the default --action)", err)
		}
		return err
	}
	if service == watcher.ServiceSageMaker && sageMakerConfig == "" {
		return fmt.Errorf("--sagemaker-config is required for --service sagemaker: it is the job lagotto submits and retries until SageMaker provisions it (add --notify to be told when it does)")
	}
	return nil
}

// loadSageMakerConfig reads a SageMaker job YAML file and converts it to JSON
// bytes for storage. Unlike loadEC2SpawnConfig, it deliberately does NOT apply
// the EC2 spawn-config key normalization or TTL defaulting (#41): a SageMaker
// config is a CreateTrainingJobInput-shaped document with an entirely different
// schema (TrainingJobName, AlgorithmSpecification, ResourceConfig, …), not
// spawn's instance_type/on_complete/ttl fields — normalizing its keys would
// corrupt them. SageMaker validates the job shape server-side at
// CreateTrainingJob (see pkg/watcher/sagemaker.go), and StoppingCondition bounds
// the runtime there, so this loader only checks the YAML is well-formed and
// passes it through.
func loadSageMakerConfig(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	// Validate the YAML is well-formed (the SageMaker API validates the job
	// contents at submit time).
	var configMap map[string]interface{}
	if err := yaml.Unmarshal(data, &configMap); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}

	// Re-marshal as JSON for DynamoDB storage.
	jsonBytes, err := json.Marshal(configMap)
	if err != nil {
		return nil, fmt.Errorf("marshal to JSON: %w", err)
	}
	return jsonBytes, nil
}

func displayRegions(regions []string) string {
	if len(regions) == 0 {
		return "(all enabled)"
	}
	return fmt.Sprintf("%v", regions)
}

// validateFleetFlags checks the goal-driven fleet flags (#70): --maintain must
// be non-negative and, when set, requires --action spawn; --until requires
// --maintain and must be a well-formed completion spec. A nil-client ParseCondition
// on s3-empty reports "needs an S3 client" — that means the spec parsed fine, so
// it's not a validation error here (the poller supplies the client at run time).
func validateFleetFlags(action watcher.ActionMode, maintain int, until string) error {
	if maintain < 0 {
		return fmt.Errorf("--maintain must be >= 0")
	}
	if maintain > 0 && action != watcher.ActionSpawn {
		return fmt.Errorf("--maintain requires --action spawn (a fleet maintains launched workers)")
	}
	if until != "" && maintain == 0 {
		return fmt.Errorf("--until requires --maintain (it's the fleet's completion condition)")
	}
	if until != "" {
		if _, err := watcher.ParseCondition(until, nil); err != nil && !strings.Contains(err.Error(), "needs an S3 client") {
			return fmt.Errorf("invalid --until: %w", err)
		}
	}
	return nil
}

func parseNotifyChannels(raw []string) ([]watcher.NotifyChannel, error) {
	var channels []watcher.NotifyChannel
	for _, s := range raw {
		parts := splitFirst(s, ':')
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid notify format %q: expected type:target (e.g., email:user@example.com)", s)
		}
		ch := watcher.NotifyChannel{Type: parts[0], Target: parts[1]}
		switch ch.Type {
		case "email":
			// Stored normalized: the address is the subscription's filter value
			// and the publish attribute (#240), so one spelling throughout.
			if err := watcher.ValidateEmail(ch.Target); err != nil {
				return nil, err
			}
			ch.Target = watcher.NormalizeEmail(ch.Target)
		case "sns":
		case "webhook":
			if err := watcher.ValidateWebhookURL(ch.Target); err != nil {
				return nil, fmt.Errorf("invalid webhook URL: %w", err)
			}
		default:
			return nil, fmt.Errorf("invalid notify type %q: must be email, webhook, or sns", ch.Type)
		}
		channels = append(channels, ch)
	}
	return channels, nil
}

func splitFirst(s string, sep byte) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}

// parseDuration parses lagotto's short duration form ("7d"/"24h"/"30m"). The
// implementation now lives in pkg/watcher so the spawner can share it (#38);
// this thin wrapper keeps the existing cmd call sites + tests unchanged.
func parseDuration(s string) (time.Duration, error) {
	return watcher.ParseDuration(s)
}
