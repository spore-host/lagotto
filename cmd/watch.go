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
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/quotacheck"
	"github.com/spore-host/lagotto/pkg/watcher"
	"github.com/spore-host/truffle/pkg/quotas"
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

If the pattern matches no instance type your region(s) offer, the first poll
prints a warning (a likely typo) but keeps watching in case the type appears.

With --service sagemaker, lagotto submits your SageMaker job (--sagemaker-config)
directly and retries it on CapacityError until SageMaker provisions it. SageMaker
has its own AWS-managed compute pool, so the attempt targets SageMaker itself.`,
	Args: cobra.ExactArgs(1),
	RunE: runWatch,
}

func init() {
	rootCmd.AddCommand(watchCmd)

	watchCmd.Flags().StringSliceVarP(&watchRegions, "regions", "r", nil, "Regions to watch (comma-separated; empty = all enabled). Widening across regions can break data co-location (cross-region egress) — prefer --azs within your data's region first.")
	watchCmd.Flags().StringSliceVar(&watchAZs, "azs", nil, "Availability zones to pin/order within the region(s), comma-separated (e.g. us-west-2b,us-west-2c). Empty = all AZs. AZ breadth is free (same-region data locality), so all AZs are tried each poll.")
	watchCmd.Flags().BoolVar(&watchSpot, "spot", false, "Watch for Spot capacity (default: On-Demand)")
	watchCmd.Flags().Float64Var(&watchMaxPrice, "max-price", 0, "Maximum acceptable price per hour (0 = any)")
	watchCmd.Flags().StringVar(&watchAction, "action", "notify", "Action on match: notify, spawn, hold")
	watchCmd.Flags().StringVar(&watchTTL, "ttl", "24h", "How long to keep watching (e.g., 24h, 7d)")
	watchCmd.Flags().StringSliceVar(&watchNotify, "notify", nil, "Notification channels (e.g., email:user@example.com, webhook:https://...)")
	watchCmd.Flags().StringVar(&watchSpawnConfig, "spawn-config", "", "spawn LaunchConfig YAML (required for --action spawn): a local path, an s3://bucket/key URI, or '-' for stdin. Any user_data_file / iam_policy_file it references is read now and stored inline, so a hosted poller can launch it with no access to this machine.")
	watchCmd.Flags().StringVar(&watchSageMakerConfig, "sagemaker-config", "", "YAML/JSON file with the SageMaker job definition (required for --service sagemaker)")
	watchCmd.Flags().StringVar(&watchService, "service", "ec2", "Capacity service: ec2, or sagemaker (submits your SageMaker job for ml.* types)")
	watchCmd.Flags().StringVar(&watchProject, "project", "", "Project label for scoping a local 'poll --daemon --project' in a shared account (default: $LAGOTTO_PROJECT)")
	watchCmd.Flags().IntVar(&watchMaintain, "maintain", 0, "Goal-driven fleet: maintain this many workers (relaunching toward the goal, even from zero) until --until holds. Requires --action spawn. 0 = single-shot (default).")
	watchCmd.Flags().StringVar(&watchUntil, "until", "", "Fleet completion condition, re-checked each poll: 's3-empty: s3://b/manifest minus s3://b/done/', 'http-200: https://…', or 'shell: <cmd>' (shell = local daemon only). When true the fleet retires.")
}

func runWatch(cmd *cobra.Command, args []string) error {
	pattern := args[0]
	ctx := context.Background()

	// Parse TTL
	ttl, err := time.ParseDuration(watchTTL)
	if err != nil {
		ttl, err = parseDuration(watchTTL)
		if err != nil {
			return fmt.Errorf("invalid TTL %q: %w", watchTTL, err)
		}
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

	// hold creates an EC2 On-Demand Capacity Reservation and has no SageMaker
	// equivalent; reject it for SageMaker watches.
	if service == watcher.ServiceSageMaker && action == watcher.ActionHold {
		return fmt.Errorf("--action hold is not supported for --service sagemaker (no capacity-reservation equivalent)")
	}

	// Load spawn config if action is spawn (EC2 watches).
	var launchConfigJSON []byte
	if service == watcher.ServiceEC2 && action == watcher.ActionSpawn {
		if watchSpawnConfig == "" {
			return fmt.Errorf("--spawn-config is required when --action=spawn")
		}
		read := watcher.NewConfigReader(func(ctx context.Context) (aws.Config, error) {
			return awscfg.Load(ctx, "")
		}, os.Stdin)
		launchConfigJSON, err = loadEC2SpawnConfig(ctx, read, watchSpawnConfig)
		if err != nil {
			return fmt.Errorf("load spawn config: %w", err)
		}
	}

	// Load the SageMaker job definition. lagotto submits this job on each attempt
	// and retries on CapacityError until SageMaker provisions it. Required unless
	// the user only wants to be notified.
	var sageMakerJobJSON []byte
	if service == watcher.ServiceSageMaker && action != watcher.ActionNotify {
		if watchSageMakerConfig == "" {
			return fmt.Errorf("--sagemaker-config is required for --service sagemaker (the job lagotto submits); use --action notify to only be told")
		}
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
	if watchUntil != "" && watcher.IsShellCondition(watchUntil) {
		fmt.Fprintln(os.Stderr, "note: shell completion conditions run only on the local 'poll --daemon' (the hosted poller has no shell sandbox).")
	}

	// Parse notify channels
	channels, err := parseNotifyChannels(watchNotify)
	if err != nil {
		return err
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

	now := time.Now().UTC()
	expiresAt := now.Add(ttl)

	project := watchProject
	if project == "" {
		project = os.Getenv("LAGOTTO_PROJECT")
	}

	w := &watcher.Watch{
		WatchID:             "w-" + uuid.New().String()[:8],
		UserID:              *identity.Arn,
		Project:             project,
		Status:              watcher.StatusActive,
		Service:             service,
		InstanceTypePattern: pattern,
		Regions:             watchRegions,
		AvailabilityZones:   watchAZs,
		Spot:                watchSpot,
		MaxPrice:            watchMaxPrice,
		Action:              action,
		NotifyChannels:      channels,
		LaunchConfigJSON:    launchConfigJSON,
		SageMakerJobJSON:    sageMakerJobJSON,
		DesiredCount:        watchMaintain,
		CompletionCondition: watchUntil,
		CreatedAt:           now,
		UpdatedAt:           now,
		ExpiresAt:           expiresAt,
		TTLTimestamp:        expiresAt.Unix(),
	}

	store := watcher.NewStore(cfg, watchesTable, historyTable)

	// Auto-create the backing tables on first use so the tool is zero-setup (#12).
	created, err := store.EnsureTables(ctx)
	if err != nil {
		return fmt.Errorf("ensure tables: %w", err)
	}
	for _, name := range created {
		fmt.Fprintf(os.Stderr, "Created DynamoDB table %s\n", name)
	}

	// Create-time feasibility check for a fleet watch (#153): tell the user NOW if
	// their --maintain goal exceeds the account's vCPU quota, instead of leaving
	// them to infer it from a fleet that never fills. Deliberately NON-FATAL and
	// without a --force escape hatch: truffle's GetQuotas swallows per-quota errors
	// (truffle#167), so "zero quota" isn't reliably distinguishable from "lookup
	// failed / no permission / throttled" — a fatal gate would break watch creation
	// for anyone lacking a read permission. (Opt-in strictness is #157.)
	quotaReport := warnFleetQuotaFeasibility(ctx, quotas.NewClientFromConfig(cfg), os.Stderr, w, cfg.Region)

	if err := store.PutWatch(ctx, w); err != nil {
		return fmt.Errorf("create watch: %w", err)
	}

	// Try to enable the polling schedule (best-effort; fails gracefully if not deployed)
	if err := enablePollingSchedule(ctx, cfg); err != nil && verbose {
		fmt.Fprintf(os.Stderr, "Note: could not enable polling schedule: %v\n", err)
	}

	if getOutputFormat() == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		if quotaReport != nil {
			// The quota report rides alongside the watch (the Report's own struct tags
			// do the rest), so a scripted caller can see the feasibility verdict that
			// was printed to stderr.
			return enc.Encode(struct {
				*watcher.Watch
				QuotaWarning *quotacheck.Report `json:"quota_warning,omitempty"`
			}{Watch: w, QuotaWarning: quotaReport})
		}
		return enc.Encode(w)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Created watch %s\n", w.WatchID)
	fmt.Fprintf(cmd.OutOrStdout(), "  Pattern:  %s\n", w.InstanceTypePattern)
	if w.Service == watcher.ServiceSageMaker {
		fmt.Fprintf(cmd.OutOrStdout(), "  Service:  sagemaker (submits your SageMaker job, retries on CapacityError)\n")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "  Regions:  %v\n", displayRegions(w.Regions))
	if len(w.AvailabilityZones) > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "  AZs:      %v\n", w.AvailabilityZones)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "  AZs:      all in region\n")
	}
	fmt.Fprintf(cmd.OutOrStdout(), "  Spot:     %v\n", w.Spot)
	if w.MaxPrice > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "  Max price: $%.4f/hr\n", w.MaxPrice)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "  Action:   %s\n", w.Action)
	if action == watcher.ActionSpawn && w.Service == watcher.ServiceEC2 {
		fmt.Fprintf(cmd.OutOrStdout(), "  Spawn config: %s\n", watchSpawnConfig)
	}
	if w.Service == watcher.ServiceSageMaker && len(w.SageMakerJobJSON) > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "  SageMaker config: %s\n", watchSageMakerConfig)
	}
	if quotaReport != nil && quotaReport.Known {
		fmt.Fprintf(cmd.OutOrStdout(), "  Quota:    %s\n", quotaSummaryRow(quotaReport, w))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "  Expires:  %s\n", w.ExpiresAt.Format(time.RFC3339))
	return nil
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
				fmt.Fprintf(out, "note: could not read EC2 service quotas in %s: %v\n", region, err)
			}
			continue
		}
		if !rep.Known {
			if verbose {
				fmt.Fprintf(out, "note: no EC2 %s-family quota figure available in %s "+
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
		fmt.Fprintf(out, "note: quota checked in the first %d of %d region(s) only.\n", maxQuotaCheckRegions, len(w.Regions))
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
		fmt.Fprintf(out, "warning: your EC2 %s-family %s vCPU quota in %s is 0 — this fleet cannot launch a single worker until it's raised.\n",
			rep.Family, lifecycle, region)
	case !rep.Feasible:
		fmt.Fprintf(out, "warning: this fleet may not reach --maintain %d: your EC2 %s-family %s vCPU quota in %s is %d vCPU with %d in use",
			w.DesiredCount, rep.Family, lifecycle, region, rep.Limit, rep.Used)
		if rep.MaxWorkers >= 0 {
			qualifier := ""
			if rep.MixedRungs {
				// Which rung matches isn't fixed at create time (#135 comma-list), so the
				// count is the most optimistic reading — name the rung it assumes.
				qualifier = "at best "
			}
			fmt.Fprintf(out, ", which fits %s%d × %s (%d vCPU each).\n",
				qualifier, rep.MaxWorkers, rep.SmallestRung, rep.VCPUsPerWorker)
		} else {
			// No literal rung in the pattern (a pure wildcard) — report the vCPU
			// headroom rather than invent a worker count.
			fmt.Fprintf(out, " (%d vCPU free), and %q has no literal size to count workers against.\n",
				rep.Limit-rep.Used, w.InstanceTypePattern)
		}
		fmt.Fprintf(out, "  The watch will still run: it maintains as many workers as quota allows, tells you when it hits the ceiling, and picks up a granted increase automatically.\n")
	default:
		return
	}
	if rep.Remediation != "" {
		fmt.Fprintf(out, "%s\n", rep.Remediation)
	}
}

// loadEC2SpawnConfig reads an EC2 --spawn-config YAML file, normalizes its keys
// (so snake_case / kebab-case / CamelCase all map), validates it parses into a
// SpawnConfigFile, and stores it as that struct's JSON so the spawner reads back
// exactly the fields it will launch with. This is what fixes the "settings
// silently dropped" gap (lagotto#19 issue #3): a key the struct doesn't know is
// surfaced here at watch-creation rather than ignored at launch.
func loadEC2SpawnConfig(ctx context.Context, read watcher.ConfigReader, source string) ([]byte, error) {
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
	// Resolve any user_data_file / iam_policy_file references to inline content
	// now, on the creating machine, so the stored watch is self-contained and a
	// hosted poller with no access to this filesystem can launch it (#132, #140).
	if err := cfg.MakeSelfContained(ctx, read); err != nil {
		return nil, fmt.Errorf("resolve spawn config references: %w", err)
	}
	jsonBytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal spawn config: %w", err)
	}
	return jsonBytes, nil
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

// enablePollingSchedule re-enables the EventBridge schedule if it was self-disabled.
func enablePollingSchedule(ctx context.Context, cfg aws.Config) error {
	scheduleName := os.Getenv("LAGOTTO_SCHEDULE_NAME")
	if scheduleName == "" {
		scheduleName = "lagotto-capacity-poller"
	}

	client := scheduler.NewFromConfig(cfg)

	current, err := client.GetSchedule(ctx, &scheduler.GetScheduleInput{
		Name: aws.String(scheduleName),
	})
	if err != nil {
		// Schedule doesn't exist (not deployed yet) — that's fine
		return nil
	}

	if current.State == schedulertypes.ScheduleStateDisabled {
		_, err = client.UpdateSchedule(ctx, &scheduler.UpdateScheduleInput{
			Name:               current.Name,
			ScheduleExpression: current.ScheduleExpression,
			FlexibleTimeWindow: current.FlexibleTimeWindow,
			Target:             current.Target,
			State:              schedulertypes.ScheduleStateEnabled,
		})
		if err != nil {
			return fmt.Errorf("enable schedule: %w", err)
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "Enabled polling schedule %s\n", scheduleName)
		}
	}
	return nil
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
		case "email", "sns":
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
