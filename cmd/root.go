package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/libs/i18n"
)

var (
	// Version is set via ldflags at build time
	Version = "dev"

	// Global flags
	outputFormat string
	verbose      bool
	watchesTable string
	historyTable string

	// Shared spore.host config flags (see libs/sporeconfig). The profile and
	// region go to pkg/awscfg; the account is a guard checked in
	// PersistentPreRunE (see account.go).
	sharedProfile string
	sharedRegion  string
	sharedAccount string

	// i18n flags
	flagLang          string
	flagNoEmoji       bool
	flagAccessibility bool
)

var rootCmd = &cobra.Command{
	Use:         "lagotto",
	Annotations: level(levelBasic),
	// Short and Long set after i18n initialization
}

var i18nInitialized = false

// Execute runs the root command.
func Execute() {
	_ = rootCmd.ParseFlags(os.Args[1:])
	ensureI18nInitialized()

	// SIGINT and SIGTERM cancel the command's context (commandContext): a
	// command holding a teardown lease gives it back, or suspends it, and a
	// creator ends its marks, each with a fresh context of its own, before
	// it exits. A second signal exits at once.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); stop() }()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitStatus(err))
	}
}

// exitStatus is the process exit status for a command error: the error's own
// ExitCode when it has one (the --require-quota refusal, #157), else 1.
func exitStatus(err error) int {
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return 1
}

func init() {
	// Cobra prints "Error: <err>" itself, and [Execute] also prints the error and
	// sets the exit code deliberately — so without this every failure was reported
	// TWICE. Harmless-looking for a one-line error, but #170's instance-role
	// refusal is a seven-line explanation with a remediation command in it, and
	// printing that twice makes it read as output noise rather than something to
	// act on. Set here rather than in Execute so it is a property of the command.
	rootCmd.SilenceErrors = true

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		ensureI18nInitialized()
		// Record shared-config flag values for pkg/awscfg (flag > env > file > default).
		awscfg.SetFlags(sharedProfile, sharedRegion)

		// --account: verify the resolved credentials belong to the named account
		// before doing anything else (#179). A refusal is not a usage error, so
		// don't follow it with the help text.
		if err := runAccountGuard(); err != nil {
			cmd.SilenceUsage = true
			return err
		}
		return nil
	}
	// The lazy CLI last mile (libs#78): after a command succeeds, report what
	// lagotto left behind, to --verbose stderr only. It never fails the
	// command, writes nothing to stdout and is bounded (lastmile.go).
	rootCmd.PersistentPostRunE = lastMileHook

	rootCmd.PersistentFlags().StringVar(&flagLang, "lang", "", "Language for output (en, es, fr, de, ja, pt)")
	rootCmd.PersistentFlags().BoolVar(&flagNoEmoji, "no-emoji", false, "Disable emoji in output")
	rootCmd.PersistentFlags().BoolVar(&flagAccessibility, "accessibility", false, "Enable accessibility mode (implies --no-emoji)")

	// Shared spore.host config: AWS profile/region/account, resolved
	// flag > env (SPORE_*/AWS_*) > ~/.config/spore/config.toml > default.
	rootCmd.PersistentFlags().StringVar(&sharedProfile, "profile", "", "AWS named profile (overrides SPORE_PROFILE/AWS_PROFILE and the shared config)")
	rootCmd.PersistentFlags().StringVar(&sharedRegion, "region", "", "Default AWS region (overrides SPORE_REGION/AWS_REGION and the shared config)")
	rootCmd.PersistentFlags().StringVar(&sharedAccount, "account", "", "Expected AWS account ID (optional guard)")

	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "table", "Output format (table, json; list also takes wide)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
	rootCmd.PersistentFlags().StringVar(&watchesTable, "watches-table", "lagotto-watches", "DynamoDB table name for watches")
	rootCmd.PersistentFlags().StringVar(&historyTable, "history-table", "lagotto-match-history", "DynamoDB table name for match history")

	rootCmd.CompletionOptions.DisableDefaultCmd = false
}

func ensureI18nInitialized() {
	if i18nInitialized {
		return
	}
	initI18n()
	i18nInitialized = true
}

func initI18n() {
	cfg := i18n.Config{
		Language:          flagLang,
		Verbose:           false,
		AccessibilityMode: flagAccessibility,
		NoEmoji:           flagNoEmoji,
	}

	if err := i18n.Init(cfg); err != nil {
		log.Printf("Warning: failed to initialize i18n: %v", err)
	}

	updateCommandDescriptions()
}

func updateCommandDescriptions() {
	rootCmd.Short = i18n.T("lagotto.root.short")
	rootCmd.Long = i18n.T("lagotto.root.long")

	if cmd, _, err := rootCmd.Find([]string{"watch"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.watch.short")
		cmd.Long = i18n.T("lagotto.watch.long")
	}
	if cmd, _, err := rootCmd.Find([]string{"list"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.list.short")
	}
	if cmd, _, err := rootCmd.Find([]string{"cancel"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.cancel.short")
	}
	if cmd, _, err := rootCmd.Find([]string{"status"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.status.short")
	}
	if cmd, _, err := rootCmd.Find([]string{"history"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.history.short")
	}
	if cmd, _, err := rootCmd.Find([]string{"poll"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.poll.short")
		cmd.Long = i18n.T("lagotto.poll.long")
	}
	if cmd, _, err := rootCmd.Find([]string{"version"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.version.short")
	}
	if cmd, _, err := rootCmd.Find([]string{"extend"}); err == nil && cmd != nil {
		cmd.Short = i18n.T("lagotto.extend.short")
	}
}

func getOutputFormat() string {
	return outputFormat
}

// commandContext is cmd's context (Execute's, cancelled by SIGINT or
// SIGTERM), or a background one for a command run without it (tests).
func commandContext(cmd *cobra.Command) context.Context {
	if cmd != nil && cmd.Context() != nil {
		return cmd.Context()
	}
	return context.Background()
}

// freshCtx is a short context for giving a lease back after ctx was
// cancelled (an interrupt): ctx's values, not its cancellation, and 30 s.
func freshCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}
