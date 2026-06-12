package cmd

import (
	"fmt"

	"github.com/spore-host/libs/update"
	"github.com/spf13/cobra"
)

var (
	GitCommit = "unknown"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  `Display version, build date, and git commit information for lagotto.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("🐕 Lagotto - EC2 Capacity Watcher\n\n")
		fmt.Printf("Version:    %s\n", Version)
		fmt.Printf("Git Commit: %s\n", GitCommit)
		fmt.Printf("Build Date: %s\n", BuildDate)
		fmt.Printf("\nProject:    https://spore.host\n")

		// Explicit, user-initiated check — report whether a newer release exists.
		if res := update.CheckNow("lagotto", Version); res == nil {
			fmt.Printf("\n(couldn't check for updates)\n")
		} else if res.HasUpdate() {
			fmt.Printf("\n⬆️  A newer version is available: %s → %s\n    %s\n",
				res.CurrentVersion, res.LatestVersion, res.UpdateURL)
		} else {
			fmt.Printf("\n✓ You're on the latest version.\n")
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
