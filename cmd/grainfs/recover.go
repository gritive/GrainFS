package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/spf13/cobra"
)

var recoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Recover from detected issues",
	Long: `Recover attempts to fix issues detected by 'grainfs doctor'.

Actions taken (auto mode):
- Restore from latest snapshot (if Raft log corrupted)
- Trigger replication monitor (for under-replicated shards)
- Reset peer health (for retry)

Use --auto for automatic recovery, or --dry-run to preview actions.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataDir, _ := cmd.Flags().GetString("data")
		autoMode, _ := cmd.Flags().GetBool("auto")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		if !autoMode && !dryRun {
			return fmt.Errorf("either --auto or --dry-run is required")
		}

		if autoMode {
			fmt.Println("🔧 Starting automatic recovery...")
			fmt.Printf("Data directory: %s\n\n", dataDir)

			// Create recovery manager (without full cluster setup for now)
			recoveryMgr := cluster.NewRecoveryManager(dataDir, nil, nil, nil)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			report, err := recoveryMgr.AutoRecover(ctx)
			if err != nil {
				return fmt.Errorf("recovery failed: %w", err)
			}

			fmt.Printf("\nRecovery completed in %s\n", report.EndTime.Sub(report.StartTime))
			fmt.Printf("Success: %v\n", report.Success)

			if len(report.ActionsTaken) > 0 {
				fmt.Println("\nActions taken:")
				for _, action := range report.ActionsTaken {
					fmt.Printf("  ✓ %s\n", action)
				}
			}

			if len(report.Errors) > 0 {
				fmt.Println("\nErrors encountered:")
				for _, err := range report.Errors {
					fmt.Printf("  ✗ %v\n", err)
				}
			}

			if !report.Success {
				os.Exit(1)
			}
		}

		if dryRun {
			fmt.Println("🔍 Dry-run mode: Previewing recovery actions...")
			doctor := cluster.NewDoctor(dataDir)
			report, err := doctor.Run()
			if err != nil {
				return err
			}

			fmt.Printf("\nCurrent health: %s\n", report.OverallHealth)
			fmt.Println("\nRecommended actions:")

			for check, result := range report.Checks {
				if result.Status == "fail" {
					switch check {
					case "raft_log":
						fmt.Printf("  • Restore from latest snapshot\n")
					case "badgerdb":
						fmt.Printf("  • Run BadgerDB repair\n")
					case "disk_space":
						fmt.Printf("  • Free up disk space or expand storage\n")
					default:
						fmt.Printf("  • Investigate %s\n", check)
					}
				}
			}
		}

		return nil
	},
}

func init() {
	recoverCmd.Flags().String("data", "./data", "GrainFS data directory")
	recoverCmd.Flags().Bool("auto", false, "Execute automatic recovery")
	recoverCmd.Flags().Bool("dry-run", false, "Preview recovery actions without executing")
	rootCmd.AddCommand(recoverCmd)
}
