package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run diagnostic checks on GrainFS",
	Long: `Doctor performs health checks on your GrainFS instance.

It checks:
- Data directory accessibility
- BadgerDB health
- Disk space
- Raft log integrity
- Blob storage integrity

Exit codes:
  0 - All checks passed
  1 - Warnings detected
  2 - Failures detected`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataDir, _ := cmd.Flags().GetString("data")
		outputJSON, _ := cmd.Flags().GetBool("json")

		doctor := cluster.NewDoctor(dataDir)
		report, err := doctor.Run()
		if err != nil {
			return fmt.Errorf("diagnostic failed: %w", err)
		}

		if outputJSON {
			if err := report.PrintJSON(); err != nil {
				return err
			}
		} else {
			// Human-readable output
			fmt.Printf("GrainFS Diagnostic Report\n")
			fmt.Printf("========================\n")
			fmt.Printf("Timestamp: %s\n", report.Timestamp.Format(time.RFC3339))
			fmt.Printf("Overall Health: %s\n\n", report.OverallHealth)

			for name, check := range report.Checks {
				icon := "✅"
				if check.Status == "warn" {
					icon = "⚠️ "
				} else if check.Status == "fail" {
					icon = "❌"
				}
				fmt.Printf("%s %s: %s\n", icon, name, check.Message)
				if check.Duration != "" {
					fmt.Printf("   Duration: %s\n", check.Duration)
				}
			}

			if len(report.Recommendations) > 0 {
				fmt.Printf("\nRecommendations:\n")
				for _, rec := range report.Recommendations {
					fmt.Printf("  • %s\n", rec)
				}
			}
		}

		// Set exit code based on health
		switch report.OverallHealth {
		case "fail":
			os.Exit(2)
		case "warn":
			os.Exit(1)
		}
		return nil
	},
}

func init() {
	doctorCmd.Flags().String("data", "./data", "GrainFS data directory")
	doctorCmd.Flags().Bool("json", false, "Output as JSON")
	rootCmd.AddCommand(doctorCmd)
}
