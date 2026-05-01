package main

import (
	"encoding/json"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/spf13/cobra"
)

var recoverCmd = &cobra.Command{
	Use:   "recover",
	Short: "Recover from detected issues",
	Long: `Recover inspects issues detected by 'grainfs doctor' and exposes
operator-driven disaster recovery commands.

The legacy --auto path is deprecated and exits without mutating data.
Use 'recover cluster plan' and 'recover cluster execute' for offline
snapshot recovery into a fresh single-node cluster.

Use --dry-run to preview doctor recommendations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataDir, _ := cmd.Flags().GetString("data")
		autoMode, _ := cmd.Flags().GetBool("auto")
		dryRun, _ := cmd.Flags().GetBool("dry-run")

		if !autoMode && !dryRun {
			return fmt.Errorf("either --auto or --dry-run is required")
		}

		if autoMode {
			return fmt.Errorf("recover --auto is deprecated and does not perform snapshot disaster recovery; use 'grainfs recover cluster plan' first")
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

var recoverClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Recover an offline Raft snapshot into a fresh single-node cluster",
}

var recoverClusterPlanCmd = &cobra.Command{
	Use:   "plan",
	Short: "Inspect a source snapshot and print the recovery plan",
	RunE: func(cmd *cobra.Command, args []string) error {
		opts, err := recoverClusterOptions(cmd)
		if err != nil {
			return err
		}
		plan, err := cluster.BuildRecoverClusterPlan(opts)
		if err != nil {
			return err
		}
		return printRecoverClusterPlan(plan)
	},
}

var recoverClusterExecuteCmd = &cobra.Command{
	Use:   "execute",
	Short: "Promote a source snapshot into a fresh target data directory",
	RunE: func(cmd *cobra.Command, args []string) error {
		opts, err := recoverClusterOptions(cmd)
		if err != nil {
			return err
		}
		plan, err := cluster.BuildRecoverClusterPlan(opts)
		if err != nil {
			return err
		}
		if err := cluster.ExecuteRecoverClusterPlan(plan); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "recovered snapshot %d/%d into %s; writes disabled until 'grainfs recover cluster verify --mark-writable'\n", plan.SnapshotIndex, plan.SnapshotTerm, opts.TargetData)
		return nil
	},
}

var recoverClusterVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify a recovered target and optionally mark it writable",
	RunE: func(cmd *cobra.Command, args []string) error {
		target, _ := cmd.Flags().GetString("target-data")
		markWritable, _ := cmd.Flags().GetBool("mark-writable")
		if target == "" {
			return fmt.Errorf("--target-data is required")
		}
		marker, err := cluster.LoadRecoverClusterMarker(target)
		if err != nil {
			return err
		}
		if marker == nil {
			return fmt.Errorf("recovery marker not found at %s", target)
		}
		if !markWritable {
			raw, _ := json.MarshalIndent(marker, "", "  ")
			fmt.Fprintln(cmd.OutOrStdout(), string(raw))
			return nil
		}
		if err := cluster.MarkRecoverClusterWritable(target); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "recovery marker marked writable: %s\n", target)
		return nil
	},
}

func init() {
	recoverCmd.Flags().String("data", "./data", "GrainFS data directory")
	recoverCmd.Flags().Bool("auto", false, "deprecated; exits without mutating data")
	recoverCmd.Flags().Bool("dry-run", false, "Preview recovery actions without executing")
	addRecoverClusterFlags(recoverClusterPlanCmd)
	addRecoverClusterFlags(recoverClusterExecuteCmd)
	recoverClusterVerifyCmd.Flags().String("target-data", "", "fresh recovered target data directory")
	recoverClusterVerifyCmd.Flags().Bool("mark-writable", false, "mark a recovered target writable after external verification")
	recoverClusterCmd.AddCommand(recoverClusterPlanCmd, recoverClusterExecuteCmd, recoverClusterVerifyCmd)
	recoverCmd.AddCommand(recoverClusterCmd)
	rootCmd.AddCommand(recoverCmd)
}

func addRecoverClusterFlags(cmd *cobra.Command) {
	cmd.Flags().String("source-data", "", "offline source GrainFS data directory")
	cmd.Flags().String("target-data", "", "fresh target GrainFS data directory")
	cmd.Flags().String("new-node-id", "", "node ID for the recovered single voter")
	cmd.Flags().String("new-raft-addr", "", "Raft address for the recovered node")
	cmd.Flags().Bool("badger-managed-mode", false, "open recovered target Raft store in managed mode")
	cmd.Flags().Bool("strip-joint-state", false, "recover a mid-joint snapshot as a clean single-node cluster")
}

func recoverClusterOptions(cmd *cobra.Command) (cluster.RecoverClusterOptions, error) {
	source, _ := cmd.Flags().GetString("source-data")
	target, _ := cmd.Flags().GetString("target-data")
	nodeID, _ := cmd.Flags().GetString("new-node-id")
	raftAddr, _ := cmd.Flags().GetString("new-raft-addr")
	managed, _ := cmd.Flags().GetBool("badger-managed-mode")
	stripJoint, _ := cmd.Flags().GetBool("strip-joint-state")
	return cluster.RecoverClusterOptions{
		SourceData:        source,
		TargetData:        target,
		NewNodeID:         nodeID,
		NewRaftAddr:       raftAddr,
		BadgerManagedMode: managed,
		StripJointState:   stripJoint,
	}, nil
}

func printRecoverClusterPlan(plan *cluster.RecoverClusterPlan) error {
	out, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
