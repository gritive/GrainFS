package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/cluster"
)

func planShowCmd() *cobra.Command {
	var dataDir string
	cmd := &cobra.Command{
		Use:   "plan-show",
		Short: "Show active rebalance plan and load snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			fsm, err := loadFSMFromStore(dataDir)
			if err != nil {
				return fmt.Errorf("plan show: %w", err)
			}

			plan := fsm.ActivePlan()
			if plan == nil {
				fmt.Println("No active rebalance plan.")
			} else {
				fmt.Printf("Active Plan: %s\n", plan.PlanID)
				fmt.Printf("  Group:     %s\n", plan.GroupID)
				fmt.Printf("  From:      %s → To: %s\n", plan.FromNode, plan.ToNode)
				fmt.Printf("  Created:   %s\n", plan.CreatedAt.Format("2006-01-02 15:04:05"))
			}

			snap := fsm.LoadSnapshot()
			if len(snap) == 0 {
				fmt.Println("\nNo load snapshot available.")
				return nil
			}
			fmt.Println("\nLoad Snapshot:")
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "NODE\tDISK_USED%\tDISK_AVAIL\tREQ/S")
			for _, e := range snap {
				fmt.Fprintf(w, "%s\t%.1f%%\t%d\t%.1f\n",
					e.NodeID, e.DiskUsedPct, e.DiskAvailBytes, e.RequestsPerSec)
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", "./data", "data directory for local FSM read")
	return cmd
}

func rebalanceCmd() *cobra.Command {
	var dryRun bool
	var dataDir string
	cmd := &cobra.Command{
		Use:   "rebalance",
		Short: "Trigger or preview a rebalance plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			fsm, err := loadFSMFromStore(dataDir)
			if err != nil {
				return fmt.Errorf("rebalance: %w", err)
			}

			snap := fsm.LoadSnapshot()
			if len(snap) < 2 {
				fmt.Println("Not enough nodes with load data.")
				return nil
			}

			heaviest, lightest, diff := cliImbalance(snap)
			cfg := cluster.DefaultRebalancerConfig()
			if diff < cfg.ImbalanceThresh {
				fmt.Printf("Cluster is balanced (diff=%.1f%% < threshold=%.1f%%).\n",
					diff, cfg.ImbalanceThresh)
				return nil
			}

			fmt.Printf("Imbalance detected: %.1f%% (heaviest=%s, lightest=%s)\n",
				diff, heaviest, lightest)
			if dryRun {
				fmt.Println("[dry-run] Would propose rebalance. Use without --dry-run to execute.")
				return nil
			}
			fmt.Println("Use --dry-run to preview. Live trigger via gRPC coming in PR-E.")
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "preview without proposing")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./data", "data directory for local FSM read")
	return cmd
}

// cliImbalance returns (heaviest, lightest, diff) from the load snapshot.
func cliImbalance(snap map[string]cluster.LoadStatEntry) (heaviest, lightest string, diff float64) {
	var maxPct, minPct float64 = -1, 101
	for _, e := range snap {
		if e.DiskUsedPct > maxPct {
			maxPct = e.DiskUsedPct
			heaviest = e.NodeID
		}
		if e.DiskUsedPct < minPct {
			minPct = e.DiskUsedPct
			lightest = e.NodeID
		}
	}
	return heaviest, lightest, maxPct - minPct
}

// loadFSMFromStore reads the MetaFSM from a local BadgerDB store.
// TODO(PR-E): implement local snapshot read via raft.BadgerLogStore.
func loadFSMFromStore(_ string) (*cluster.MetaFSM, error) {
	return cluster.NewMetaFSM(), nil
}
