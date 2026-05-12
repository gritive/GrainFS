package main

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management commands",
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status (leader, term, peers)",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := clusterClientFromCmd(cmd)
		if err != nil {
			return err
		}
		format, _ := cmd.Flags().GetString("format")
		return runClusterStatus(cmd.Context(), client, format, cmd.OutOrStdout())
	},
}

// runClusterStatus is the testable core: format=="json" returns raw JSON
// (forward-compat), other values use typed Status.
func runClusterStatus(ctx context.Context, client *clusteradmin.Client, format string, w io.Writer) error {
	if format == "json" {
		raw, err := client.StatusRaw(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintln(w, string(raw))
		return nil
	}
	s, err := client.Status(ctx)
	if err != nil {
		return err
	}
	printClusterStatus(w, s)
	return nil
}

func printClusterStatus(w io.Writer, s *clusteradmin.Status) {
	fmt.Fprintf(w, "mode:      %s\n", s.Mode)
	if s.Mode == "cluster" {
		fmt.Fprintf(w, "node_id:   %s\n", s.NodeID)
		fmt.Fprintf(w, "state:     %s\n", s.State)
		fmt.Fprintf(w, "term:      %d\n", s.Term)
		fmt.Fprintf(w, "leader_id: %s\n", s.LeaderID)
		fmt.Fprintf(w, "peers:     %d\n", len(s.Peers))
		for _, p := range s.Peers {
			fmt.Fprintf(w, "  - %s\n", p)
		}
	}
}

func init() {
	clusterCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (required, e.g. ./tmp/admin.sock)")
	clusterCmd.PersistentFlags().String("format", "text",
		"Output format: text or json (status/peers/events; ignored elsewhere)")

	clusterCmd.AddCommand(clusterStatusCmd)
	clusterCmd.AddCommand(clusterRemovePeerCmd())
	clusterCmd.AddCommand(clusterPeersCmd())
	clusterCmd.AddCommand(clusterEventsCmd())
	// Day-2 ops (Phase 1, metaRaft scope):
	clusterCmd.AddCommand(clusterTransferLeaderCmd)
	clusterCmd.AddCommand(clusterDrainCmd)
	clusterCmd.AddCommand(clusterHealthCmd())
	clusterCmd.AddCommand(clusterPlacementCmd())
	clusterCmd.AddCommand(clusterBalancerCmd)

	// Task 12: read-only cluster config inspection.
	clusterConfigShowCmd.Flags().Bool("json", false, "raw JSON output")
	clusterConfigGetCmd.Flags().Bool("json", false, "raw JSON output")
	clusterConfigCmd.AddCommand(clusterConfigShowCmd, clusterConfigGetCmd, clusterConfigDiffCmd)
	clusterCmd.AddCommand(clusterConfigCmd)

	rootCmd.AddCommand(clusterCmd)
}
