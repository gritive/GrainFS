package main

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

var clusterBalancerCmd = &cobra.Command{
	Use:   "balancer",
	Short: "Cluster balancer commands",
}

var clusterBalancerStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster balancer state and per-node load stats",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := clusterClientFromCmd(cmd)
		if err != nil {
			return err
		}
		format, _ := cmd.Flags().GetString("format")
		return runClusterBalancerStatus(cmd.Context(), client, format, cmd.OutOrStdout())
	},
}

func runClusterBalancerStatus(ctx context.Context, client *clusteradmin.Client, format string, w io.Writer) error {
	b, err := client.BalancerStatus(ctx)
	if err != nil {
		return err
	}
	if format == "json" {
		return printJSONIndented(w, b)
	}
	if !b.Available {
		fmt.Fprintln(w, "balancer: not available (cluster mode not configured)")
		return nil
	}
	if !b.Active {
		fmt.Fprintln(w, "balancer: disabled")
		return nil
	}
	fmt.Fprintln(w, "balancer: enabled (active)")
	fmt.Fprintf(w, "imbalance: %.1f%%\n", b.ImbalancePct)
	if len(b.Nodes) > 0 {
		fmt.Fprintln(w, "\nNODES")
		fmt.Fprintln(w, "  ID       DISK_USED   DISK_AVAIL    REQ/S")
		for _, n := range b.Nodes {
			fmt.Fprintf(w, "  %-8s %5.1f%%      %12d  %5.1f\n",
				n.NodeID, n.DiskUsedPct, n.DiskAvailBytes, n.RequestsPerSec)
		}
	}
	return nil
}

func init() {
	clusterBalancerCmd.AddCommand(clusterBalancerStatusCmd)
}
