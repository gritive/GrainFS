package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func clusterPeersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "peers",
		Short: "List cluster voters with leader and liveness state",
		RunE:  runClusterPeers,
	}
	cmd.Flags().Duration("timeout", 5*time.Second, "Request timeout")
	return cmd
}

func runClusterPeers(cmd *cobra.Command, _ []string) error {
	endpoint, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return err
	}
	format, _ := cmd.Flags().GetString("format")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	return clusteradmin.Peers(cmd.Context(), clusteradmin.PeersOptions{
		BaseOptions: clusteradmin.BaseOptions{
			Endpoint: endpoint,
			Timeout:  timeout,
			Stdout:   cmd.OutOrStdout(),
		},
		Format: format,
	})
}

func clusterEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show recent cluster audit events",
		RunE:  runClusterEvents,
	}
	cmd.Flags().Duration("since", time.Hour, "Look back this far for events")
	cmd.Flags().Int("limit", 50, "Max events to return")
	cmd.Flags().StringSlice("type", nil, "Client-side filter on event Action (repeatable, e.g. cluster-join,cluster-remove-peer)")
	cmd.Flags().Duration("timeout", 5*time.Second, "Request timeout")
	return cmd
}

func clusterHealthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Show aggregated cluster health (quorum, peers, issues)",
		RunE:  runClusterHealthCmd,
	}
	return cmd
}

func runClusterHealthCmd(cmd *cobra.Command, _ []string) error {
	client, err := clusterClientFromCmd(cmd)
	if err != nil {
		return err
	}
	format, _ := cmd.Flags().GetString("format")
	return runClusterHealth(cmd.Context(), client, format, cmd.OutOrStdout())
}

// runClusterHealth fetches typed Health and renders it. Issues are derived
// server-side (A3-a) so dashboard and CLI render the same diagnostics.
func runClusterHealth(ctx context.Context, client *clusteradmin.Client, format string, w io.Writer) error {
	h, err := client.Health(ctx)
	if err != nil {
		return err
	}
	if format == "json" {
		return printJSONIndented(w, h)
	}
	fmt.Fprintf(w, "mode:     %s\n", h.Mode)
	fmt.Fprintf(w, "degraded: %v\n", h.Degraded)
	if h.Quorum.VotersTotal > 0 {
		verdict := "healthy"
		if !h.Quorum.Healthy {
			verdict = "QUORUM LOST"
		}
		fmt.Fprintf(w, "quorum:   %d/%d alive (need %d) — %s\n",
			h.Quorum.AliveCount, h.Quorum.VotersTotal, h.Quorum.Required, verdict)
	}
	if h.LeaderID != "" {
		fmt.Fprintf(w, "leader:   %s (term %d)\n", h.LeaderID, h.Term)
	}
	if len(h.Peers) > 0 {
		fmt.Fprintln(w, "\nPEERS")
		fmt.Fprintln(w, "  ID       STATE     RAFT_ADDR")
		for _, p := range h.Peers {
			fmt.Fprintf(w, "  %-8s %-9s %s\n", p.PeerID, p.State, p.RaftAddr)
		}
	}
	fmt.Fprintln(w, "\nISSUES")
	if len(h.Issues) == 0 {
		fmt.Fprintln(w, "  (none)")
	} else {
		for _, issue := range h.Issues {
			fmt.Fprintf(w, "  - %s\n", issue)
		}
	}
	return nil
}

func clusterPlacementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "placement [bucket] [key]",
		Short: "Show object placement summary and detail",
		Args:  cobra.MaximumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := clusterClientFromCmd(cmd)
			if err != nil {
				return err
			}
			bucket := ""
			key := ""
			if len(args) >= 1 {
				bucket = args[0]
			}
			if len(args) == 2 {
				key = args[1]
			}
			format, _ := cmd.Flags().GetString("format")
			return runClusterPlacement(cmd.Context(), client, bucket, key, format, cmd.OutOrStdout())
		},
	}
	return cmd
}

func runClusterPlacement(ctx context.Context, client *clusteradmin.Client, bucket, key, format string, w io.Writer) error {
	report, err := client.Placement(ctx, clusteradmin.PlacementOptions{Bucket: bucket, Key: key, Limit: 100})
	if err != nil {
		return err
	}
	if format == "json" {
		return printJSONIndented(w, report)
	}
	fmt.Fprintf(w, "Desired policy: %s\n", report.DesiredPolicyBasis)
	fmt.Fprintf(w, "Objects: %d\n", report.ObjectCount)
	fmt.Fprintf(w, "Bytes: %d\n", report.Bytes)
	fmt.Fprintf(w, "Pending upgrade: %d\n", report.PendingUpgradeCount)
	fmt.Fprintf(w, "Downgrade skipped: %d\n", report.DowngradeSkippedCount)
	fmt.Fprintf(w, "Unknown: %d\n", report.UnknownLayoutCount)
	fmt.Fprintf(w, "Repair needed: %d\n", report.RepairNeededCount)
	if bucket != "" {
		fmt.Fprintf(w, "Bucket: %s\n", bucket)
	}
	if key != "" {
		fmt.Fprintf(w, "Key: %s\n", key)
	}
	if len(report.Details) == 0 {
		return nil
	}
	fmt.Fprintln(w, "\nDETAIL")
	fmt.Fprintln(w, "  BUCKET       KEY          VERSION      GROUP       ACTUAL  DESIRED  STATE              NODES")
	for _, row := range report.Details {
		actual := fmt.Sprintf("%d+%d", row.ActualECData, row.ActualECParity)
		desired := fmt.Sprintf("%d+%d", row.DesiredECData, row.DesiredECParity)
		fmt.Fprintf(w, "  %-12s %-12s %-12s %-11s %-7s %-8s %-18s %s\n",
			row.Bucket, row.Key, row.VersionID, row.PlacementGroupID, actual, desired, row.LayoutState, strings.Join(row.NodeIDs, ", "))
	}
	return nil
}

// printJSONIndented renders v as indented JSON to w. Shared helper for
// --format=json output across cluster subcommands.
func printJSONIndented(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func runClusterEvents(cmd *cobra.Command, _ []string) error {
	endpoint, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return err
	}
	since, _ := cmd.Flags().GetDuration("since")
	limit, _ := cmd.Flags().GetInt("limit")
	types, _ := cmd.Flags().GetStringSlice("type")
	format, _ := cmd.Flags().GetString("format")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	return clusteradmin.Events(cmd.Context(), clusteradmin.EventsOptions{
		BaseOptions: clusteradmin.BaseOptions{
			Endpoint: endpoint,
			Timeout:  timeout,
			Stdout:   cmd.OutOrStdout(),
		},
		Since:       since,
		Limit:       limit,
		TypeFilters: types,
		Format:      format,
	})
}
