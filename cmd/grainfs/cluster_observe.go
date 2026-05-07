package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
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
		Endpoint: endpoint,
		Format:   format,
		Timeout:  timeout,
		Stdout:   cmd.OutOrStdout(),
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
		Use:   "placement [bucket]",
		Short: "Show shard groups and bucket assignments",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := clusterClientFromCmd(cmd)
			if err != nil {
				return err
			}
			bucket := ""
			if len(args) == 1 {
				bucket = args[0]
			}
			format, _ := cmd.Flags().GetString("format")
			return runClusterPlacement(cmd.Context(), client, bucket, format, cmd.OutOrStdout())
		},
	}
	return cmd
}

// runClusterPlacement reuses the cluster status endpoint — no new server
// route. It renders shard groups + bucket assignments as a human-readable
// view; an optional bucket arg filters to a single row.
func runClusterPlacement(ctx context.Context, client *clusteradmin.Client, bucket, format string, w io.Writer) error {
	s, err := client.Status(ctx)
	if err != nil {
		return err
	}
	if format == "json" {
		out := struct {
			Mode              string                    `json:"mode"`
			ShardGroups       []clusteradmin.ShardGroup `json:"shard_groups,omitempty"`
			BucketAssignments map[string]string         `json:"bucket_assignments,omitempty"`
			Bucket            string                    `json:"bucket,omitempty"`
		}{
			Mode:              s.Mode,
			ShardGroups:       s.ShardGroups,
			BucketAssignments: s.BucketAssignments,
			Bucket:            bucket,
		}
		return printJSONIndented(w, out)
	}
	if s.Mode == "local" {
		fmt.Fprintln(w, "single-node mode — no shard groups")
		return nil
	}
	if len(s.ShardGroups) == 0 && len(s.BucketAssignments) == 0 {
		fmt.Fprintln(w, "no shard groups configured")
		return nil
	}
	if bucket == "" {
		fmt.Fprintln(w, "SHARD GROUPS")
		fmt.Fprintln(w, "  ID         PEERS")
		for _, sg := range s.ShardGroups {
			fmt.Fprintf(w, "  %-10s %s\n", sg.ID, strings.Join(sg.PeerIDs, ", "))
		}
		fmt.Fprintln(w, "\nBUCKET ASSIGNMENTS")
		if len(s.BucketAssignments) == 0 {
			fmt.Fprintln(w, "  (none)")
			return nil
		}
		fmt.Fprintln(w, "  BUCKET       GROUP      PEERS")
		// Sorted iteration for deterministic output.
		buckets := make([]string, 0, len(s.BucketAssignments))
		for b := range s.BucketAssignments {
			buckets = append(buckets, b)
		}
		sort.Strings(buckets)
		for _, b := range buckets {
			g := s.BucketAssignments[b]
			peers := lookupGroupPeers(s.ShardGroups, g)
			fmt.Fprintf(w, "  %-12s %-10s %s\n", b, g, strings.Join(peers, ", "))
		}
		return nil
	}
	g, ok := s.BucketAssignments[bucket]
	if !ok {
		fmt.Fprintf(w, "bucket %q not assigned to any group\n", bucket)
		return nil
	}
	peers := lookupGroupPeers(s.ShardGroups, g)
	fmt.Fprintln(w, "BUCKET       GROUP      PEERS")
	fmt.Fprintf(w, "  %-12s %-10s %s\n", bucket, g, strings.Join(peers, ", "))
	return nil
}

func lookupGroupPeers(groups []clusteradmin.ShardGroup, id string) []string {
	for _, g := range groups {
		if g.ID == id {
			return g.PeerIDs
		}
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
		Endpoint:    endpoint,
		Since:       since,
		Limit:       limit,
		TypeFilters: types,
		Format:      format,
		Timeout:     timeout,
		Stdout:      cmd.OutOrStdout(),
	})
}
