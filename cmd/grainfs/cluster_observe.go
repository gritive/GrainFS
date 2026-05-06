package main

import (
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
	cmd.Flags().String("endpoint", "http://127.0.0.1:9000", "GrainFS server endpoint")
	cmd.Flags().String("format", "text", "Output format: text or json")
	cmd.Flags().Duration("timeout", 5*time.Second, "Request timeout")
	return cmd
}

func runClusterPeers(cmd *cobra.Command, _ []string) error {
	endpoint, _ := cmd.Flags().GetString("endpoint")
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
	cmd.Flags().String("endpoint", "http://127.0.0.1:9000", "GrainFS server endpoint (must be localhost)")
	cmd.Flags().Duration("since", time.Hour, "Look back this far for events")
	cmd.Flags().Int("limit", 50, "Max events to return")
	cmd.Flags().StringSlice("type", nil, "Client-side filter on event Action (repeatable, e.g. cluster-join,cluster-remove-peer)")
	cmd.Flags().String("format", "text", "Output format: text or json")
	cmd.Flags().Duration("timeout", 5*time.Second, "Request timeout")
	return cmd
}

func runClusterEvents(cmd *cobra.Command, _ []string) error {
	endpoint, _ := cmd.Flags().GetString("endpoint")
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
