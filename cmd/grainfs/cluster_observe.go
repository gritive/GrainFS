package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	client := clusteradmin.NewClient(endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	status, err := client.Status(ctx)
	if err != nil {
		return err
	}

	if format == "json" {
		raw, _ := json.MarshalIndent(status, "", "  ")
		fmt.Fprintln(cmd.OutOrStdout(), string(raw))
		return nil
	}
	if status.Mode != "cluster" {
		fmt.Fprintf(cmd.OutOrStdout(), "mode: %s (no peers)\n", status.Mode)
		return nil
	}
	return clusteradmin.RenderPeersTable(cmd.OutOrStdout(), clusteradmin.PeersFromStatus(status))
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

	client := clusteradmin.NewClient(endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	events, err := client.EventLog(ctx, since, limit)
	if err != nil {
		return err
	}
	events = clusteradmin.FilterEventsByAction(events, types)

	if format == "json" {
		raw, _ := json.MarshalIndent(events, "", "  ")
		fmt.Fprintln(cmd.OutOrStdout(), string(raw))
		return nil
	}
	return clusteradmin.RenderEventsTable(cmd.OutOrStdout(), events)
}
