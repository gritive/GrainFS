package main

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/clusteradmin"
	"github.com/spf13/cobra"
)

// clusterConfigCmd is cobra glue; wire + rendering live in clusteradmin.
var clusterConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect or change cluster-wide policy",
}

// clusterConfigFetch is the shared GET preamble for show/get/diff.
func clusterConfigFetch(cmd *cobra.Command) (*clusteradmin.ClusterConfigResponse, error) {
	ep, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return nil, err
	}
	return clusteradmin.NewClient(ep).ClusterConfigGet(cmd.Context())
}

// clusterConfigPatchRaw is the shared PATCH path for set/reset.
func clusterConfigPatchRaw(cmd *cobra.Command, body map[string]any) error {
	ep, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return err
	}
	rev, _ := cmd.Flags().GetUint64("if-match-rev")
	newRev, err := clusteradmin.NewClient(ep).ClusterConfigPatchRaw(cmd.Context(), body, rev)
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), `{"rev":%d}`+"\n", newRev)
	return nil
}

var clusterConfigShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Print all cluster config values (rev + effective + source)",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := clusterConfigFetch(cmd)
		if err != nil {
			return err
		}
		asJSON, _ := cmd.Flags().GetBool("json")
		return clusteradmin.RenderClusterConfigShow(cmd.OutOrStdout(), resp, asJSON)
	},
}

var clusterConfigGetCmd = &cobra.Command{
	Use:   "get <key>",
	Args:  cobra.ExactArgs(1),
	Short: "Print one cluster config value",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := clusterConfigFetch(cmd)
		if err != nil {
			return err
		}
		asJSON, _ := cmd.Flags().GetBool("json")
		return clusteradmin.RenderClusterConfigGet(cmd.OutOrStdout(), resp, args[0], asJSON)
	},
}

var clusterConfigDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show only explicit overrides (vs code defaults)",
	RunE: func(cmd *cobra.Command, args []string) error {
		resp, err := clusterConfigFetch(cmd)
		if err != nil {
			return err
		}
		return clusteradmin.RenderClusterConfigDiff(cmd.OutOrStdout(), resp)
	},
}

var clusterConfigSetCmd = &cobra.Command{
	Use:   "set <key>=<value> [<key>=<value> ...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Set one or more cluster config values (atomic single PATCH)",
	RunE: func(cmd *cobra.Command, args []string) error {
		body, err := clusteradmin.ParseClusterConfigKVs(args)
		if err != nil {
			return err
		}
		return clusterConfigPatchRaw(cmd, body)
	},
}

var clusterConfigResetCmd = &cobra.Command{
	Use:   "reset <key> [<key> ...]",
	Args:  cobra.MinimumNArgs(1),
	Short: "Reset one or more keys to defaults",
	RunE: func(cmd *cobra.Command, args []string) error {
		return clusterConfigPatchRaw(cmd, map[string]any{"reset_keys": args})
	},
}
