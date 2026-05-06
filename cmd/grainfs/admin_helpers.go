package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// adminClientFromCmd reads the per-command --endpoint and --data flags and
// returns a configured admin client. Used by non-volume admin CLI commands
// (dashboard, bucket scrub) that share the same connection mechanics.
func adminClientFromCmd(cmd *cobra.Command) (*volumeadmin.Client, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	dataDir, _ := cmd.Flags().GetString("data")
	return volumeadmin.NewClient(endpoint, dataDir)
}

// jsonOut reports whether the --json flag is set on cmd (or any parent). It
// matches the volume-CLI convention so the bucket-scrub and dashboard
// commands stay user-visible-consistent.
func jsonOut(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("json")
	return v
}

// printJSON pretty-prints v as JSON to cmd's stdout.
func printJSON(cmd *cobra.Command, v any) error {
	return volumeadmin.PrintJSON(cmd.OutOrStdout(), v)
}
