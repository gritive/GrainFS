package main

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/volumeadmin"
)

// DefaultAdminTimeout caps a single admin CLI request when the user does not
// pass --timeout. Replaces the former hardcoded http.Client.Timeout: 30s in
// volumeadmin.NewClient (which silently capped BaseOptions.Timeout > 30s).
const DefaultAdminTimeout = 30 * time.Second

// registerAdminTimeoutFlag adds --timeout to cmd's PersistentFlags so every
// admin subcommand below it inherits it. Unset → DefaultAdminTimeout (30s).
// Explicit 0 (`--timeout 0`) → uncapped; required for long-running follow
// loops like `volume scrub` and `scrub` whose per-poll timeout was previously
// implicit via http.Client.Timeout.
func registerAdminTimeoutFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().Duration("timeout", 0,
		"admin command timeout (e.g. 30s, 5m); unset = 30s default; 0 = uncapped (for long follow loops)")
}

// adminTimeoutFromCmd resolves --timeout. Returns DefaultAdminTimeout when the
// flag was not set; returns the user-supplied value (including 0 for "no cap")
// when explicitly passed.
func adminTimeoutFromCmd(cmd *cobra.Command) time.Duration {
	if !cmd.Flags().Changed("timeout") {
		return DefaultAdminTimeout
	}
	d, _ := cmd.Flags().GetDuration("timeout")
	return d
}

// applyAdminTimeout wraps ctx with the resolved admin timeout. Used by admin
// runners (dashboard, bucket scrub) that don't go through volumeadmin's
// withTimeout helper. d <= 0 returns ctx unchanged (no cap), matching
// volumeadmin.withTimeout's contract.
func applyAdminTimeout(ctx context.Context, cmd *cobra.Command) (context.Context, context.CancelFunc) {
	d := adminTimeoutFromCmd(cmd)
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

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
