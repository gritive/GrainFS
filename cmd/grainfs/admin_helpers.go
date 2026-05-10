package main

import (
	"context"
	"fmt"
	"strings"
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

// registerAdminEndpointFlag adds --endpoint to cmd's PersistentFlags so every
// admin subcommand below it inherits a single shared resolver. The value is a
// bare UDS path (e.g. ./tmp/admin.sock); when unset, resolution fails fast.
func registerAdminEndpointFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (required, e.g. ./tmp/admin.sock)")
}

// adminClientFromCmd reads --endpoint and returns a configured admin client.
// Used by admin CLI commands (volume, dashboard, bucket scrub) that share the
// same connection mechanics.
func adminClientFromCmd(cmd *cobra.Command) (*volumeadmin.Client, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	return volumeadmin.NewClient(endpoint)
}

// jsonOut reports whether the caller wants JSON output. Single source of
// truth: --format=json. Unified across cluster/volume/dashboard CLIs.
func jsonOut(cmd *cobra.Command) bool {
	f, _ := cmd.Flags().GetString("format")
	return f == "json"
}

// printJSON pretty-prints v as JSON to cmd's stdout.
func printJSON(cmd *cobra.Command, v any) error {
	return volumeadmin.PrintJSON(cmd.OutOrStdout(), v)
}

// adminEndpointFromCmd resolves --endpoint and validates it. Used by all admin
// CLI groups (iam, bucket, …) that talk to the admin Unix socket. Rejects
// http(s):// URLs because admin protocol runs over UDS only.
func adminEndpointFromCmd(cmd *cobra.Command) (string, error) {
	ep, _ := cmd.Flags().GetString("endpoint")
	ep = strings.TrimSpace(ep)
	if ep == "" {
		return "", fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: use --endpoint <data-dir>/admin.sock")
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return "", fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", ep)
	}
	return strings.TrimPrefix(ep, "unix:"), nil
}
