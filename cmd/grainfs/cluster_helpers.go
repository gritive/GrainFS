package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// clusterClientFromCmd resolves --endpoint and returns a configured
// clusteradmin client. Rejects http(s):// URLs because the CLI talks to
// the admin Unix socket; HTTP /api/cluster/* is dashboard-only since
// v0.0.89.
//
// The clusteradmin package itself accepts http:// for test injection;
// the rejection lives at the CLI layer to keep tests flexible while
// preventing operator confusion in the field.
func clusterClientFromCmd(cmd *cobra.Command) (*clusteradmin.Client, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: grainfs cluster --endpoint <data-dir>/admin.sock <subcommand>")
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return nil, fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  HTTP /api/cluster/* is dashboard-only since v0.0.89.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", endpoint)
	}
	return clusteradmin.NewClient(endpoint), nil
}
