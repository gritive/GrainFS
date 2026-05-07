package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

// clusterEndpointFromCmd resolves --endpoint and validates it. Rejects
// http(s):// URLs because the CLI talks to the admin Unix socket; HTTP
// /api/cluster/* is dashboard-only since v0.0.89.
//
// The clusteradmin package itself accepts http:// for test injection;
// the rejection lives at the CLI layer to keep tests flexible while
// preventing operator confusion in the field.
func clusterEndpointFromCmd(cmd *cobra.Command) (string, error) {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: grainfs cluster --endpoint <data-dir>/admin.sock <subcommand>")
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return "", fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  HTTP /api/cluster/* is dashboard-only since v0.0.89.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", endpoint)
	}
	return endpoint, nil
}

// clusterClientFromCmd returns a clusteradmin.Client configured from --endpoint.
// Used by status / remove-peer; peers / events use clusterEndpointFromCmd
// because they call package-level helpers (clusteradmin.Peers / Events) that
// take an Endpoint string and construct their own client internally.
func clusterClientFromCmd(cmd *cobra.Command) (*clusteradmin.Client, error) {
	endpoint, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return nil, err
	}
	return clusteradmin.NewClient(endpoint), nil
}
