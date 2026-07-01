package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/credentialadmin"
)

var credentialCmd = &cobra.Command{
	Use:   "credential",
	Short: "Protocol credential commands",
}

var credentialCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a protocol credential",
	Example: `  grainfs credential create --sa sa_app --protocol nfs --resource bucket/bucket1 --mode rw
  grainfs credential create --sa sa_app --protocol s3 --resource bucket/bucket1 --mode ro --expires-at 2026-06-01T00:00:00Z`,
	RunE: runCredentialCreate,
}

var credentialListCmd = &cobra.Command{
	Use:     "list",
	Short:   "List protocol credentials",
	Example: `  grainfs credential list --sa sa_app --protocol nfs --resource bucket/bucket1`,
	RunE:    runCredentialList,
}

var credentialGetCmd = &cobra.Command{
	Use:     "get <id>",
	Short:   "Show a protocol credential",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs credential get pc_abc123`,
	RunE:    runCredentialGet,
}

var credentialRotateCmd = &cobra.Command{
	Use:     "rotate <id>",
	Short:   "Rotate a protocol credential secret",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs credential rotate pc_abc123`,
	RunE:    runCredentialRotate,
}

var credentialRevokeCmd = &cobra.Command{
	Use:     "revoke <id>",
	Short:   "Revoke a protocol credential",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs credential revoke pc_abc123`,
	RunE:    runCredentialRevoke,
}

func init() {
	pf := credentialCmd.PersistentFlags()
	pf.String("format", "text", "Output format: text or json")
	registerAdminEndpointFlag(credentialCmd, "admin Unix socket path (required, e.g. ./tmp/admin.sock)")
	registerAdminTimeoutFlag(credentialCmd)

	credentialCreateCmd.Flags().String("sa", "", "service account id")
	credentialCreateCmd.Flags().String("protocol", "", "protocol: s3")
	credentialCreateCmd.Flags().String("resource", "", "resource id: bucket/<name>")
	credentialCreateCmd.Flags().String("mode", "", "access mode: ro or rw")
	credentialCreateCmd.Flags().String("expires-at", "", "optional RFC3339 expiration time")
	credentialListCmd.Flags().String("sa", "", "filter by service account id")
	credentialListCmd.Flags().String("protocol", "", "filter by protocol")
	credentialListCmd.Flags().String("resource", "", "filter by resource id: bucket/<name>")

	credentialCmd.AddCommand(
		credentialCreateCmd,
		credentialListCmd,
		credentialGetCmd,
		credentialRotateCmd,
		credentialRevokeCmd,
	)
	rootCmd.AddCommand(credentialCmd)
}

func credentialBaseOptionsFromCmd(cmd *cobra.Command) credentialadmin.BaseOptions {
	endpoint, _ := cmd.Flags().GetString("endpoint")
	return credentialadmin.BaseOptions{
		Endpoint: endpoint,
		JSONOut:  jsonOut(cmd),
		Timeout:  adminTimeoutFromCmd(cmd),
		Stdout:   cmd.OutOrStdout(),
		Stderr:   cmd.ErrOrStderr(),
	}
}

func runCredentialCreate(cmd *cobra.Command, args []string) error {
	saID, _ := cmd.Flags().GetString("sa")
	protocol, _ := cmd.Flags().GetString("protocol")
	resource, _ := cmd.Flags().GetString("resource")
	mode, _ := cmd.Flags().GetString("mode")
	expiresAt, _ := cmd.Flags().GetString("expires-at")
	return credentialadmin.RunCreate(cmd.Context(), credentialadmin.CreateOptions{
		BaseOptions: credentialBaseOptionsFromCmd(cmd),
		SAID:        saID,
		Protocol:    protocol,
		Resource:    resource,
		Mode:        mode,
		ExpiresAt:   expiresAt,
	})
}

func runCredentialList(cmd *cobra.Command, args []string) error {
	saID, _ := cmd.Flags().GetString("sa")
	protocol, _ := cmd.Flags().GetString("protocol")
	resource, _ := cmd.Flags().GetString("resource")
	return credentialadmin.RunList(cmd.Context(), credentialadmin.ListOptions{
		BaseOptions: credentialBaseOptionsFromCmd(cmd),
		SAID:        saID,
		Protocol:    protocol,
		Resource:    resource,
	})
}

func runCredentialGet(cmd *cobra.Command, args []string) error {
	return credentialadmin.RunGet(cmd.Context(), credentialadmin.GetOptions{
		BaseOptions: credentialBaseOptionsFromCmd(cmd),
		ID:          args[0],
	})
}

func runCredentialRotate(cmd *cobra.Command, args []string) error {
	return credentialadmin.RunRotate(cmd.Context(), credentialadmin.RotateOptions{
		BaseOptions: credentialBaseOptionsFromCmd(cmd),
		ID:          args[0],
	})
}

func runCredentialRevoke(cmd *cobra.Command, args []string) error {
	return credentialadmin.RunRevoke(cmd.Context(), credentialadmin.RevokeOptions{
		BaseOptions: credentialBaseOptionsFromCmd(cmd),
		ID:          args[0],
	})
}
