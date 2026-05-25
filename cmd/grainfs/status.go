package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/statusadmin"
)

// statusCmd returns the `grainfs status` command.
func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show cluster/iam/encryption/tls/audit/jwt status",
		Example: `  grainfs status
  grainfs status --json`,
		RunE: func(c *cobra.Command, _ []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			return statusadmin.RunGetStatus(c.Context(), statusadmin.GetOptions{
				BaseOptions: statusadmin.BaseOptions{
					Endpoint: sock,
					JSONOut:  asJSON || !stdoutIsTerminal(),
					Stdout:   c.OutOrStdout(),
					Stderr:   c.ErrOrStderr(),
					Timeout:  adminTimeoutFromCmd(c),
				},
			})
		},
	}
	cmd.Flags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	cmd.Flags().Bool("json", false, "output raw JSON")
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func init() {
	rootCmd.AddCommand(statusCmd())
}
