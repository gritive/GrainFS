package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/auditadmin"
)

// auditCmd returns the `grainfs audit` command tree.
func auditCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "audit",
		Short: "Query the audit log via DuckDB/Iceberg",
	}
	cmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	cmd.PersistentFlags().Bool("json", false, "output raw JSON")

	cmd.AddCommand(
		auditQueryCmd(),
		auditRecentDeniesCmd(),
		auditBySACmd(),
		auditByRequestIDCmd(),
	)
	return cmd
}

func auditQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "query <SQL>",
		Short:   "Execute a read-only SQL query against the audit table",
		Example: `  grainfs audit query "SELECT count(*) FROM grainfs_iceberg.audit.s3 WHERE auth_status='deny'"`,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			return auditadmin.RunAuditQuery(c.Context(), auditadmin.QueryOptions{
				BaseOptions: auditadmin.BaseOptions{
					Endpoint: sock,
					JSONOut:  asJSON,
					Stdout:   c.OutOrStdout(),
					Stderr:   c.ErrOrStderr(),
					Timeout:  adminTimeoutFromCmd(c),
				},
				SQL: args[0],
			})
		},
	}
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func auditRecentDeniesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "recent-denies",
		Short: "Show the most-recent deny-outcome audit entries",
		Example: `  grainfs audit recent-denies
  grainfs audit recent-denies --limit 10`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			limit, _ := c.Flags().GetInt("limit")
			asJSON, _ := c.Flags().GetBool("json")
			return auditadmin.RunAuditRecentDenies(c.Context(), auditadmin.RecentDeniesOptions{
				BaseOptions: auditadmin.BaseOptions{
					Endpoint: sock,
					JSONOut:  asJSON,
					Stdout:   c.OutOrStdout(),
					Stderr:   c.ErrOrStderr(),
					Timeout:  adminTimeoutFromCmd(c),
				},
				Limit: limit,
			})
		},
	}
	cmd.Flags().Int("limit", 50, "maximum number of rows to return")
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func auditBySACmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "by-sa <sa_id>",
		Short:   "Show audit entries for a specific service account",
		Example: `  grainfs audit by-sa sa:abc123`,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			limit, _ := c.Flags().GetInt("limit")
			asJSON, _ := c.Flags().GetBool("json")
			return auditadmin.RunAuditBySA(c.Context(), auditadmin.BySAOptions{
				BaseOptions: auditadmin.BaseOptions{
					Endpoint: sock,
					JSONOut:  asJSON,
					Stdout:   c.OutOrStdout(),
					Stderr:   c.ErrOrStderr(),
					Timeout:  adminTimeoutFromCmd(c),
				},
				SAID:  args[0],
				Limit: limit,
			})
		},
	}
	cmd.Flags().Int("limit", 50, "maximum number of rows to return")
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func auditByRequestIDCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "by-request-id <request_id>",
		Short:   "Show the audit entry for a specific request ID",
		Example: `  grainfs audit by-request-id 01938abc-1234-7000-0000-000000000000`,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			return auditadmin.RunAuditByRequestID(c.Context(), auditadmin.ByRequestIDOptions{
				BaseOptions: auditadmin.BaseOptions{
					Endpoint: sock,
					JSONOut:  asJSON,
					Stdout:   c.OutOrStdout(),
					Stderr:   c.ErrOrStderr(),
					Timeout:  adminTimeoutFromCmd(c),
				},
				RequestID: args[0],
			})
		},
	}
	registerAdminTimeoutFlag(cmd)
	return cmd
}

func init() {
	rootCmd.AddCommand(auditCmd())
}
