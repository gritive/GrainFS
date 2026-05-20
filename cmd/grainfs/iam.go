package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamCmd = &cobra.Command{
	Use:   "iam",
	Short: "Manage GrainFS IAM (ServiceAccounts, AccessKeys, Grants)",
}

// iamBaseOptionsFromCmd reads the flags every iam runner shares and builds
// a populated BaseOptions. --json (bool) maps to JSONOut; --endpoint resolves
// via the shared admin helper (env fallback included).
func iamBaseOptionsFromCmd(cmd *cobra.Command) (iamadmin.BaseOptions, error) {
	ep, err := adminEndpointFromCmd(cmd)
	if err != nil {
		return iamadmin.BaseOptions{}, err
	}
	asJSON, _ := cmd.Flags().GetBool("json")
	return iamadmin.BaseOptions{
		Endpoint: ep,
		JSONOut:  asJSON,
		Stdout:   cmd.OutOrStdout(),
		Stderr:   cmd.ErrOrStderr(),
	}, nil
}

// --- sa ---

var iamSACmd = &cobra.Command{Use: "sa", Short: "Manage ServiceAccounts"}

var iamSACreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a ServiceAccount; returns the first AccessKey + one-time secret",
	Example: `  grainfs iam sa create alice --description "data team"
  grainfs iam --json sa create alice`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		desc, _ := c.Flags().GetString("description")
		return iamadmin.RunSACreate(c.Context(), iamadmin.SACreateOptions{
			BaseOptions: base, Name: args[0], Description: desc,
		})
	},
}

var iamSAListCmd = &cobra.Command{
	Use:   "list",
	Short: "List ServiceAccounts",
	Example: `  grainfs iam sa list
  grainfs iam --json sa list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunSAList(c.Context(), iamadmin.SAListOptions{BaseOptions: base})
	},
}

var iamSAGetCmd = &cobra.Command{
	Use:   "get <sa_id>",
	Short: "Show SA detail",
	Example: `  grainfs iam sa get sa-abc123
  grainfs iam --json sa get sa-abc123`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunSAGet(c.Context(), iamadmin.SAGetOptions{BaseOptions: base, SAID: args[0]})
	},
}

var iamSADeleteCmd = &cobra.Command{
	Use:     "delete <sa_id>",
	Short:   "Delete an SA (cascades to its keys + grants via FSM)",
	Example: `  grainfs iam sa delete sa-abc123`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunSADelete(c.Context(), iamadmin.SADeleteOptions{BaseOptions: base, SAID: args[0]})
	},
}

// --- key ---

var iamKeyCmd = &cobra.Command{Use: "key", Short: "Manage SA AccessKeys"}

var iamKeyCreateCmd = &cobra.Command{
	Use:   "create <sa_id>",
	Short: "Issue a new AccessKey for the SA (one-time secret_key in response)",
	Example: `  grainfs iam key create sa-abc123
  grainfs iam key create sa-abc123 --bucket logs --bucket reports`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		buckets, _ := c.Flags().GetStringSlice("bucket")
		return iamadmin.RunKeyCreate(c.Context(), iamadmin.KeyCreateOptions{
			BaseOptions: base, SAID: args[0], Buckets: buckets,
		})
	},
}

var iamKeyRevokeCmd = &cobra.Command{
	Use:     "revoke <sa_id> <access_key>",
	Short:   "Revoke an AccessKey",
	Example: `  grainfs iam key revoke sa-abc123 AK1234`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunKeyRevoke(c.Context(), iamadmin.KeyRevokeOptions{
			BaseOptions: base, SAID: args[0], AccessKey: args[1],
		})
	},
}

// --- grant ---

var iamGrantCmd = &cobra.Command{Use: "grant", Short: "Manage Grants"}

var iamGrantPutCmd = &cobra.Command{
	Use:     "put <sa_id> <bucket> <role>",
	Short:   "Grant role on bucket to SA (role: Read|Write|Admin)",
	Example: `  grainfs iam grant put sa-abc123 my-bucket Write`,
	Args:    cobra.ExactArgs(3),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGrantPut(c.Context(), iamadmin.GrantPutOptions{
			BaseOptions: base, SAID: args[0], Bucket: args[1], Role: args[2],
		})
	},
}

var iamGrantDeleteCmd = &cobra.Command{
	Use:     "delete <sa_id> <bucket>",
	Short:   "Remove grant from SA on bucket",
	Example: `  grainfs iam grant delete sa-abc123 my-bucket`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGrantDelete(c.Context(), iamadmin.GrantDeleteOptions{
			BaseOptions: base, SAID: args[0], Bucket: args[1],
		})
	},
}

var iamGrantListCmd = &cobra.Command{
	Use:   "list",
	Short: "List grants (filter with --sa or --bucket)",
	Example: `  grainfs iam grant list
  grainfs iam grant list --sa sa-abc123
  grainfs iam grant list --bucket my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		sa, _ := c.Flags().GetString("sa")
		bucket, _ := c.Flags().GetString("bucket")
		return iamadmin.RunGrantList(c.Context(), iamadmin.GrantListOptions{
			BaseOptions: base, SAFilter: sa, BucketFilter: bucket,
		})
	},
}

func init() {
	iamCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	iamCmd.PersistentFlags().Bool("json", false, "output raw JSON")

	iamSACreateCmd.Flags().String("description", "", "free-form SA description")
	iamKeyCreateCmd.Flags().StringSlice("bucket", nil,
		"restrict the new key to specific buckets (repeatable; default: unrestricted)")
	iamGrantListCmd.Flags().String("sa", "", "filter by sa_id")
	iamGrantListCmd.Flags().String("bucket", "", "filter by bucket")

	iamSACmd.AddCommand(iamSACreateCmd, iamSAListCmd, iamSAGetCmd, iamSADeleteCmd)
	iamKeyCmd.AddCommand(iamKeyCreateCmd, iamKeyRevokeCmd)
	iamGrantCmd.AddCommand(iamGrantPutCmd, iamGrantDeleteCmd, iamGrantListCmd)
	iamCmd.AddCommand(iamSACmd, iamKeyCmd, iamGrantCmd)
	rootCmd.AddCommand(iamCmd)
}
