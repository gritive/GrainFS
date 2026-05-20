package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/bucketadmin"
)

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Bucket-scoped admin commands (upstream credentials, etc.)",
}

// bucketBaseOptionsFromCmd reads --endpoint and --json from the bucket
// command tree (PersistentFlags) and builds a populated BaseOptions.
func bucketBaseOptionsFromCmd(cmd *cobra.Command) (bucketadmin.BaseOptions, error) {
	ep, err := adminEndpointFromCmd(cmd)
	if err != nil {
		return bucketadmin.BaseOptions{}, err
	}
	asJSON, _ := cmd.Flags().GetBool("json")
	return bucketadmin.BaseOptions{
		Endpoint: ep,
		JSONOut:  asJSON,
		Stdout:   cmd.OutOrStdout(),
		Stderr:   cmd.ErrOrStderr(),
	}, nil
}

var bucketCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a bucket",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket create my-bucket
  GRAINFS_ADMIN_SOCKET=./tmp/admin.sock grainfs bucket create my-bucket
  grainfs bucket create my-bucket --attach-sa sa_123 --attach-policy bucket-admin`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		attachSA, _ := c.Flags().GetString("attach-sa")
		attachPolicy, _ := c.Flags().GetString("attach-policy")
		if (attachSA == "") != (attachPolicy == "") {
			return fmt.Errorf("--attach-sa and --attach-policy must be provided together")
		}
		return bucketadmin.RunCreate(c.Context(), bucketadmin.CreateOptions{
			BaseOptions: base, Name: args[0],
			AttachSA: attachSA, AttachRole: attachPolicy,
		})
	},
}

var bucketListCmd = &cobra.Command{
	Use:   "list",
	Short: "List buckets (internal __grainfs_* buckets are always excluded)",
	Example: `  grainfs bucket list
  grainfs bucket --json list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunList(c.Context(), bucketadmin.ListOptions{BaseOptions: base})
	},
}

var bucketInfoCmd = &cobra.Command{
	Use:   "info <name>",
	Short: "Show bucket details (name + object count)",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket info my-bucket
  grainfs bucket --json info my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunInfo(c.Context(), bucketadmin.InfoOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var bucketDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a bucket (must be empty unless --force)",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket delete my-bucket
  grainfs bucket delete --force my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		force, _ := c.Flags().GetBool("force")
		recursive, _ := c.Flags().GetBool("recursive")
		return bucketadmin.RunDelete(c.Context(), bucketadmin.DeleteOptions{
			BaseOptions: base, Name: args[0],
			Force: force, Recursive: recursive,
		})
	},
}

func init() {
	bucketCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	bucketCmd.PersistentFlags().Bool("json", false, "output raw JSON")

	bucketCreateCmd.Flags().String("attach-sa", "", "service account ID to attach an initial bucket policy to")
	bucketCreateCmd.Flags().String("attach-policy", "", "built-in policy name to attach with --attach-sa")
	bucketDeleteCmd.Flags().Bool("force", false, "remove all objects then delete the bucket (one Raft commit per object — avoid on large buckets)")
	bucketDeleteCmd.Flags().Bool("recursive", false, "alias for --force; kept for parity with future cmd flag surface")

	bucketCmd.AddCommand(
		bucketCreateCmd, bucketListCmd, bucketInfoCmd, bucketDeleteCmd,
	)
	// The upstream/policy/versioning subtrees register themselves onto
	// bucketCmd from their own init() blocks (matching the iam.go template).
	rootCmd.AddCommand(bucketCmd)
}
