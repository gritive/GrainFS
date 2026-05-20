package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamBucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Manage buckets via IAM (create with optional attach-sa/policy, delete, policy, list)",
}

var iamBucketPolicyCmd = &cobra.Command{
	Use:   "policy",
	Short: "Manage IAM-attached bucket policies (put/delete)",
}

var iamBucketCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a bucket (optionally attach an SA+policy in one step)",
	Example: `  grainfs iam bucket create my-bucket
  grainfs iam bucket create analytics --attach-sa sa-abc123 --attach-policy readwrite`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		attachSA, _ := c.Flags().GetString("attach-sa")
		attachPolicy, _ := c.Flags().GetString("attach-policy")
		if (attachSA == "") != (attachPolicy == "") {
			return fmt.Errorf("--attach-sa and --attach-policy must be provided together")
		}
		return iamadmin.RunBucketCreate(c.Context(), iamadmin.BucketCreateOptions{
			BaseOptions:  base,
			Name:         args[0],
			AttachSA:     attachSA,
			AttachPolicy: attachPolicy,
		})
	},
}

var iamBucketDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Short:   "Delete a bucket (must be empty unless --force)",
	Example: `  grainfs iam bucket delete my-bucket`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		force, _ := c.Flags().GetBool("force")
		return iamadmin.RunBucketDelete(c.Context(), iamadmin.BucketDeleteOptions{
			BaseOptions: base,
			Name:        args[0],
			Force:       force,
		})
	},
}

var iamBucketListCmd = &cobra.Command{
	Use:   "list",
	Short: "List buckets (internal __grainfs_* buckets are always excluded)",
	Example: `  grainfs iam bucket list
  grainfs iam --json bucket list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunBucketList(c.Context(), iamadmin.BucketListOptions{BaseOptions: base})
	},
}

var iamBucketPolicyPutCmd = &cobra.Command{
	Use:     "put <bucket> --file <path>",
	Short:   "Upload a bucket IAM policy from a JSON file",
	Example: `  grainfs iam bucket policy put my-bucket --file policy.json`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		filePath, _ := c.Flags().GetString("file")
		policy, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("read policy file: %w", err)
		}
		return iamadmin.RunBucketPolicyPut(c.Context(), iamadmin.BucketPolicyPutOptions{
			BaseOptions: base,
			Bucket:      args[0],
			Policy:      policy,
		})
	},
}

var iamBucketPolicyDeleteCmd = &cobra.Command{
	Use:     "delete <bucket>",
	Short:   "Remove a bucket IAM policy",
	Example: `  grainfs iam bucket policy delete my-bucket`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunBucketPolicyDelete(c.Context(), iamadmin.BucketPolicyDeleteOptions{
			BaseOptions: base,
			Bucket:      args[0],
		})
	},
}

func init() {
	iamBucketCreateCmd.Flags().String("attach-sa", "", "service account ID to attach an initial bucket policy to")
	iamBucketCreateCmd.Flags().String("attach-policy", "", "policy name to attach with --attach-sa (required when --attach-sa is set)")
	iamBucketDeleteCmd.Flags().Bool("force", false, "remove all objects then delete the bucket")
	iamBucketPolicyPutCmd.Flags().String("file", "", "path to the JSON policy document (required)")
	_ = iamBucketPolicyPutCmd.MarkFlagRequired("file")

	iamBucketPolicyCmd.AddCommand(iamBucketPolicyPutCmd, iamBucketPolicyDeleteCmd)
	iamBucketCmd.AddCommand(
		iamBucketCreateCmd,
		iamBucketDeleteCmd,
		iamBucketListCmd,
		iamBucketPolicyCmd,
	)
	iamCmd.AddCommand(iamBucketCmd)
}
