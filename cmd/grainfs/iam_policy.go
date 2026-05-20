package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamPolicyCmd = &cobra.Command{
	Use:   "policy",
	Short: "Manage IAM policies (put/delete/attach/detach/list/validate/simulate)",
}

var iamPolicyPutCmd = &cobra.Command{
	Use:   "put <name>",
	Short: "Upload or replace a custom policy",
	Example: `  grainfs iam policy put my-pol --file my-pol.json
  grainfs iam --json policy put my-pol --file my-pol.json`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		file, _ := c.Flags().GetString("file")
		return iamadmin.RunPolicyPut(c.Context(), iamadmin.PolicyPutOptions{
			BaseOptions: base, Name: args[0], FilePath: file,
		})
	},
}

var iamPolicyGetCmd = &cobra.Command{
	Use:   "get <name>",
	Short: "Fetch and print a policy document",
	Example: `  grainfs iam policy get readonly
  grainfs iam policy get my-pol`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunPolicyGet(c.Context(), iamadmin.PolicyGetOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamPolicyListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all policy names",
	Example: `  grainfs iam policy list
  grainfs iam --json policy list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunPolicyList(c.Context(), iamadmin.PolicyListOptions{BaseOptions: base})
	},
}

var iamPolicyDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Short:   "Delete a custom policy (built-in names are refused by the server)",
	Example: `  grainfs iam policy delete my-pol`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunPolicyDelete(c.Context(), iamadmin.PolicyDeleteOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamPolicyAttachCmd = &cobra.Command{
	Use:   "attach <policy>",
	Short: "Attach a policy to a ServiceAccount (--sa) or group (--group)",
	Example: `  grainfs iam policy attach readonly --sa sa-abc123
  grainfs iam policy attach my-pol --sa sa-abc123 --i-know`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		said, _ := c.Flags().GetString("sa")
		groupID, _ := c.Flags().GetString("group")
		iKnow, _ := c.Flags().GetBool("i-know")
		return iamadmin.RunPolicyAttach(c.Context(), iamadmin.PolicyAttachOptions{
			BaseOptions: base,
			PolicyName:  args[0],
			SAID:        said,
			GroupID:     groupID,
			IKnow:       iKnow,
		})
	},
}

var iamPolicyDetachCmd = &cobra.Command{
	Use:     "detach <policy>",
	Short:   "Detach a policy from a ServiceAccount (--sa) or group (--group)",
	Example: `  grainfs iam policy detach readonly --sa sa-abc123`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		said, _ := c.Flags().GetString("sa")
		groupID, _ := c.Flags().GetString("group")
		return iamadmin.RunPolicyDetach(c.Context(), iamadmin.PolicyDetachOptions{
			BaseOptions: base,
			PolicyName:  args[0],
			SAID:        said,
			GroupID:     groupID,
		})
	},
}

var iamPolicyValidateCmd = &cobra.Command{
	Use:     "validate",
	Short:   "Validate a policy file locally (no server call)",
	Example: `  grainfs iam policy validate --file my-pol.json`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			// For validate we don't need a server endpoint; ignore endpoint errors.
			base.Endpoint = ""
			base.Stdout = c.OutOrStdout()
			base.Stderr = c.ErrOrStderr()
		}
		file, _ := c.Flags().GetString("file")
		return iamadmin.RunPolicyValidate(c.Context(), iamadmin.PolicyValidateOptions{
			BaseOptions: base, FilePath: file,
		})
	},
}

var iamPolicySimulateCmd = &cobra.Command{
	Use:   "simulate",
	Short: "Simulate a policy decision for a ServiceAccount",
	Example: `  grainfs iam policy simulate --sa sa-abc123 --action s3:GetObject --resource arn:aws:s3:::my-bucket/*
  grainfs iam --json policy simulate --sa sa-abc123 --action s3:PutObject --resource arn:aws:s3:::my-bucket/key`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		said, _ := c.Flags().GetString("sa")
		action, _ := c.Flags().GetString("action")
		resource, _ := c.Flags().GetString("resource")
		return iamadmin.RunPolicySimulate(c.Context(), iamadmin.PolicySimulateOptions{
			BaseOptions: base,
			SAID:        said,
			Action:      action,
			Resource:    resource,
		})
	},
}

func init() {
	iamPolicyPutCmd.Flags().String("file", "", "path to policy JSON file (required)")
	_ = iamPolicyPutCmd.MarkFlagRequired("file")

	iamPolicyValidateCmd.Flags().String("file", "", "path to policy JSON file (required)")
	_ = iamPolicyValidateCmd.MarkFlagRequired("file")

	iamPolicyAttachCmd.Flags().String("sa", "", "ServiceAccount ID to attach to")
	iamPolicyAttachCmd.Flags().Bool("i-know", false, "suppress Resource:* scope warning")

	iamPolicyDetachCmd.Flags().String("sa", "", "ServiceAccount ID to detach from")

	iamPolicySimulateCmd.Flags().String("sa", "", "ServiceAccount ID")
	iamPolicySimulateCmd.Flags().String("action", "", "IAM action (e.g. s3:GetObject)")
	iamPolicySimulateCmd.Flags().String("resource", "*", "resource ARN or *")

	iamPolicyCmd.AddCommand(
		iamPolicyPutCmd,
		iamPolicyGetCmd,
		iamPolicyListCmd,
		iamPolicyDeleteCmd,
		iamPolicyAttachCmd,
		iamPolicyDetachCmd,
		iamPolicyValidateCmd,
		iamPolicySimulateCmd,
	)
	iamCmd.AddCommand(iamPolicyCmd)
}
