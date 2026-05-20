package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamGroupCmd = &cobra.Command{
	Use:   "group",
	Short: "Manage IAM groups (create/delete/member add|remove/policy attach|detach)",
}

var iamGroupMemberCmd = &cobra.Command{
	Use:   "member",
	Short: "Add or remove ServiceAccount members from a group",
}

var iamGroupPolicyCmd = &cobra.Command{
	Use:   "policy",
	Short: "Attach or detach policies from a group",
}

var iamGroupCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new group (empty; attach members/policies separately)",
	Example: `  grainfs iam group create data-team
  grainfs iam --json group create data-team`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupCreate(c.Context(), iamadmin.GroupCreateOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamGroupDeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Short:   "Delete a group",
	Example: `  grainfs iam group delete data-team`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupDelete(c.Context(), iamadmin.GroupDeleteOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamGroupMemberAddCmd = &cobra.Command{
	Use:     "add <group> <sa_id>",
	Short:   "Add a ServiceAccount to a group",
	Example: `  grainfs iam group member add data-team sa-abc123`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupMemberAdd(c.Context(), iamadmin.GroupMemberAddOptions{
			BaseOptions: base, GroupName: args[0], SAID: args[1],
		})
	},
}

var iamGroupMemberRemoveCmd = &cobra.Command{
	Use:     "remove <group> <sa_id>",
	Short:   "Remove a ServiceAccount from a group",
	Example: `  grainfs iam group member remove data-team sa-abc123`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupMemberRemove(c.Context(), iamadmin.GroupMemberRemoveOptions{
			BaseOptions: base, GroupName: args[0], SAID: args[1],
		})
	},
}

var iamGroupPolicyAttachCmd = &cobra.Command{
	Use:     "attach <group> <policy>",
	Short:   "Attach a policy to a group",
	Example: `  grainfs iam group policy attach data-team readonly`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupPolicyAttach(c.Context(), iamadmin.GroupPolicyAttachOptions{
			BaseOptions: base, GroupName: args[0], PolicyName: args[1],
		})
	},
}

var iamGroupPolicyDetachCmd = &cobra.Command{
	Use:     "detach <group> <policy>",
	Short:   "Detach a policy from a group",
	Example: `  grainfs iam group policy detach data-team readonly`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunGroupPolicyDetach(c.Context(), iamadmin.GroupPolicyDetachOptions{
			BaseOptions: base, GroupName: args[0], PolicyName: args[1],
		})
	},
}

func init() {
	iamGroupMemberCmd.AddCommand(iamGroupMemberAddCmd, iamGroupMemberRemoveCmd)
	iamGroupPolicyCmd.AddCommand(iamGroupPolicyAttachCmd, iamGroupPolicyDetachCmd)
	iamGroupCmd.AddCommand(
		iamGroupCreateCmd,
		iamGroupDeleteCmd,
		iamGroupMemberCmd,
		iamGroupPolicyCmd,
	)
	iamCmd.AddCommand(iamGroupCmd)
}
