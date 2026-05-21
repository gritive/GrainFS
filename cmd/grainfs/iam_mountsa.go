package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/iamadmin"
)

var iamMountSACmd = &cobra.Command{
	Use:   "mount-sa",
	Short: "Manage MountSAs (NFS/9P mount service accounts)",
}

var iamMountSAPolicyCmd = &cobra.Command{
	Use:   "policy",
	Short: "Attach or detach policies from a MountSA",
}

var iamMountSACreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a NFS/9P mount service account",
	Example: `  grainfs iam mount-sa create nfs-client --uid 1000
  grainfs iam --json mount-sa create nfs-client --uid 1000`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		uid, _ := c.Flags().GetUint32("uid")
		createdBy, _ := c.Flags().GetString("created-by")
		return iamadmin.RunMountSACreate(c.Context(), iamadmin.MountSACreateOptions{
			BaseOptions: base,
			Name:        args[0],
			UID:         uid,
			CreatedBy:   createdBy,
		})
	},
}

var iamMountSAListCmd = &cobra.Command{
	Use:   "list",
	Short: "List MountSAs",
	Example: `  grainfs iam mount-sa list
  grainfs iam --json mount-sa list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunMountSAList(c.Context(), iamadmin.MountSAListOptions{BaseOptions: base})
	},
}

var iamMountSAGetCmd = &cobra.Command{
	Use:   "get <name>",
	Short: "Show MountSA detail",
	Example: `  grainfs iam mount-sa get nfs-client
  grainfs iam --json mount-sa get nfs-client`,
	Args: cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunMountSAGet(c.Context(), iamadmin.MountSAGetOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamMountSADeleteCmd = &cobra.Command{
	Use:     "delete <name>",
	Short:   "Delete a MountSA",
	Example: `  grainfs iam mount-sa delete nfs-client`,
	Args:    cobra.ExactArgs(1),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunMountSADelete(c.Context(), iamadmin.MountSADeleteOptions{
			BaseOptions: base, Name: args[0],
		})
	},
}

var iamMountSAPolicyAttachCmd = &cobra.Command{
	Use:     "attach <mount-sa> <policy>",
	Short:   "Attach a policy to a MountSA",
	Example: `  grainfs iam mount-sa policy attach nfs-client NFSMountOnly`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunMountSAPolicyAttach(c.Context(), iamadmin.MountSAPolicyAttachOptions{
			BaseOptions: base, MountSA: args[0], PolicyName: args[1],
		})
	},
}

var iamMountSAPolicyDetachCmd = &cobra.Command{
	Use:     "detach <mount-sa> <policy>",
	Short:   "Detach a policy from a MountSA",
	Example: `  grainfs iam mount-sa policy detach nfs-client NFSMountOnly`,
	Args:    cobra.ExactArgs(2),
	RunE: func(c *cobra.Command, args []string) error {
		base, err := iamBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return iamadmin.RunMountSAPolicyDetach(c.Context(), iamadmin.MountSAPolicyDetachOptions{
			BaseOptions: base, MountSA: args[0], PolicyName: args[1],
		})
	},
}

func init() {
	iamMountSACreateCmd.Flags().Uint32("uid", 0, "numeric UID for the NFS/9P principal (required)")
	iamMountSACreateCmd.Flags().String("created-by", "", "creator identifier (optional, informational)")

	iamMountSAPolicyCmd.AddCommand(iamMountSAPolicyAttachCmd, iamMountSAPolicyDetachCmd)
	iamMountSACmd.AddCommand(
		iamMountSACreateCmd,
		iamMountSAListCmd,
		iamMountSAGetCmd,
		iamMountSADeleteCmd,
		iamMountSAPolicyCmd,
	)
	iamCmd.AddCommand(iamMountSACmd)
}
