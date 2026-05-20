package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/bucketadmin"
)

var bucketVersioningParentCmd = &cobra.Command{
	Use:   "versioning",
	Short: "Manage bucket versioning state",
}

var bucketVersioningGetCmd = &cobra.Command{
	Use:   "get <bucket>",
	Short: "Get the current versioning state of a bucket",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket versioning get my-bucket
  grainfs bucket --json versioning get my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunVersioningGet(c.Context(), bucketadmin.VersioningGetOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

var bucketVersioningEnableCmd = &cobra.Command{
	Use:     "enable <bucket>",
	Short:   "Enable versioning for a bucket",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs bucket versioning enable my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunVersioningEnable(c.Context(), bucketadmin.VersioningEnableOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

var bucketVersioningSuspendCmd = &cobra.Command{
	Use:     "suspend <bucket>",
	Short:   "Suspend versioning for a bucket",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs bucket versioning suspend my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunVersioningSuspend(c.Context(), bucketadmin.VersioningSuspendOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

func init() {
	bucketVersioningParentCmd.AddCommand(
		bucketVersioningGetCmd,
		bucketVersioningEnableCmd,
		bucketVersioningSuspendCmd,
	)
	bucketCmd.AddCommand(bucketVersioningParentCmd)
}
