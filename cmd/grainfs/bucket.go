package main

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Bucket-scoped admin commands (upstream credentials, etc.)",
}

func init() {
	bucketCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (required, e.g. ./tmp/admin.sock)")
	bucketCmd.AddCommand(bucketUpstreamCmd(), bucketCreateCmd(), bucketListCmd(), bucketDeleteCmd())
	rootCmd.AddCommand(bucketCmd)
}

func bucketCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create a bucket",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "POST", "/v1/buckets",
				map[string]string{"name": args[0]})
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}
}

func bucketListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List buckets (internal __grainfs_* buckets are always excluded)",
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/buckets", nil)
			if err != nil {
				return err
			}
			fmt.Fprintln(c.OutOrStdout(), string(out))
			return nil
		},
	}
}

func bucketDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a bucket (must be empty unless --force)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			force, _ := c.Flags().GetBool("force")
			path := "/v1/buckets/" + url.PathEscape(args[0])
			if force {
				path += "?force=true"
			}
			_, err = iamRequest(c.Context(), sock, "DELETE", path, nil)
			return err
		},
	}
	cmd.Flags().Bool("force", false, "remove all objects then delete the bucket")
	return cmd
}
