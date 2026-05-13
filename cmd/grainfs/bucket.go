package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/adminapi"
)

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Bucket-scoped admin commands (upstream credentials, etc.)",
}

func init() {
	bucketCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (overrides GRAINFS_ADMIN_SOCKET env var)")
	bucketCmd.PersistentFlags().Bool("json", false, "output raw JSON")
	bucketCmd.AddCommand(
		bucketCreateCmd(),
		bucketListCmd(),
		bucketDeleteCmd(),
		bucketInfoCmd(),
		bucketUpstreamCmd(),
	)
	rootCmd.AddCommand(bucketCmd)
}

func bucketCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create a bucket",
		Args:  cobra.ExactArgs(1),
		Example: `  grainfs bucket create my-bucket
  GRAINFS_ADMIN_SOCKET=./tmp/admin.sock grainfs bucket create my-bucket`,
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
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			fmt.Fprintf(c.OutOrStdout(), "Created bucket %s\n", args[0])
			return nil
		},
	}
}

func bucketListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List buckets (internal __grainfs_* buckets are always excluded)",
		Example: `  grainfs bucket list
  grainfs bucket --json list`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/buckets", nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var resp adminapi.ListBucketsAdminResp
			if err := json.Unmarshal(out, &resp); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tHAS_UPSTREAM")
			for _, b := range resp.Buckets {
				upstream := "no"
				if b.HasUpstream {
					upstream = "yes"
				}
				fmt.Fprintf(tw, "%s\t%s\n", b.Name, upstream)
			}
			return tw.Flush()
		},
	}
}

func bucketDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a bucket (must be empty unless --force)",
		Args:  cobra.ExactArgs(1),
		Example: `  grainfs bucket delete my-bucket
  grainfs bucket delete --force my-bucket`,
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
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Deleted bucket %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().Bool("force", false, "remove all objects then delete the bucket (one Raft commit per object — avoid on large buckets)")
	return cmd
}

func bucketInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <name>",
		Short: "Show bucket details (name + object count)",
		Args:  cobra.ExactArgs(1),
		Example: `  grainfs bucket info my-bucket
  grainfs bucket --json info my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET", "/v1/buckets/"+url.PathEscape(args[0]), nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var info adminapi.BucketInfo
			if err := json.Unmarshal(out, &info); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tOBJECTS\tVERSIONING\tHAS_UPSTREAM")
			objects := "<unknown>"
			if info.ObjectCount != nil {
				objects = fmt.Sprintf("%d", *info.ObjectCount)
			}
			versioning := "-"
			if info.Versioning != "" {
				versioning = info.Versioning
			}
			upstream := "no"
			if info.HasUpstream {
				upstream = "yes"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", info.Name, objects, versioning, upstream)
			return tw.Flush()
		},
	}
}
