package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func bucketVersioningCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "versioning",
		Short: "Manage bucket versioning state",
	}
	cmd.AddCommand(
		bucketVersioningGetCmd(),
		bucketVersioningEnableCmd(),
		bucketVersioningSuspendCmd(),
	)
	return cmd
}

func bucketVersioningGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get <bucket>",
		Short: "Get the current versioning state of a bucket",
		Args:  cobra.ExactArgs(1),
		Example: `  grainfs bucket versioning get my-bucket
  grainfs bucket --json versioning get my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			out, err := iamRequest(c.Context(), sock, "GET",
				"/v1/buckets/"+url.PathEscape(args[0])+"/versioning", nil)
			if err != nil {
				return err
			}
			asJSON, _ := c.Flags().GetBool("json")
			if asJSON {
				fmt.Fprintln(c.OutOrStdout(), string(out))
				return nil
			}
			var resp adminapi.BucketVersioningResp
			if err := json.Unmarshal(out, &resp); err != nil {
				return fmt.Errorf("parse response: %w", err)
			}
			status := resp.Status
			if status == "" {
				status = "Off"
			}
			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "BUCKET\tVERSIONING")
			fmt.Fprintf(tw, "%s\t%s\n", args[0], status)
			return tw.Flush()
		},
	}
}

func bucketVersioningEnableCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "enable <bucket>",
		Short:   "Enable versioning for a bucket",
		Args:    cobra.ExactArgs(1),
		Example: `  grainfs bucket versioning enable my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "PUT",
				"/v1/buckets/"+url.PathEscape(args[0])+"/versioning",
				map[string]string{"status": "Enabled"})
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Versioning enabled for bucket %s\n", args[0])
			return nil
		},
	}
}

func bucketVersioningSuspendCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "suspend <bucket>",
		Short:   "Suspend versioning for a bucket",
		Args:    cobra.ExactArgs(1),
		Example: `  grainfs bucket versioning suspend my-bucket`,
		RunE: func(c *cobra.Command, args []string) error {
			sock, err := adminEndpointFromCmd(c)
			if err != nil {
				return err
			}
			_, err = iamRequest(c.Context(), sock, "PUT",
				"/v1/buckets/"+url.PathEscape(args[0])+"/versioning",
				map[string]string{"status": "Suspended"})
			if err != nil {
				return err
			}
			fmt.Fprintf(c.OutOrStdout(), "Versioning suspended for bucket %s\n", args[0])
			return nil
		},
	}
}
