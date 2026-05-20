package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/bucketadmin"
)

var bucketUpstreamCmd = &cobra.Command{
	Use:   "upstream",
	Short: "Manage per-bucket pull-through upstream credentials",
}

var bucketUpstreamPutCmd = &cobra.Command{
	Use:   "put <bucket>",
	Short: "Register or rotate the upstream credentials for a bucket",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket upstream put my-bucket \
      --scheme s3 --endpoint-url https://s3.example.com \
      --access-key AKID --secret-key SK \
      --region us-east-1 --remote-bucket upstream-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		scheme, _ := c.Flags().GetString("scheme")
		endpoint, _ := c.Flags().GetString("endpoint-url")
		ak, _ := c.Flags().GetString("access-key")
		sk, _ := c.Flags().GetString("secret-key")
		region, _ := c.Flags().GetString("region")
		remoteBucket, _ := c.Flags().GetString("remote-bucket")
		return bucketadmin.RunUpstreamPut(c.Context(), bucketadmin.UpstreamPutOptions{
			BaseOptions:  base,
			Bucket:       args[0],
			Scheme:       scheme,
			Endpoint:     endpoint,
			AccessKey:    ak,
			SecretKey:    sk,
			Region:       region,
			RemoteBucket: remoteBucket,
		})
	},
}

var bucketUpstreamGetCmd = &cobra.Command{
	Use:   "get <bucket>",
	Short: "Show the upstream config for a bucket (secret_key never returned)",
	Args:  cobra.ExactArgs(1),
	Example: `  grainfs bucket upstream get my-bucket
  grainfs bucket --json upstream get my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunUpstreamGet(c.Context(), bucketadmin.UpstreamGetOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

var bucketUpstreamListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all bucket-upstream configurations",
	Example: `  grainfs bucket upstream list
  grainfs bucket --json upstream list`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunUpstreamList(c.Context(), bucketadmin.UpstreamListOptions{
			BaseOptions: base,
		})
	},
}

var bucketUpstreamDeleteCmd = &cobra.Command{
	Use:     "delete <bucket>",
	Short:   "Remove the upstream config for a bucket",
	Args:    cobra.ExactArgs(1),
	Example: `  grainfs bucket upstream delete my-bucket`,
	RunE: func(c *cobra.Command, args []string) error {
		base, err := bucketBaseOptionsFromCmd(c)
		if err != nil {
			return err
		}
		return bucketadmin.RunUpstreamDelete(c.Context(), bucketadmin.UpstreamDeleteOptions{
			BaseOptions: base, Bucket: args[0],
		})
	},
}

func init() {
	bucketUpstreamPutCmd.Flags().String("scheme", "s3",
		"upstream scheme (s3 or r2)")
	bucketUpstreamPutCmd.Flags().String("endpoint-url", "",
		"upstream S3 endpoint URL (e.g., https://s3.example.com)")
	bucketUpstreamPutCmd.Flags().String("access-key", "",
		"upstream access key")
	bucketUpstreamPutCmd.Flags().String("secret-key", "",
		"upstream secret key")
	bucketUpstreamPutCmd.Flags().String("region", "",
		"upstream region (e.g., us-east-1)")
	bucketUpstreamPutCmd.Flags().String("remote-bucket", "",
		"upstream bucket name on the remote (defaults to the local bucket name)")

	bucketUpstreamCmd.AddCommand(
		bucketUpstreamPutCmd,
		bucketUpstreamGetCmd,
		bucketUpstreamListCmd,
		bucketUpstreamDeleteCmd,
	)
	bucketCmd.AddCommand(bucketUpstreamCmd)
}
