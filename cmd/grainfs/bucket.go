package main

import "github.com/spf13/cobra"

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Bucket-scoped admin commands (upstream credentials, etc.)",
}

func init() {
	bucketCmd.PersistentFlags().String("endpoint", "",
		"admin Unix socket path (required, e.g. ./tmp/admin.sock)")
	bucketCmd.AddCommand(bucketUpstreamCmd())
	rootCmd.AddCommand(bucketCmd)
}
