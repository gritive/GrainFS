package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func init() {
	clusterCmd.AddCommand(clusterVerifyPerVersionCmd())
}

func clusterVerifyPerVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify-per-version",
		Short: "Report per-version quorum-meta cutover readiness for this node",
		Long: `Scans this node's hosted-group buckets and reports how many object
versions have a readable per-version quorum-meta blob (S4a cutover gate).

Exits non-zero if gaps+stuck+unknown > 0.

Note: this is a node-local check. Cluster-wide cutover readiness requires
every node to report 0 gaps+stuck+unknown.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := clusterEndpointFromCmd(cmd)
			if err != nil {
				return err
			}
			bucket, _ := cmd.Flags().GetString("bucket")
			return clusteradmin.RunVerifyPerVersion(cmd.Context(), clusteradmin.VerifyPerVersionCutoverOptions{
				Endpoint: endpoint,
				Bucket:   bucket,
				Out:      cmd.OutOrStdout(),
			})
		},
	}
	cmd.Flags().String("bucket", "", "restrict scan to a single bucket (default: all hosted buckets)")
	return cmd
}
