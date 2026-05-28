package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func init() {
	clusterCmd.AddCommand(clusterCompleteCutoverCmd())
}

func clusterCompleteCutoverCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "complete-cutover",
		Short: "Complete Zero-CA TLS cutover by dropping the cluster key",
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := clusterEndpointFromCmd(cmd)
			if err != nil {
				return err
			}
			return clusteradmin.RunCompleteCutover(cmd.Context(), clusteradmin.CompleteCutoverOptions{
				Endpoint: endpoint,
				Out:      cmd.OutOrStdout(),
			})
		},
	}
}
