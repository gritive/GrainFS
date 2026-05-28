package main

import (
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func init() {
	clusterCmd.AddCommand(clusterRevokeNodeCmd())
}

func clusterRevokeNodeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke-node <node-id>",
		Short: "Revoke a Zero-CA node identity and remove it from the cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := clusterEndpointFromCmd(cmd)
			if err != nil {
				return err
			}
			return clusteradmin.RunRevokeNode(cmd.Context(), clusteradmin.RevokeNodeOptions{
				Endpoint: endpoint,
				NodeID:   args[0],
				Out:      cmd.OutOrStdout(),
			})
		},
	}
}
