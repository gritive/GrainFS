package main

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func init() {
	clusterCmd.AddCommand(clusterInviteCmd())
}

func clusterInviteCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "invite",
		Short: "Zero-CA invite tooling (mint one-time join invites)",
	}
	c.AddCommand(clusterInviteCreateCmd())
	return c
}

func clusterInviteCreateCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "create",
		Short: "Mint a one-time invite and print the operator bundle (leader only)",
		Long: `Mints a single-use Ed25519 invite on the meta-raft leader and prints the
operator bundle token. Hand the token to the joining node out-of-band and set
it as GRAINFS_INVITE_BUNDLE before starting that node; it carries the seed
node's join-listener address + pinned SPKI and the cluster identity.

Run this against the leader's admin socket (--endpoint <data-dir>/admin.sock).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoint, err := clusterEndpointFromCmd(cmd)
			if err != nil {
				return err
			}
			ttl, _ := cmd.Flags().GetDuration("ttl")
			return clusteradmin.RunInviteCreate(cmd.Context(), clusteradmin.InviteCreateOptions{
				Endpoint: endpoint,
				TTL:      ttl,
				Out:      cmd.OutOrStdout(),
			})
		},
	}
	c.Flags().Duration("ttl", time.Hour, "invite lifetime before it expires")
	return c
}
