package main

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

func clusterRemovePeerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-peer <id>",
		Short: "Remove a voter from the meta-Raft cluster (joint consensus)",
		Long: `Remove a voter from the meta-Raft cluster via §4.3 joint consensus.

The endpoint is the admin Unix socket — run this on the leader node.
Pre-flight refuses the request if it would drop live voters below the
post-removal quorum; pass --force to override (e.g., evicting a permanently
dead peer when ChangeMembership can still commit).`,
		Args: cobra.ExactArgs(1),
		Example: `  # Evict a dead voter (quorum stays intact)
  grainfs cluster --endpoint <data-dir>/admin.sock remove-peer node-3 --yes

  # Force-evict despite quorum loss (last-resort disaster recovery)
  grainfs cluster --endpoint <data-dir>/admin.sock remove-peer node-2 --force --yes`,
		RunE: runClusterRemovePeer,
	}
	cmd.Flags().Bool("force", false, "Bypass pre-flight quorum check")
	cmd.Flags().BoolP("yes", "y", false, "Skip interactive confirmation")
	cmd.Flags().Duration("timeout", 30*time.Second, "Request timeout")
	return cmd
}

func runClusterRemovePeer(cmd *cobra.Command, args []string) error {
	endpoint, err := clusterEndpointFromCmd(cmd)
	if err != nil {
		return err
	}
	force, _ := cmd.Flags().GetBool("force")
	assumeYes, _ := cmd.Flags().GetBool("yes")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	return clusteradmin.RemovePeer(cmd.Context(), clusteradmin.RemovePeerOptions{
		Endpoint:  endpoint,
		ID:        args[0],
		Force:     force,
		AssumeYes: assumeYes,
		Timeout:   timeout,
		Stdout:    cmd.OutOrStdout(),
		Stderr:    cmd.ErrOrStderr(),
		Stdin:     cmd.InOrStdin(),
	})
}
