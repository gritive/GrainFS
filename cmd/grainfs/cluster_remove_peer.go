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

The endpoint is admin-restricted to localhost — run this on the leader node.
Pre-flight refuses the request if it would drop live voters below the
post-removal quorum; pass --force to override (e.g., evicting a permanently
dead peer when ChangeMembership can still commit).`,
		Args: cobra.ExactArgs(1),
		Example: `  # 죽은 노드 축출 (정족수 손실 없음)
  grainfs cluster remove-peer node-3 --yes

  # 정족수 미달이지만 강제 (재해 복구 직전 단계)
  grainfs cluster remove-peer node-2 --force --yes`,
		RunE: runClusterRemovePeer,
	}
	cmd.Flags().String("endpoint", "http://127.0.0.1:9000", "GrainFS server endpoint (must be localhost)")
	cmd.Flags().Bool("force", false, "Bypass pre-flight quorum check")
	cmd.Flags().BoolP("yes", "y", false, "Skip interactive confirmation")
	cmd.Flags().Duration("timeout", 30*time.Second, "Request timeout")
	return cmd
}

func runClusterRemovePeer(cmd *cobra.Command, args []string) error {
	endpoint, _ := cmd.Flags().GetString("endpoint")
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
