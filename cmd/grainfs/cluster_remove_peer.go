package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
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
	id := args[0]
	endpoint, _ := cmd.Flags().GetString("endpoint")
	force, _ := cmd.Flags().GetBool("force")
	assumeYes, _ := cmd.Flags().GetBool("yes")
	timeout, _ := cmd.Flags().GetDuration("timeout")

	client := clusteradmin.NewClient(endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	status, err := client.Status(ctx)
	if err != nil {
		return fmt.Errorf("fetch cluster status: %w", err)
	}
	if status.Mode != "cluster" {
		return fmt.Errorf("node is not in cluster mode (mode=%q); remove-peer requires a Raft cluster", status.Mode)
	}
	if !clusteradmin.Contains(status.Peers, id) {
		return fmt.Errorf("peer %q is not a current voter; current peers: %s", id, strings.Join(status.Peers, ", "))
	}
	if status.State != "Leader" {
		return fmt.Errorf("connected node is not the leader (state=%s); rerun on leader %q", status.State, status.LeaderID)
	}

	live := clusteradmin.LivePeersFromStatus(status.Peers, status.DownNodes, status.NodeID)
	pre := clusteradmin.Check(status.Peers, live, id)

	fmt.Fprintf(cmd.ErrOrStderr(), "Removing %s from cluster:\n  %s\n", id, pre.Summary())
	if pre.WouldBlock && !force {
		return fmt.Errorf("pre-flight: alive_after (%d) < new_quorum (%d); rerun with --force to override",
			pre.AliveAfter, pre.NewQuorum)
	}

	if !assumeYes {
		fmt.Fprint(cmd.ErrOrStderr(), "Proceed? [y/N]: ")
		ans, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
		ans = strings.TrimSpace(strings.ToLower(ans))
		if ans != "y" && ans != "yes" {
			return errors.New("aborted")
		}
	}

	if err := client.RemovePeer(ctx, id, force); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", id)
	return nil
}
