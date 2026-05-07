package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/clusteradmin"
)

type transferOpts struct {
	Wait    bool
	Timeout time.Duration
}

var clusterTransferLeaderCmd = &cobra.Command{
	Use:   "transfer-leader",
	Short: "Trigger immediate leadership transfer to another voter",
	Long: `Send a TimeoutNow RPC to a peer to trigger immediate election.
Raft picks the best peer (highest matchIndex) as the target — no manual
target selection. Useful for graceful node maintenance: transfer leadership
away before stopping the current leader.

Status code policy:
  not currently the leader → 409 with leader_id hint
  single-node mode         → 503
  race during transfer     → 409 with retry hint`,
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := clusterClientFromCmd(cmd)
		if err != nil {
			return err
		}
		opts := transferOpts{}
		opts.Wait, _ = cmd.Flags().GetBool("wait")
		opts.Timeout, _ = cmd.Flags().GetDuration("timeout")
		return runClusterTransferLeader(cmd.Context(), client, opts, cmd.OutOrStdout())
	},
}

func runClusterTransferLeader(ctx context.Context, client *clusteradmin.Client, opts transferOpts, w io.Writer) error {
	res, err := client.TransferLeader(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "old_leader: %s\n", res.OldLeader)
	fmt.Fprintf(w, "term:       %d (pre-transfer)\n", res.Term)
	if res.TargetHint != "" {
		fmt.Fprintf(w, "target hint: %s\n", res.TargetHint)
	}
	if !opts.Wait {
		fmt.Fprintln(w, "(use --wait to block until new leader confirmed)")
		return nil
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	fmt.Fprintf(w, "waiting for leader change (timeout %s)...\n", timeout)
	for time.Now().Before(deadline) {
		s, err := client.Status(ctx)
		if err == nil && s.LeaderID != "" && s.LeaderID != res.OldLeader && s.Term > res.Term {
			fmt.Fprintf(w, "new leader: %s (term %d)\n", s.LeaderID, s.Term)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errors.New("timed out waiting for leader change")
}

type drainOpts struct {
	Yes     bool
	Timeout time.Duration
}

var clusterDrainCmd = &cobra.Command{
	Use:   "drain <id>",
	Short: "Remove a voter via graceful transfer-leader + remove-peer composite",
	Long: `Composite orchestration:
  1. Status fetch — identify current leader
  2. If <id> is the leader: transfer-leader + wait for new leader
  3. Remove-peer <id> via joint consensus

Partial failure semantics (A2-a):
  - Transfer fail            → STOP. Voter set unchanged.
  - Transfer ok + remove fail → progressive feedback. Cluster left with new
    leader but <id> still in voter set. Operator can retry remove-peer
    against the new leader.

Self-drain is allowed (T1-a) — admin socket may close as the node leaves
the cluster after removal. Run with --yes to skip the leader confirmation.`,
	Args: cobra.ExactArgs(1),
	Example: `  # Drain a follower (no transfer needed)
  grainfs cluster --endpoint <data>/admin.sock drain node-2 --yes

  # Drain the current leader (transfer first, then remove)
  grainfs cluster --endpoint <data>/admin.sock drain node-1 --yes`,
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := clusterClientFromCmd(cmd)
		if err != nil {
			return err
		}
		opts := drainOpts{}
		opts.Yes, _ = cmd.Flags().GetBool("yes")
		opts.Timeout, _ = cmd.Flags().GetDuration("timeout")
		return runClusterDrain(cmd.Context(), client, args[0], opts, cmd.OutOrStdout())
	},
}

func runClusterDrain(ctx context.Context, client *clusteradmin.Client, id string, opts drainOpts, w io.Writer) error {
	fmt.Fprintf(w, "target:    %s\n", id)
	s, err := client.Status(ctx)
	if err != nil {
		return fmt.Errorf("fetch cluster status: %w", err)
	}
	isLeader := s.LeaderID == id
	fmt.Fprintf(w, "is leader: %v\n", isLeader)

	if isLeader {
		if !opts.Yes {
			return fmt.Errorf("draining the current leader is high-risk; pass --yes to confirm")
		}
		fmt.Fprintln(w, "transferring leadership...")
		if _, terr := client.TransferLeader(ctx); terr != nil {
			return fmt.Errorf("transfer-leader: %w (voter set unchanged)", terr)
		}
		timeout := opts.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		deadline := time.Now().Add(timeout)
		oldTerm := s.Term
		var newLeader string
		var newTerm uint64
		for time.Now().Before(deadline) {
			ns, sErr := client.Status(ctx)
			if sErr == nil && ns.LeaderID != "" && ns.LeaderID != id && ns.Term > oldTerm {
				newLeader = ns.LeaderID
				newTerm = ns.Term
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		if newLeader == "" {
			return errors.New("timed out waiting for leader change after transfer")
		}
		fmt.Fprintf(w, "leadership transferred to %s (term %d)\n", newLeader, newTerm)
	}

	fmt.Fprintf(w, "removing voter %s...\n", id)
	rerr := client.RemovePeer(ctx, id, false /* force=false */)
	if rerr != nil {
		if isLeader {
			fmt.Fprintln(w, "leadership transferred but voter removal failed:")
			fmt.Fprintf(w, "  error: %v\n", rerr)
			fmt.Fprintf(w, "  cluster status: leader changed to a different node; %s still in voter set\n", id)
			fmt.Fprintln(w, "  investigate or retry: cluster --endpoint <leader-admin.sock> remove-peer "+id)
		}
		return fmt.Errorf("remove-peer: %w", rerr)
	}
	fmt.Fprintf(w, "drained — rejoin with: grainfs join <leader-addr> --data-dir <data> ...\n")
	return nil
}

func init() {
	clusterTransferLeaderCmd.Flags().Bool("wait", false, "Block until new leader confirmed via status polling")
	clusterTransferLeaderCmd.Flags().Duration("timeout", 30*time.Second, "--wait timeout")
	clusterDrainCmd.Flags().BoolP("yes", "y", false, "Confirm leader-drain without prompt")
	clusterDrainCmd.Flags().Duration("timeout", 30*time.Second, "Transfer-leader wait timeout after transfer")
}
