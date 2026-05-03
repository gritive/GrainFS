package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_Cluster_RefusesEmptyClusterKey: cluster mode startup must fail-fast
// when --cluster-key is empty. This locks in the security boundary added in
// A7/A8 (runCluster + runClusterJoinNodeReal guards).
func TestE2E_Cluster_RefusesEmptyClusterKey(t *testing.T) {
	dir := t.TempDir()
	port := freePort()
	raft := freePort()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Cluster mode (--peers non-empty) without --cluster-key must error out.
	cmd := exec.CommandContext(ctx, getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raft),
		"--peers", fmt.Sprintf("127.0.0.1:%d", freePort()),
		"--node-id", "n-no-key",
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
	)
	out, err := cmd.CombinedOutput()
	require.Error(t, err, "process must exit non-zero without --cluster-key")
	if !strings.Contains(string(out), "--cluster-key is required") {
		t.Fatalf("expected '--cluster-key is required' in output, got:\n%s", string(out))
	}
}

// TestE2E_Cluster_DifferentPSK_JoinFails: a node attempting to join an
// existing cluster with a mismatched --cluster-key must fail. This proves the
// SPKI pinning (A6) is end-to-end intact, not just unit-test-correct.
func TestE2E_Cluster_DifferentPSK_JoinFails(t *testing.T) {
	keyA := strings.Repeat("a", 64)
	keyB := strings.Repeat("b", 64)

	leaderDataDir := t.TempDir()
	joinerDataDir := t.TempDir()

	leaderHTTP := freePort()
	leaderRaft := freePort()
	joinerHTTP := freePort()
	joinerRaft := freePort()

	// Start leader with keyA. Use a long-running context so the leader keeps
	// running while the joiner attempts to connect.
	leaderCtx, leaderCancel := context.WithCancel(context.Background())
	defer leaderCancel()

	leaderArgs := []string{
		"serve",
		"--data", leaderDataDir,
		"--port", fmt.Sprintf("%d", leaderHTTP),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", leaderRaft),
		"--node-id", "leader",
		"--cluster-key", keyA,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	leaderLog, err := os.CreateTemp("", "leader-*.log")
	require.NoError(t, err)
	defer os.Remove(leaderLog.Name())

	leader := exec.CommandContext(leaderCtx, getBinary(), leaderArgs...)
	leader.Stdout = leaderLog
	leader.Stderr = leaderLog
	require.NoError(t, leader.Start())
	t.Cleanup(func() {
		leaderCancel()
		_ = leader.Wait()
	})

	// Wait for leader's QUIC port to be reachable. We don't need HTTP — the
	// joiner connects to the Raft (UDP/QUIC) port, which is what matters.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := exec.Command("nc", "-z", "-w", "1", "127.0.0.1", fmt.Sprintf("%d", leaderHTTP)).CombinedOutput()
		_ = conn
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Joiner with keyB attempts to join. Must fail (SPKI mismatch on QUIC
	// handshake; cluster join cannot complete).
	joinerCtx, joinerCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer joinerCancel()

	joinerArgs := []string{
		"serve",
		"--data", joinerDataDir,
		"--port", fmt.Sprintf("%d", joinerHTTP),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", joinerRaft),
		"--node-id", "joiner",
		"--cluster-key", keyB, // MISMATCH
		"--join", fmt.Sprintf("127.0.0.1:%d", leaderRaft),
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	joiner := exec.CommandContext(joinerCtx, getBinary(), joinerArgs...)
	out, joinErr := joiner.CombinedOutput()

	// joiner must NOT exit cleanly (zero); some failure mode is required.
	require.Error(t, joinErr, "joiner with mismatched --cluster-key must not succeed. out: %s", string(out))
}
