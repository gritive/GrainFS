package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClusterRefusesEmptyClusterKeyE2E: cluster mode startup must fail-fast
// when --cluster-key is empty. Join mode (triggered by .join-pending) is the
// only cluster mode; without --cluster-key the boot must error out.
func TestClusterRefusesEmptyClusterKeyE2E(t *testing.T) {
	dir := t.TempDir()
	encKeyFile := makeSharedEncryptionKeyFile(t)
	port := freePort()
	raft := freePort()

	// Write .join-pending to trigger join mode (which requires --cluster-key).
	require.NoError(t, os.WriteFile(
		fmt.Sprintf("%s/%s", dir, joinPendingFile),
		[]byte(fmt.Sprintf("127.0.0.1:%d", freePort())), 0o600))

	cmd := exec.Command(getBinary(), "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raft),
		"--node-id", "n-no-key",
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--encryption-key-file", encKeyFile,
	)
	out, err := cmd.CombinedOutput()
	require.Error(t, err, "process must exit non-zero without --cluster-key")
	if !strings.Contains(string(out), "--cluster-key is required") {
		t.Fatalf("expected '--cluster-key is required' in output, got:\n%s", string(out))
	}
}

// TestClusterDifferentPSKJoinFailsE2E: a node attempting to join an
// existing cluster with a mismatched --cluster-key must fail. This proves the
// SPKI pinning (A6) is end-to-end intact, not just unit-test-correct.
func TestClusterDifferentPSKJoinFailsE2E(t *testing.T) {
	keyA := strings.Repeat("a", 64)
	keyB := strings.Repeat("b", 64)

	leaderDataDir := shortTempDir(t)
	joinerDataDir := shortTempDir(t)
	encKeyFile := makeSharedEncryptionKeyFile(t)

	leaderHTTP := freePort()
	leaderRaft := freePort()
	joinerHTTP := freePort()
	joinerRaft := freePort()

	// Start leader with keyA (solo bootstrap).
	leaderCtx, leaderCancel := context.WithCancel(context.Background())

	leaderArgs := []string{
		"serve",
		"--data", leaderDataDir,
		"--port", fmt.Sprintf("%d", leaderHTTP),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", leaderRaft),
		"--node-id", "leader",
		"--cluster-key", keyA,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	leaderLog, err := os.CreateTemp("", "leader-*.log")
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			if b, err := os.ReadFile(leaderLog.Name()); err == nil {
				t.Logf("leader log:\n%s", b)
			}
		}
		os.Remove(leaderLog.Name())
	})

	leader := exec.CommandContext(leaderCtx, getBinary(), leaderArgs...)
	leader.Stdout = leaderLog
	leader.Stderr = leaderLog
	require.NoError(t, leader.Start())
	t.Cleanup(func() {
		leaderCancel()
		_ = leader.Wait()
	})

	waitForPort(t, leaderHTTP, 15*time.Second)

	// Joiner with keyB: write .join-pending pointing to leader, then boot.
	// Must fail (SPKI mismatch on QUIC handshake; cluster join cannot complete).
	require.NoError(t, os.WriteFile(
		fmt.Sprintf("%s/%s", joinerDataDir, joinPendingFile),
		[]byte(fmt.Sprintf("127.0.0.1:%d", leaderRaft)), 0o600))

	joinerCtx, joinerCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer joinerCancel()

	joinerArgs := []string{
		"serve",
		"--data", joinerDataDir,
		"--port", fmt.Sprintf("%d", joinerHTTP),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", joinerRaft),
		"--node-id", "joiner",
		"--cluster-key", keyB, // MISMATCH
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--encryption-key-file", encKeyFile,
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	joiner := exec.CommandContext(joinerCtx, getBinary(), joinerArgs...)
	out, joinErr := combinedOutputWithWaitDelay(joiner)

	require.Error(t, joinErr, "joiner with mismatched --cluster-key must not succeed. out: %s", string(out))
	require.False(t, errors.Is(joinerCtx.Err(), context.DeadlineExceeded), "joiner must fail from PSK rejection, not from test timeout. out: %s", string(out))
	require.Contains(t, string(out), "peer cert SPKI", "joiner should surface the PSK/SPKI rejection. out: %s", string(out))
}
