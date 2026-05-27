package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestClassifyInviteJoinResume is the pure resume-gate table.
func TestClassifyInviteJoinResume(t *testing.T) {
	tests := []struct {
		name         string
		bundle       bool
		persistedKey bool
		complete     bool
		acked        bool
		want         inviteJoinDecision
	}{
		{"no bundle -> normal", false, false, false, false, inviteNormalBoot},
		{"no bundle even if durable -> normal", false, true, true, true, inviteNormalBoot},
		{"bundle, no key -> fresh", true, false, false, false, inviteFreshJoin},
		{"bundle, key, incomplete -> resume", true, true, false, false, inviteResume},
		{"bundle, key, complete but not acked -> resume", true, true, true, false, inviteResume},
		{"bundle, key, complete and acked -> normal", true, true, true, true, inviteNormalBoot},
		{"bundle, key, acked but incomplete -> resume", true, true, false, true, inviteResume},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyInviteJoinResume(tt.bundle, tt.persistedKey, tt.complete, tt.acked)
			if got != tt.want {
				t.Fatalf("classifyInviteJoinResume(%v,%v,%v,%v) = %d, want %d",
					tt.bundle, tt.persistedKey, tt.complete, tt.acked, got, tt.want)
			}
		})
	}
}

// TestGateInviteJoin_DiskClassification drives gateInviteJoin against real
// on-disk artifact combinations.
func TestGateInviteJoin_DiskClassification(t *testing.T) {
	t.Run("empty dir + bundle -> fresh", func(t *testing.T) {
		dir := t.TempDir()
		if got := gateInviteJoin(dir, true); got != inviteFreshJoin {
			t.Fatalf("got %d want FreshJoin", got)
		}
	})

	t.Run("empty dir + no bundle -> normal", func(t *testing.T) {
		dir := t.TempDir()
		if got := gateInviteJoin(dir, false); got != inviteNormalBoot {
			t.Fatalf("got %d want NormalBoot", got)
		}
	})

	t.Run("node key present, sentinel present -> resume", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteResume {
			t.Fatalf("got %d want Resume", got)
		}
	})

	t.Run("all artifacts durable + no sentinel -> normal", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.encryptionKey, []byte("k"))
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, p.kekGen0, []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		// no node.key.unsealed, no sentinel
		if got := gateInviteJoin(dir, true); got != inviteNormalBoot {
			t.Fatalf("got %d want NormalBoot", got)
		}
	})

	t.Run("unsealed key present means incomplete -> resume", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.encryptionKey, []byte("k"))
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, p.kekGen0, []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.nodeKeyUnsealed, []byte("plain")) // shred didn't complete
		if got := gateInviteJoin(dir, true); got != inviteResume {
			t.Fatalf("got %d want Resume (unsealed key still present)", got)
		}
	})
}

// TestMaybeInviteJoin_NodeIDResolution: a FreshJoin gate that can't actually
// dial still resolves + persists the node id and writes it back to opts.
func TestMaybeInviteJoin_NodeIDResolution(t *testing.T) {
	dir := t.TempDir()
	// Pre-seed node-id so GenerateNodeID is deterministic + idempotent.
	idFile := filepath.Join(dir, "node-id")
	mustWrite(t, idFile, []byte("fixed-node-id\n"))

	opts := &ServeOptions{DataDir: dir, RaftAddr: "10.0.0.1:7000"}
	// No bundle env -> NormalBoot, opts untouched, nil state.
	st, err := maybeInviteJoin(t.Context(), opts)
	if err != nil {
		t.Fatalf("normal boot err: %v", err)
	}
	if st != nil {
		t.Fatalf("expected nil state on normal boot, got %+v", st)
	}
	if opts.NodeID != "" {
		t.Fatalf("normal boot should not set NodeID, got %q", opts.NodeID)
	}
}

// TestMaybeInviteJoin_RejectsEphemeralRaftAddr: invite-join with a :0 raft addr
// is refused before any dial (requires a stable advertised addr).
func TestMaybeInviteJoin_RejectsEphemeralRaftAddr(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(inviteBundleEnv, mintTestBundleToken(t))

	opts := &ServeOptions{DataDir: dir, RaftAddr: "127.0.0.1:0"}
	_, err := maybeInviteJoin(t.Context(), opts)
	if err == nil {
		t.Fatal("expected error for ephemeral :0 raft addr")
	}
}

// TestMaybeInviteJoin_ResumePopulatesClusterKey: on Resume, opts.ClusterKey is
// read from keys.d/current.key (the gate runs BEFORE bootQUICTransport, so an
// empty key would trip the --cluster-key gate).
func TestMaybeInviteJoin_ResumePopulatesClusterKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(inviteBundleEnv, mintTestBundleToken(t))
	mustWrite(t, filepath.Join(dir, "node-id"), []byte("fixed-node-id\n"))

	// Make the gate classify Resume: persisted node key + pending sentinel.
	p := inviteJoinPathsFor(dir)
	mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
	mustWrite(t, p.pendingSentinel, []byte("pending"))

	// Stage the transport PSK on disk (Phase-1 mirrored it on the prior boot).
	const psk = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	if err := transport.NewKeystore(dir).WriteCurrent(psk); err != nil {
		t.Fatal(err)
	}

	opts := &ServeOptions{DataDir: dir, RaftAddr: "10.0.0.1:7000"}
	st, err := maybeInviteJoin(t.Context(), opts)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if st == nil {
		t.Fatal("resume should return non-nil state")
	}
	if opts.ClusterKey != psk {
		t.Fatalf("resume should populate opts.ClusterKey from current.key, got %q", opts.ClusterKey)
	}
}

// TestStageInviteSecrets writes every secret where the normal boot reads them.
func TestStageInviteSecrets(t *testing.T) {
	dir := t.TempDir()
	encKey := []byte("encryption-key-bytes")
	clusterID := []byte("0123456789abcdef") // 16 bytes
	gens := []cluster.KEKGen{
		{Gen: 0, Key: []byte("kek-gen-0")},
		{Gen: 1, Key: []byte("kek-gen-1")},
	}
	if err := stageInviteSecrets(dir, encKey, gens, clusterID); err != nil {
		t.Fatalf("stageInviteSecrets: %v", err)
	}

	if got := mustRead(t, filepath.Join(dir, "encryption.key")); string(got) != string(encKey) {
		t.Fatalf("encryption.key = %q", got)
	}
	if got := mustRead(t, filepath.Join(dir, nodeconfig.ClusterIDFile)); string(got) != string(clusterID) {
		t.Fatalf("cluster.id = %q", got)
	}
	keysDir := nodeconfig.New(dir).KEKDir()
	if got := mustRead(t, filepath.Join(keysDir, "0.key")); string(got) != "kek-gen-0" {
		t.Fatalf("keys/0.key = %q", got)
	}
	if got := mustRead(t, filepath.Join(keysDir, "1.key")); string(got) != "kek-gen-1" {
		t.Fatalf("keys/1.key = %q", got)
	}
}

// TestInviteSealBindContext matches W7 MetaJoinReceiver.sealBindContext layout:
// clusterID‖inviteID‖nodeID‖leaderID.
func TestInviteSealBindContext(t *testing.T) {
	got := inviteSealBindContext([]byte("CID"), "inv", "node", "leader")
	want := "CIDinvnodeleader"
	if string(got) != want {
		t.Fatalf("bindCtx = %q, want %q", got, want)
	}
}

// TestInvitePendingSentinelRoundTrip verifies binary sentinel encode/decode.
func TestInvitePendingSentinelRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := &inviteJoinState{
		seedAddr: "seed:7000",
		seedSPKI: [32]byte{1, 2, 3},
		inviteID: "invite-123",
		nodeID:   "node-abc",
		raftAddr: "10.0.0.1:7000",
		leaderID: "leader-xyz",
		nodeSPKI: [32]byte{9, 9, 9},
	}
	path := filepath.Join(dir, invitePendingFile)
	if err := writeInvitePendingSentinel(dir, path, st); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	rec, ok := readInvitePendingSentinel(dir)
	if !ok {
		t.Fatal("read sentinel: not found")
	}
	if rec.inviteID != st.inviteID || rec.leaderID != st.leaderID ||
		rec.seedAddr != st.seedAddr || rec.nodeID != st.nodeID ||
		rec.raftAddr != st.raftAddr || rec.seedSPKI != st.seedSPKI ||
		rec.nodeSPKI != st.nodeSPKI {
		t.Fatalf("sentinel round-trip mismatch: %+v vs %+v", rec, st)
	}
}

func mustWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// mintTestBundleToken builds a minimal valid InviteBundle token for gate tests.
func mintTestBundleToken(t *testing.T) string {
	t.Helper()
	priv, _, id, err := cluster.MintInviteKeypair()
	if err != nil {
		t.Fatal(err)
	}
	return cluster.EncodeInviteBundle(cluster.InviteBundle{
		InvitePriv:   priv,
		InviteID:     id,
		ClusterIDHex: "0123456789abcdef0123456789abcdef",
		SeedAddr:     "seed:7000",
	})
}
