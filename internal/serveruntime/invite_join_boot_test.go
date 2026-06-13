package serveruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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
		{"bundle, no key, incomplete -> fresh", true, false, false, false, inviteFreshJoin},
		// Previously-bricking case: a Phase-1 crash AFTER persisting the node key +
		// sentinel but BEFORE staging leaves {persistedKey, !complete, !acked}. The
		// gate MUST re-run Phase-1 (FreshJoin), NOT route to Phase-2 (which would
		// hard-fail on the missing keys.d/current.key and brick the node forever).
		{"bundle, key, incomplete, not acked -> fresh (was bricking)", true, true, false, false, inviteFreshJoin},
		{"bundle, complete but not acked -> resume", true, true, true, false, inviteResume},
		// P2: a complete-but-unacked sentinel must resume Phase-2 even WITHOUT the
		// bundle env (one-shot env; a restart need not re-set it). Otherwise the
		// node boots on staged secrets but never sends the membership ACK.
		{"no bundle, complete but not acked -> resume", false, true, true, false, inviteResume},
		{"bundle, complete and acked -> normal", true, true, true, true, inviteNormalBoot},
		// Incomplete dominates acked: re-run Phase-1 rather than resume.
		{"bundle, key, acked but incomplete -> fresh", true, true, false, true, inviteFreshJoin},
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

	t.Run("node key + sentinel only (no staged secrets) -> fresh", func(t *testing.T) {
		// Phase-1 crashed after persisting the key + sentinel but before staging:
		// artifacts incomplete, so re-run Phase-1 (the previously-bricking path).
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteFreshJoin {
			t.Fatalf("got %d want FreshJoin (incomplete staging)", got)
		}
	})

	t.Run("all zero-CA artifacts staged + sentinel present -> resume", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteResume {
			t.Fatalf("got %d want Resume", got)
		}
	})

	t.Run("all zero-CA artifacts staged without encryption.key + sentinel present -> resume", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		require.Equal(t, inviteResume, gateInviteJoin(dir, true))
	})

	t.Run("all zero-CA artifacts staged without encryption.key + sentinel present + NO bundle -> resume", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		require.Equal(t, inviteResume, gateInviteJoin(dir, false))
	})

	t.Run("all zero-CA artifacts staged + sentinel present + NO bundle -> resume (P2)", func(t *testing.T) {
		// The bundle env is one-shot: a restart that does not re-set it must still
		// resume the Phase-2 ACK from the complete-but-unacked sentinel, NOT boot as
		// a non-member on the staged secrets.
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, false); got != inviteResume {
			t.Fatalf("got %d want Resume (no bundle env, complete+unacked)", got)
		}
	})

	t.Run("staged KEK is a non-zero gen (rotated+pruned) -> resume", func(t *testing.T) {
		// A cluster that rotated+pruned gen-0 ships only a higher gen; the staged
		// KEK file is e.g. 3.key, not 0.key. artifactsComplete must still hold.
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "3.key"), []byte("kek3"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteResume {
			t.Fatalf("got %d want Resume (gen-3 staged, no gen-0)", got)
		}
	})

	t.Run("all zero-CA secrets staged but PSK (current.key) missing -> fresh (P2)", func(t *testing.T) {
		// Crash window: node.key.enc written + node.key.unsealed shredded, but the
		// transport PSK never reached keys.d/current.key. Without current.key in the
		// completeness gate this would classify as Resume, which then hard-fails
		// (inviteJoinResumeFromSentinel needs the PSK) and can no longer rerun
		// Phase-1. The gate must keep it FreshJoin instead.
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		// no current.key, no node.key.unsealed
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteFreshJoin {
			t.Fatalf("got %d want FreshJoin (PSK missing = incomplete)", got)
		}
	})

	t.Run("all artifacts durable + no sentinel -> normal", func(t *testing.T) {
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		// no node.key.unsealed, no sentinel
		if got := gateInviteJoin(dir, true); got != inviteNormalBoot {
			t.Fatalf("got %d want NormalBoot", got)
		}
	})

	t.Run("all artifacts + leftover unsealed key + not acked -> resume (P1)", func(t *testing.T) {
		// Crash window: Phase-1 wrote node.key.enc + current.key (durable) but the
		// shred of node.key.unsealed never completed. ALL durable completion
		// artifacts exist, so artifactsComplete must hold DESPITE the leftover
		// unsealed key — otherwise the node re-runs Phase-1 or (without the one-shot
		// bundle env) boots as a non-member and splits from the cluster. The leftover
		// key is shredded idempotently on the resume path. Previously this state was
		// mis-classified FreshJoin because the gate required node.key.unsealed absent.
		dir := t.TempDir()
		p := inviteJoinPathsFor(dir)
		mustWrite(t, p.clusterID, []byte("cid"))
		mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
		mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
		mustWrite(t, p.currentKey, []byte("psk"))
		mustWrite(t, p.nodeKeyUnsealed, []byte("plain")) // shred didn't complete
		mustWrite(t, p.pendingSentinel, []byte("pending"))
		if got := gateInviteJoin(dir, true); got != inviteResume {
			t.Fatalf("got %d want Resume (all durable artifacts present; leftover unsealed key is irrelevant)", got)
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
	st, err := maybeInviteJoin(t.Context(), opts, dir)
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
	_, err := maybeInviteJoin(t.Context(), opts, dir)
	if err == nil {
		t.Fatal("expected error for ephemeral :0 raft addr")
	}
}

// stageResumeArtifacts writes all Phase-1 artifacts + a REAL binary sentinel +
// the mirrored transport PSK so the gate classifies Resume and the resume path
// can reconstruct its state from the sentinel. Returns the staged PSK.
func stageResumeArtifacts(t *testing.T, dir string) string {
	t.Helper()
	p := inviteJoinPathsFor(dir)
	mustWrite(t, p.clusterID, []byte("cid"))
	mustWrite(t, filepath.Join(p.keysDir, "0.key"), []byte("kek0"))
	mustWrite(t, p.nodeKeyEnc, []byte("sealed"))
	// A REAL binary sentinel (the resume path now decodes it, not just stat()s it).
	if err := writeInvitePendingSentinel(dir, p.pendingSentinel, &inviteJoinState{
		seedAddr:      "seed:7000",
		seedSPKI:      [32]byte{1, 2, 3},
		inviteID:      "invite-123",
		nodeID:        "fixed-node-id",
		raftAddr:      "10.0.0.1:7000",
		leaderID:      "leader-xyz",
		nodeSPKI:      [32]byte{9, 9, 9},
		nodeKeyKEKGen: 0,
	}); err != nil {
		t.Fatal(err)
	}
	const psk = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	if err := transport.NewKeystore(dir).WriteCurrent(psk); err != nil {
		t.Fatal(err)
	}
	return psk
}

// TestMaybeInviteJoin_ResumePopulatesClusterKey: on Resume, opts.ClusterKey is
// read from keys.d/current.key (the gate runs BEFORE bootClusterTransport, so an
// empty key would trip the cluster-transport-key-missing gate).
func TestMaybeInviteJoin_ResumePopulatesClusterKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(inviteBundleEnv, mintTestBundleToken(t))
	mustWrite(t, filepath.Join(dir, "node-id"), []byte("fixed-node-id\n"))
	psk := stageResumeArtifacts(t, dir)

	opts := &ServeOptions{DataDir: dir, RaftAddr: "10.0.0.1:7000"}
	st, err := maybeInviteJoin(t.Context(), opts, dir)
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

// TestMaybeInviteJoin_ResumeWithoutBundleEnv is the P2 load-bearing assertion: a
// complete-but-unacked sentinel must resume Phase-2 even though the one-shot
// GRAINFS_INVITE_BUNDLE env is NOT set on this restart. State is reconstructed
// from the sentinel; opts.ClusterKey is loaded from keys.d/current.key.
func TestMaybeInviteJoin_ResumeWithoutBundleEnv(t *testing.T) {
	dir := t.TempDir()
	// NOTE: deliberately NO t.Setenv(inviteBundleEnv, ...).
	mustWrite(t, filepath.Join(dir, "node-id"), []byte("fixed-node-id\n"))
	psk := stageResumeArtifacts(t, dir)

	// No RaftAddr: resume must NOT depend on it (the sentinel carries raftAddr).
	opts := &ServeOptions{DataDir: dir}
	st, err := maybeInviteJoin(t.Context(), opts, dir)
	if err != nil {
		t.Fatalf("resume without bundle env: %v", err)
	}
	if st == nil {
		t.Fatal("resume without bundle env should return non-nil state")
	}
	if opts.ClusterKey != psk {
		t.Fatalf("resume should populate opts.ClusterKey from current.key, got %q", opts.ClusterKey)
	}
	if st.seedAddr != "seed:7000" || st.inviteID != "invite-123" ||
		st.nodeID != "fixed-node-id" || st.raftAddr != "10.0.0.1:7000" {
		t.Fatalf("resume state not reconstructed from sentinel: %+v", st)
	}
}

// TestMaybeInviteJoin_MultiDataDirUsesPrimary is the P2 multi-disk regression:
// when --data is a comma-separated multi-drive list, maybeInviteJoin must stage
// + read under the PRIMARY dir (DataDirs[0]) — the same dir cfg.DataDir resolves
// to and Phase-2 reads — NOT the raw "dirA,dirB" opts.DataDir string. We stage
// resume artifacts under dirA, then drive the resume path with opts.DataDir set
// to the literal comma string and the primary dataDir param = dirA, and assert
// the state resolves from dirA with nothing created under the comma string.
func TestMaybeInviteJoin_MultiDataDirUsesPrimary(t *testing.T) {
	base := t.TempDir()
	dirA := filepath.Join(base, "dirA")
	dirB := filepath.Join(base, "dirB")
	if err := os.MkdirAll(dirA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dirB, 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dirA, "node-id"), []byte("fixed-node-id\n"))
	psk := stageResumeArtifacts(t, dirA) // all Phase-1 artifacts land under dirA

	// Raw --data flag carries both drives; DataDirs[0] is the primary. No bundle
	// env: the complete-but-unacked sentinel under dirA resumes Phase-2.
	rawData := dirA + "," + dirB
	opts := &ServeOptions{DataDir: rawData, DataDirs: []string{dirA, dirB}}
	primary := opts.DataDirs[0]

	st, err := maybeInviteJoin(t.Context(), opts, primary)
	if err != nil {
		t.Fatalf("multi-disk resume: %v", err)
	}
	if st == nil {
		t.Fatal("multi-disk resume should return non-nil state")
	}
	// State reconstructed from the sentinel under dirA.
	if st.nodeID != "fixed-node-id" || st.raftAddr != "10.0.0.1:7000" {
		t.Fatalf("resume state not read from primary dir sentinel: %+v", st)
	}
	// PSK read from dirA/keys.d/current.key (not the comma string).
	if opts.ClusterKey != psk {
		t.Fatalf("opts.ClusterKey = %q, want PSK from primary dir", opts.ClusterKey)
	}
	// The literal "dirA,dirB" directory must NOT exist (no staging leaked there).
	if _, err := os.Stat(rawData); !os.IsNotExist(err) {
		t.Fatalf("literal comma-string dir %q should not exist (got err=%v)", rawData, err)
	}
}

// TestStageInviteSecrets writes every KEK secret where normal boot reads it
// without creating a legacy static encryption key.
func TestStageInviteSecrets(t *testing.T) {
	dir := t.TempDir()
	clusterID := []byte("0123456789abcdef") // 16 bytes
	gens := []cluster.KEKGen{
		{Gen: 0, Key: []byte("kek-gen-0")},
		{Gen: 1, Key: []byte("kek-gen-1")},
	}
	if err := stageInviteSecrets(dir, gens, clusterID); err != nil {
		t.Fatalf("stageInviteSecrets: %v", err)
	}

	require.NoFileExists(t, filepath.Join(dir, "encryption.key"))
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

func TestStageInviteSecretsSkipsEncryptionKeyWhenBootstrapOmitsIt(t *testing.T) {
	dir := t.TempDir()
	clusterID := []byte("0123456789abcdef") // 16 bytes
	gens := []cluster.KEKGen{
		{Gen: 0, Key: []byte("kek-gen-0")},
		{Gen: 1, Key: []byte("kek-gen-1")},
	}

	require.NoError(t, stageInviteSecrets(dir, gens, clusterID))
	require.NoFileExists(t, filepath.Join(dir, "encryption.key"))
	require.Equal(t, clusterID, mustRead(t, filepath.Join(dir, nodeconfig.ClusterIDFile)))

	keysDir := nodeconfig.New(dir).KEKDir()
	require.Equal(t, []byte("kek-gen-0"), mustRead(t, filepath.Join(keysDir, "0.key")))
	require.Equal(t, []byte("kek-gen-1"), mustRead(t, filepath.Join(keysDir, "1.key")))
}

// TestInviteSealBindContext matches W7 MetaJoinReceiver.sealBindContext layout:
// domain tag ‖ len-prefixed(clusterID) ‖ len-prefixed(inviteID) ‖
// len-prefixed(nodeID) ‖ len-prefixed(leaderID). The length prefixing closes a
// boundary-ambiguity class; this asserts the exact canonical bytes the leader
// must reproduce.
func TestInviteSealBindContext(t *testing.T) {
	lp := func(b []byte) []byte {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(b)))
		return append(l[:], b...)
	}
	var want []byte
	want = append(want, "grainfs-invite-seal-v1"...)
	want = append(want, lp([]byte("CID"))...)
	want = append(want, lp([]byte("inv"))...)
	want = append(want, lp([]byte("node"))...)
	want = append(want, lp([]byte("leader"))...)

	got := inviteSealBindContext([]byte("CID"), "inv", "node", "leader")
	if !bytes.Equal(got, want) {
		t.Fatalf("bindCtx = %x, want %x", got, want)
	}

	// Boundary-ambiguity guard: (inviteID="ab", nodeID="c") and
	// (inviteID="a", nodeID="bc") MUST yield distinct context bytes.
	a := inviteSealBindContext([]byte("CID"), "ab", "c", "leader")
	b := inviteSealBindContext([]byte("CID"), "a", "bc", "leader")
	if bytes.Equal(a, b) {
		t.Fatal("bindCtx collided across distinct (inviteID,nodeID) splits")
	}
}

// TestInvitePendingSentinelRoundTrip verifies binary sentinel encode/decode.
func TestInvitePendingSentinelRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := &inviteJoinState{
		seedAddr:      "seed:7000",
		seedSPKI:      [32]byte{1, 2, 3},
		inviteID:      "invite-123",
		nodeID:        "node-abc",
		raftAddr:      "10.0.0.1:7000",
		leaderID:      "leader-xyz",
		nodeSPKI:      [32]byte{9, 9, 9},
		nodeKeyKEKGen: 3,
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
		rec.nodeSPKI != st.nodeSPKI || rec.nodeKeyKEKGen != st.nodeKeyKEKGen {
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

func TestNodeKeyGenSidecar_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	if err := writeNodeKeyGen(dir, 3); err != nil {
		t.Fatalf("writeNodeKeyGen: %v", err)
	}
	got, ok := readNodeKeyGen(dir)
	if !ok {
		t.Fatal("readNodeKeyGen: not found")
	}
	if got != 3 {
		t.Fatalf("gen=%d want 3", got)
	}
	info, err := os.Stat(filepath.Join(dir, "keys.d", nodeKeyGenFile))
	if err != nil {
		t.Fatalf("stat node.key.gen: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o want 0600", info.Mode().Perm())
	}
}

func TestNodeKeyGenSidecar_RejectsMissingMalformedAndNonCanonical(t *testing.T) {
	dir := t.TempDir()
	if _, ok := readNodeKeyGen(dir); ok {
		t.Fatal("missing sidecar decoded successfully")
	}

	cases := map[string]string{
		"empty":      "",
		"alpha":      "abc",
		"leading0":   "03",
		"plus":       "+3",
		"newline":    "3\n",
		"overflow32": "4294967296",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			mustWrite(t, filepath.Join(dir, "keys.d", nodeKeyGenFile), []byte(data))
			if _, ok := readNodeKeyGen(dir); ok {
				t.Fatalf("malformed sidecar %q decoded successfully", data)
			}
		})
	}
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

func TestLoadAndMigrateInviteNodeKey_ReSealsOlderKEKGenToActive(t *testing.T) {
	dir := t.TempDir()
	encKey := bytes.Repeat([]byte{0xCD}, 32)
	store := newNIKEKStore(t, 0, 3, 4)
	sealKEK, err := store.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}

	cert, wantSPKI, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	// Phase-1 seal: under the KEK gen only.
	if err := transport.SealNodeKey(dir, sealKEK, cert); err != nil {
		t.Fatalf("SealNodeKey under KEK gen: %v", err)
	}
	if err := writeNodeKeyGen(dir, 3); err != nil {
		t.Fatalf("writeNodeKeyGen: %v", err)
	}
	// Pre-migration: must NOT decrypt under encKey yet.
	if _, _, err := transport.LoadNodeKey(dir, encKey); err == nil {
		t.Fatal("node.key.enc unexpectedly decrypts under encKey before migration")
	}

	gotCert, gotSPKI, gotGen, err := loadAndMigrateInviteNodeKey(dir, store, 3, true)
	if err != nil {
		t.Fatalf("loadAndMigrateInviteNodeKey: %v", err)
	}
	require.Equal(t, store.ActiveVersion(), gotGen)
	if gotSPKI != wantSPKI {
		t.Fatalf("SPKI changed across re-seal: got %x want %x", gotSPKI, wantSPKI)
	}
	if gotCert.PrivateKey == nil {
		t.Fatal("returned cert has nil private key")
	}
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	if _, spki, err := transport.LoadNodeKey(dir, activeKEK); err != nil {
		t.Fatalf("node.key.enc does not decrypt under active KEK after migration: %v", err)
	} else if spki != wantSPKI {
		t.Fatalf("post-migration SPKI mismatch: got %x want %x", spki, wantSPKI)
	}
	if gen, ok := readNodeKeyGen(dir); !ok {
		t.Fatal("node.key.gen must remain after KEK migration")
	} else if gen != store.ActiveVersion() {
		t.Fatalf("node.key.gen=%d want active %d", gen, store.ActiveVersion())
	}

	// Idempotent resume: a second call (e.g. after a crash before the sentinel
	// clear) must succeed via node.key.gen even if fallback state is stale.
	if _, spki, gotGen, err := loadAndMigrateInviteNodeKey(dir, store, 3, true); err != nil {
		t.Fatalf("idempotent resume failed: %v", err)
	} else if spki != wantSPKI {
		t.Fatalf("resume SPKI mismatch: got %x want %x", spki, wantSPKI)
	} else {
		require.Equal(t, store.ActiveVersion(), gotGen)
	}
}

func TestLoadAndMigrateInviteNodeKey_RejectsLegacyStaticSealedNodeKey(t *testing.T) {
	dir := t.TempDir()
	encKey := bytes.Repeat([]byte{0xCD}, 32)
	store := newNIKEKStore(t, 0, 1, 2)

	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, encKey, cert); err != nil {
		t.Fatalf("SealNodeKey under encKey: %v", err)
	}
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)

	if err := writeNodeKeyGen(dir, 3); err != nil {
		t.Fatalf("writeNodeKeyGen stale sidecar: %v", err)
	}

	_, _, _, err = loadAndMigrateInviteNodeKey(dir, store, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get node key KEK gen 3")
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestLoadAndMigrateInviteNodeKey_RejectsMissingSidecarForKEKSealedKey(t *testing.T) {
	dir := t.TempDir()
	store := newNIKEKStore(t, 0, 1, 2)
	kek2, err := store.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}

	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, kek2, cert); err != nil {
		t.Fatalf("SealNodeKey under KEK gen 2: %v", err)
	}
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc: %v", err)
	}

	if _, _, _, err := loadAndMigrateInviteNodeKey(dir, store, 0, false); err == nil {
		t.Fatal("expected missing node.key.gen to fail")
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc was overwritten after missing sidecar failure")
	}
}

func TestStageInviteJoinTransportKey_PreDropPersistsDeliveredPSK(t *testing.T) {
	dir := t.TempDir()
	opts := &ServeOptions{}

	if err := stageInviteJoinTransportKey(dir, opts, []byte("delivered-psk"), false); err != nil {
		t.Fatalf("stageInviteJoinTransportKey: %v", err)
	}
	if opts.ClusterKey != "delivered-psk" {
		t.Fatalf("ClusterKey = %q, want delivered PSK", opts.ClusterKey)
	}
	disk, err := transport.NewKeystore(dir).ReadCurrent()
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if disk != "delivered-psk" {
		t.Fatalf("disk key = %q, want delivered PSK", disk)
	}
}

func TestStageInviteJoinTransportKey_PostDropUsesLocalPlaceholder(t *testing.T) {
	dir := t.TempDir()
	opts := &ServeOptions{}

	if err := stageInviteJoinTransportKey(dir, opts, nil, true); err != nil {
		t.Fatalf("stageInviteJoinTransportKey: %v", err)
	}
	if opts.ClusterKey == "" {
		t.Fatal("ClusterKey is empty, want local placeholder")
	}
	if opts.ClusterKey == "delivered-psk" {
		t.Fatal("ClusterKey unexpectedly reused delivered PSK")
	}
	disk, err := transport.NewKeystore(dir).ReadCurrent()
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if disk != opts.ClusterKey {
		t.Fatalf("disk key = %q, want ClusterKey %q", disk, opts.ClusterKey)
	}
}

func TestStageInviteJoinTransportKey_PostDropIgnoresDeliveredPSK(t *testing.T) {
	dir := t.TempDir()
	opts := &ServeOptions{}

	if err := stageInviteJoinTransportKey(dir, opts, []byte("delivered-psk"), true); err != nil {
		t.Fatalf("stageInviteJoinTransportKey: %v", err)
	}
	if opts.ClusterKey == "" {
		t.Fatal("ClusterKey is empty, want local placeholder")
	}
	if opts.ClusterKey == "delivered-psk" {
		t.Fatal("post-drop path persisted delivered PSK instead of local placeholder")
	}
	disk, err := transport.NewKeystore(dir).ReadCurrent()
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if disk != opts.ClusterKey {
		t.Fatalf("disk key = %q, want ClusterKey %q", disk, opts.ClusterKey)
	}
	if disk == "delivered-psk" {
		t.Fatal("disk key reused delivered PSK after cluster key drop")
	}
}

func TestInviteNodeKeySealKey_PreDropUsesHighestKEK(t *testing.T) {
	gens := []cluster.KEKGen{
		{Gen: 1, Key: bytes.Repeat([]byte{0x01}, 32)},
		{Gen: 3, Key: bytes.Repeat([]byte{0x03}, 32)},
	}

	gen, key, err := inviteNodeKeySealKey(gens)
	require.NoError(t, err)
	require.Equal(t, uint32(3), gen)
	require.Equal(t, gens[1].Key, key, "pre-drop seal key did not use highest KEK generation")
}

func TestInviteNodeKeySealKey_PostDropUsesHighestKEK(t *testing.T) {
	gens := []cluster.KEKGen{
		{Gen: 3, Key: bytes.Repeat([]byte{0x03}, 32)},
	}

	gen, key, err := inviteNodeKeySealKey(gens)
	require.NoError(t, err)
	require.Equal(t, uint32(3), gen)
	require.Equal(t, gens[0].Key, key, "post-drop seal key did not use highest KEK generation")
}

func TestInviteNodeKeySealKey_RejectsMissingKEKGenerations(t *testing.T) {
	_, _, err := inviteNodeKeySealKey(nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bootstrap secrets contain no KEK generations")
}

func TestInviteJoinDial_PassesBindToBuilder(t *testing.T) {
	srvCert, srvSPKI, err := transport.GenerateNodeIdentity("cid", "seed")
	if err != nil {
		t.Fatalf("seed identity: %v", err)
	}
	ln, err := transport.NewHTTPJoinListener("127.0.0.1:0", srvCert,
		func(ctx context.Context, peerSPKI [32]byte, bind []byte, req []byte) ([]byte, error) {
			return cluster.EncodeJoinReplyForTest(cluster.JoinReply{Accepted: true, Status: cluster.JoinStatusOK})
		})
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	defer ln.Close()

	cliCert, _, err := transport.GenerateNodeIdentity("cid", "joiner")
	if err != nil {
		t.Fatalf("joiner identity: %v", err)
	}
	var gotBind []byte
	_, err = inviteJoinDialWith(context.Background(), transport.DialJoinHTTP, ln.Addr(), srvSPKI, cliCert,
		func(bind []byte) (cluster.JoinRequest, error) {
			gotBind = append([]byte(nil), bind...)
			return cluster.JoinRequest{JoinPhase: 1, NodeID: "joiner", Address: "127.0.0.1:1"}, nil
		})
	if err != nil {
		t.Fatalf("inviteJoinDial: %v", err)
	}
	if len(gotBind) != 32 {
		t.Fatalf("builder bind len=%d want 32", len(gotBind))
	}
}
