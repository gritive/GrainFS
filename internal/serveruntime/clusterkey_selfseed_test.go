package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

func TestDirEmptyStrict(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "meta")

	empty, err := dirEmptyStrict(sub) // absent
	require.NoError(t, err)
	require.True(t, empty)

	require.NoError(t, os.MkdirAll(sub, 0o700)) // present, empty
	empty, err = dirEmptyStrict(sub)
	require.NoError(t, err)
	require.True(t, empty)

	require.NoError(t, os.WriteFile(filepath.Join(sub, "x"), []byte("y"), 0o600)) // non-empty
	empty, err = dirEmptyStrict(sub)
	require.NoError(t, err)
	require.False(t, empty)
}

func TestFileAbsentStrict(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cluster.id")

	absent, err := fileAbsentStrict(p)
	require.NoError(t, err)
	require.True(t, absent)

	require.NoError(t, os.WriteFile(p, []byte("z"), 0o600))
	absent, err = fileAbsentStrict(p)
	require.NoError(t, err)
	require.False(t, absent)
}

// On a non-ErrNotExist error (traversing through a regular file → ENOTDIR) the
// strict probes MUST surface the error, never report "absent"/"empty".
func TestStrictProbesFailClosedOnError(t *testing.T) {
	dir := t.TempDir()
	notADir := filepath.Join(dir, "afile")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o600))
	child := filepath.Join(notADir, "child") // path traverses a file

	_, err := fileAbsentStrict(child)
	require.Error(t, err)

	_, err = dirEmptyStrict(child)
	require.Error(t, err)
}

// kekDirEmptyStrict: missing → empty; has <N>.key → not empty; dir-is-a-file → error.
func TestKekDirEmptyStrict(t *testing.T) {
	dir := t.TempDir()

	empty, err := kekDirEmptyStrict(filepath.Join(dir, "keys")) // absent
	require.NoError(t, err)
	require.True(t, empty)

	keys := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keys, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(keys, "3.key"), []byte("k"), 0o600))
	empty, err = kekDirEmptyStrict(keys)
	require.NoError(t, err)
	require.False(t, empty)

	fileAsDir := filepath.Join(dir, "keysfile")
	require.NoError(t, os.WriteFile(fileAsDir, []byte("x"), 0o600))
	_, err = kekDirEmptyStrict(fileAsDir)
	require.Error(t, err)
}

func writeProbeFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
}

func freshGenesisState(t *testing.T) *bootState {
	t.Helper()
	d := t.TempDir()
	return &bootState{
		cfg:     Config{DataDir: d, ClusterKey: ""},
		metaDir: filepath.Join(d, "meta"),
		raftDir: filepath.Join(d, "raft"),
	}
}

func TestSelfSeedDecision(t *testing.T) {
	t.Run("fresh genesis seeds", func(t *testing.T) {
		seed, err := selfSeedDecision(freshGenesisState(t))
		require.NoError(t, err)
		require.True(t, seed)
	})
	t.Run("flag present blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		st.cfg.ClusterKey = "deadbeef"
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("joinMode blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		st.joinMode = true
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("inviteJoinMode blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		st.inviteJoinMode = true
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("priorState (raft non-empty) blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		writeProbeFile(t, filepath.Join(st.raftDir, "0001.log"))
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("staged cluster.id blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		writeProbeFile(t, filepath.Join(st.cfg.DataDir, "cluster.id"))
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("staged KEK <N>.key blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		writeProbeFile(t, filepath.Join(st.cfg.DataDir, "keys", "3.key"))
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("invite node.key.unsealed blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		writeProbeFile(t, filepath.Join(st.cfg.DataDir, "node.key.unsealed"))
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("invite-join-pending sentinel blocks", func(t *testing.T) {
		st := freshGenesisState(t)
		writeProbeFile(t, filepath.Join(st.cfg.DataDir, ".invite-join-pending"))
		seed, err := selfSeedDecision(st)
		require.NoError(t, err)
		require.False(t, seed)
	})
	t.Run("fail-closed: KEK dir unreadable errors, no seed", func(t *testing.T) {
		st := freshGenesisState(t)
		// make the keys path a regular file → KeysDirIsEmpty errors (ENOTDIR).
		writeProbeFile(t, filepath.Join(st.cfg.DataDir, "keys"))
		seed, err := selfSeedDecision(st)
		require.Error(t, err)
		require.False(t, seed)
	})
}

// bootValidateConfig on a fresh dir with an empty flag must self-seed: no error,
// ClusterKey populated, current.key persisted, and a restart loads it (no reseed).
func TestBootValidateConfigSelfSeeds(t *testing.T) {
	d := t.TempDir()
	st := &bootState{cfg: Config{DataDir: d, NodeID: "n1"}}
	require.NoError(t, bootValidateConfig(st))
	require.NotEmpty(t, st.cfg.ClusterKey, "fresh genesis should self-seed")

	got, err := transport.NewKeystore(d).ReadCurrent()
	require.NoError(t, err)
	require.Equal(t, st.cfg.ClusterKey, got)

	// restart: same dir, fresh state, empty flag → loads disk, no error, no reseed.
	st2 := &bootState{cfg: Config{DataDir: d, NodeID: "n1"}}
	require.NoError(t, bootValidateConfig(st2))
}

// bootValidateConfig with priorState (raft state present) and no key must NOT
// seed — the existing --cluster-key error fires.
func TestBootValidateConfigNoSeedOnPriorState(t *testing.T) {
	d := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(d, "raft"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(d, "raft", "x"), []byte("y"), 0o600))
	st := &bootState{cfg: Config{DataDir: d, NodeID: "n1"}}
	err := bootValidateConfig(st)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster-key")
}
