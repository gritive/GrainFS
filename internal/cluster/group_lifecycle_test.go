package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInstantiateLocalGroup_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := GroupLifecycleConfig{
		NodeID:  "self",
		DataDir: dir,
	}
	entry := ShardGroupEntry{ID: "group-x", PeerIDs: []string{"self"}}
	gb, err := instantiateLocalGroup(cfg, entry)
	require.NoError(t, err)
	require.NotNil(t, gb)
	require.Equal(t, "group-x", gb.ID())

	// Files exist
	require.DirExists(t, filepath.Join(dir, "groups", "group-x", "badger"))
	require.DirExists(t, filepath.Join(dir, "groups", "group-x", "raft"))
	require.DirExists(t, filepath.Join(dir, "groups", "group-x", "blobs"))

	require.NoError(t, shutdownLocalGroup(context.Background(), gb, 5*time.Second))
}

func TestInstantiateLocalGroup_Idempotent_Recovery(t *testing.T) {
	dir := t.TempDir()
	cfg := GroupLifecycleConfig{NodeID: "self", DataDir: dir}
	entry := ShardGroupEntry{ID: "group-y", PeerIDs: []string{"self"}}

	gb1, err := instantiateLocalGroup(cfg, entry)
	require.NoError(t, err)
	require.NoError(t, shutdownLocalGroup(context.Background(), gb1, 5*time.Second))

	// Re-instantiate using the same dataDir (recovery path). Must succeed.
	gb2, err := instantiateLocalGroup(cfg, entry)
	require.NoError(t, err)
	require.Equal(t, "group-y", gb2.ID())
	require.NoError(t, shutdownLocalGroup(context.Background(), gb2, 5*time.Second))
}

func TestInstantiateLocalGroup_BadgerOpenFails_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	// Block BadgerDB by creating a non-directory at the expected path.
	groupsDir := filepath.Join(dir, "groups")
	require.NoError(t, os.MkdirAll(groupsDir, 0o755))
	groupDir := filepath.Join(groupsDir, "group-z")
	require.NoError(t, os.MkdirAll(groupDir, 0o755))
	// Place a regular file where the badger directory should go.
	require.NoError(t, os.WriteFile(filepath.Join(groupDir, "badger"), []byte("blocker"), 0o644))

	cfg := GroupLifecycleConfig{NodeID: "self", DataDir: dir}
	entry := ShardGroupEntry{ID: "group-z", PeerIDs: []string{"self"}}
	_, err := instantiateLocalGroup(cfg, entry)
	require.Error(t, err)
}

func TestInstantiateLocalGroup_EmptyGroupID(t *testing.T) {
	dir := t.TempDir()
	cfg := GroupLifecycleConfig{NodeID: "self", DataDir: dir}
	_, err := instantiateLocalGroup(cfg, ShardGroupEntry{ID: ""})
	require.Error(t, err)
}

func TestInstantiateLocalGroup_EmptyNodeID(t *testing.T) {
	dir := t.TempDir()
	cfg := GroupLifecycleConfig{DataDir: dir}
	_, err := instantiateLocalGroup(cfg, ShardGroupEntry{ID: "g", PeerIDs: []string{"a"}})
	require.Error(t, err)
}

// slowGroupCloser wraps a real GroupBackend and adds a Close delay — used to
// verify shutdownLocalGroup's timeout path.
type slowGroupCloser struct {
	gb    *GroupBackend
	delay time.Duration
}

func (s *slowGroupCloser) ID() string { return s.gb.ID() }
func (s *slowGroupCloser) Close() error {
	time.Sleep(s.delay)
	return s.gb.Close()
}

func TestShutdownLocalGroup_Timeout_Ungraceful(t *testing.T) {
	dir := t.TempDir()
	cfg := GroupLifecycleConfig{NodeID: "self", DataDir: dir}
	gb, err := instantiateLocalGroup(cfg, ShardGroupEntry{ID: "group-slow", PeerIDs: []string{"self"}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gb.Close() })

	slow := &slowGroupCloser{gb: gb, delay: 200 * time.Millisecond}

	start := time.Now()
	err = shutdownLocalGroup(context.Background(), slow, 50*time.Millisecond)
	elapsed := time.Since(start)

	require.True(t, errors.Is(err, errShutdownTimeout), "expected errShutdownTimeout, got %v", err)
	require.Less(t, elapsed, 100*time.Millisecond, "must return within ~timeout, got %v", elapsed)
}

func TestShutdownLocalGroup_Nil_NoError(t *testing.T) {
	require.NoError(t, shutdownLocalGroup(context.Background(), nil, 100*time.Millisecond))
}
