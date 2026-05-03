package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

type readinessSnapshotBackend struct {
	failures int
	calls    int
}

func (b *readinessSnapshotBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	b.calls++
	if b.calls <= b.failures {
		return nil, errors.New("snapshot route not ready")
	}
	return nil, nil
}

func (b *readinessSnapshotBackend) RestoreObjects([]storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	return 0, nil, nil
}

func TestWaitForSnapshotBackendReady_RetriesUntilRouteReady(t *testing.T) {
	backend := &readinessSnapshotBackend{failures: 2}

	err := waitForSnapshotBackendReady(context.Background(), backend, time.Second)
	require.NoError(t, err)
	require.GreaterOrEqual(t, backend.calls, 3)
}

func TestWaitForSnapshotBackendReady_TimesOut(t *testing.T) {
	backend := &readinessSnapshotBackend{failures: 1000}

	err := waitForSnapshotBackendReady(context.Background(), backend, 10*time.Millisecond)
	require.Error(t, err)
}

func TestShardGroupPlaceholderAllowsRemoteRouting(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	entry := cluster.ShardGroupEntry{
		ID:      "group-remote",
		PeerIDs: []string{"node-a", "node-b", "node-c"},
	}

	ensureShardGroupPlaceholder(mgr, entry)

	got := mgr.Get(entry.ID)
	require.NotNil(t, got)
	require.Equal(t, entry.PeerIDs, got.PeerIDs())
	require.Nil(t, got.Backend())
}

func TestShardGroupPlaceholderDoesNotReplaceExistingGroup(t *testing.T) {
	mgr := cluster.NewDataGroupManager()
	existing := cluster.NewDataGroup("group-remote", []string{"node-a"})
	mgr.Add(existing)

	ensureShardGroupPlaceholder(mgr, cluster.ShardGroupEntry{
		ID:      "group-remote",
		PeerIDs: []string{"node-b", "node-c"},
	})

	require.Same(t, existing, mgr.Get("group-remote"))
}

func TestSeedShardGroupVotersUsesConsistentPeerIDs(t *testing.T) {
	peers := []string{"127.0.0.1:20008", "127.0.0.1:20011", "127.0.0.1:20014"}

	group0 := seedShardGroupVoters("127.0.0.1:20005", peers, "group-0", 3)
	require.Equal(t, []string{
		"127.0.0.1:20005",
		"127.0.0.1:20008",
		"127.0.0.1:20011",
		"127.0.0.1:20014",
	}, group0)

	groupN := seedShardGroupVoters("127.0.0.1:20005", peers, "group-7", 3)
	require.Len(t, groupN, 3)
	for _, peer := range groupN {
		require.Contains(t, append([]string{"127.0.0.1:20005"}, peers...), peer)
		require.NotContains(t, peer, "perf-load-N16")
	}
}
