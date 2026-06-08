package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestIndexGroupManager_InstantiateAndStart_OrderedShards drives the Task-5
// manager assembly path on a SOLO in-proc setup: InstantiateAndStart over
// entries [index-00, index-01, index-02] builds three local index groups, each
// in its own raft node, then Shards() exposes them ORDERED by group ID so
// Shards()[i] is the group for hash%N==i. A PUT through each shard's Writer
// round-trips through that group's FSM, and Close() shuts every group + store
// down without leaking.
func TestIndexGroupManager_InstantiateAndStart_OrderedShards(t *testing.T) {
	dir := t.TempDir()
	const n = 3
	entries := make([]IndexGroupEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = IndexGroupEntry{ID: indexGroupIDForTest(i, n), PeerIDs: []string{"n1"}}
	}

	mgr := NewIndexGroupManager()
	cfg := IndexGroupLifecycleConfig{
		NodeID:  "n1",
		DataDir: dir,
		// Deterministic K0 active KEK — the minimum each index-group MetaFSM needs
		// to seal/open its raft-snapshot envelope. The manager builds its own FSMs.
		KEKStore: newTestKEKStore(t, bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)),
	}

	// Solo groups: the node IS the leader so proposeOrForward never forwards; pass
	// a nil send (leader-local) — the manager leaves each hook nil.
	var send IndexGroupForwardSend = nil

	require.NoError(t, mgr.InstantiateAndStart(context.Background(), cfg, entries, send))
	t.Cleanup(mgr.Close)

	shards := mgr.Shards()
	require.Len(t, shards, n, "Shards() must expose one shard per entry")

	// Ordering: Shards()[i] must be the group for index-0i. We assert each shard's
	// Reader is the *indexGroup registered under index-0i.
	for i := 0; i < n; i++ {
		id := indexGroupIDForTest(i, n)
		g, ok := mgr.Lookup(id)
		require.True(t, ok, "group %s must be registered", id)
		require.Same(t, g, shards[i].Reader, "Shards()[%d] must be group %s", i, id)
		require.Same(t, g, shards[i].Writer, "Shards()[%d] writer must be group %s", i, id)
		require.Same(t, g, shards[i].Lister, "Shards()[%d] lister must be group %s", i, id)
	}

	// Each solo group must elect itself leader before a local propose succeeds.
	for i := 0; i < n; i++ {
		g := shards[i].Writer.(*indexGroup)
		require.Eventually(t, g.node.IsLeader, 5*time.Second, 20*time.Millisecond,
			"solo index group %d should elect itself leader", i)
	}

	// PUT through each shard's Writer round-trips through that shard's Reader.
	ctx := context.Background()
	for i := 0; i < n; i++ {
		entry := ObjectIndexEntry{
			Bucket:           "b",
			Key:              fmt.Sprintf("k%d", i),
			VersionID:        "v1",
			PlacementGroupID: indexGroupIDForTest(i, n),
			Size:             int64(i + 1),
			ModTime:          1,
		}
		require.NoError(t, shards[i].Writer.ProposeObjectIndex(ctx, entry, false), "put on shard %d", i)
		got, ok := shards[i].Reader.ObjectIndexLatest("b", fmt.Sprintf("k%d", i))
		require.True(t, ok, "read-your-write on shard %d", i)
		assert.Equal(t, "v1", got.VersionID)
		assert.Equal(t, int64(i+1), got.Size)
	}

	// Idempotency: re-running InstantiateAndStart over the same entries must be a
	// no-op (skip already-registered), not a second BadgerDB open on the same dir.
	require.NoError(t, mgr.InstantiateAndStart(context.Background(), cfg, entries, send),
		"InstantiateAndStart must be idempotent (restart/replay-scan + callback both call it)")
	require.Len(t, mgr.Shards(), n, "idempotent re-run must not add shards")
}

// indexGroupIDForTest mirrors serveruntime.indexGroupID's zero-pad scheme so the
// cluster-package test asserts the same lexicographic == numeric ordering the
// manager relies on. Kept local to the cluster package (no serveruntime import).
func indexGroupIDForTest(i, n int) string {
	width := len(fmt.Sprintf("%d", n-1))
	return fmt.Sprintf("index-%0*d", width, i)
}
