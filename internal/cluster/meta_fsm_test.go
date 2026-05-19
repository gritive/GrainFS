package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
)

func makeAddNodeCmd(t *testing.T, id, addr string, role uint8) []byte {
	t.Helper()
	data, err := encodeMetaAddNodeCmd(MetaNodeEntry{ID: id, Address: addr, Role: role})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeAddNode, data)
	require.NoError(t, err)
	return cmd
}

func makeRemoveNodeCmd(t *testing.T, id string) []byte {
	t.Helper()
	data, err := encodeMetaRemoveNodeCmd(id)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeRemoveNode, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_AddNode(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0))
	require.NoError(t, err)

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-1", nodes[0].ID)
	assert.Equal(t, "10.0.0.1:7001", nodes[0].Address)
	assert.Equal(t, uint8(0), nodes[0].Role)
}

func TestMetaFSM_Apply_RemoveNode(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "10.0.0.2:7001", 0)))

	require.NoError(t, f.applyCmd(makeRemoveNodeCmd(t, "node-1")))

	nodes := f.Nodes()
	require.Len(t, nodes, 1)
	assert.Equal(t, "node-2", nodes[0].ID)
}

func TestMetaFSM_Apply_NoOp(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.1:7001", 0)))

	noopCmd, err := encodeMetaCmd(MetaCmdTypeNoOp, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(noopCmd))

	assert.Len(t, f.Nodes(), 1, "NoOp must not change state")
}

func TestMetaFSM_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-2", "addr-2:7001", 1)))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	nodes := f2.Nodes()
	require.Len(t, nodes, 2)
	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n.ID] = true
	}
	assert.True(t, ids["node-1"])
	assert.True(t, ids["node-2"])
}

func makePutShardGroupCmd(t *testing.T, id string, peers []string) []byte {
	t.Helper()
	data, err := encodeMetaPutShardGroupCmd(ShardGroupEntry{ID: id, PeerIDs: peers})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutShardGroup, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutShardGroup(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0", "node-1"}))
	require.NoError(t, err)

	groups := f.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0", "node-1"}, groups[0].PeerIDs)
}

func TestMetaFSM_ShardGroups_NormalizesLegacyPeerAddresses(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-0", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-1", "10.0.0.2:7001", 0)))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"10.0.0.1:7001", "node-1"})))

	groups := f.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, []string{"node-0", "node-1"}, groups[0].PeerIDs)
}

// TestMetaFSM_OnShardGroupAdded_FiresOnApply verifies the callback registered
// via SetOnShardGroupAdded receives every applied PutShardGroup entry with
// independently allocated PeerIDs (so the callback can keep references safely).
func TestMetaFSM_OnShardGroupAdded_FiresOnApply(t *testing.T) {
	f := NewMetaFSM()

	var got []ShardGroupEntry
	var mu sync.Mutex
	f.SetOnShardGroupAdded(func(e ShardGroupEntry) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, e)
	})

	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "g-1", []string{"a", "b", "c"})))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "g-2", []string{"a", "d", "e"})))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 2)
	assert.Equal(t, "g-1", got[0].ID)
	assert.Equal(t, []string{"a", "b", "c"}, got[0].PeerIDs)
	assert.Equal(t, "g-2", got[1].ID)

	// Ensure the callback received a defensive copy — mutating must not
	// affect the FSM's stored state.
	got[0].PeerIDs[0] = "MUTATED"
	stored := f.ShardGroups()
	for _, g := range stored {
		if g.ID == "g-1" {
			assert.Equal(t, "a", g.PeerIDs[0], "FSM state must be insulated from callback mutation")
		}
	}
}

func TestMetaFSM_OnShardGroupAdded_NormalizesLegacyPeerAddresses(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-0", "10.0.0.1:7001", 0)))

	var got ShardGroupEntry
	f.SetOnShardGroupAdded(func(e ShardGroupEntry) {
		got = e
	})

	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"10.0.0.1:7001"})))

	assert.Equal(t, []string{"node-0"}, got.PeerIDs)
}

// TestMetaFSM_OnShardGroupAdded_AsyncCallbackDoesNotBlockApply verifies that
// callers who dispatch the callback to a goroutine do not block the apply
// path. The fix for the cold-start serve.go race: instantiateLocalGroup is
// heavy (BadgerDB+raft.Node), so wrapping in `go func()` keeps apply moving.
//
// REGRESSION GUARD: pre-fix, a slow callback (50ms) ran 8 times serially
// during the seed loop's apply → 400ms blocking → meta-Raft replication
// stalls → no leader. Async callback decouples apply from heavy startup.
func TestMetaFSM_OnShardGroupAdded_AsyncCallbackDoesNotBlockApply(t *testing.T) {
	f := NewMetaFSM()

	asyncDone := make(chan struct{}, 8)
	f.SetOnShardGroupAdded(func(e ShardGroupEntry) {
		go func() {
			time.Sleep(50 * time.Millisecond) // simulate heavy work
			asyncDone <- struct{}{}
		}()
	})

	start := time.Now()
	for i := 0; i < 8; i++ {
		require.NoError(t, f.applyCmd(makePutShardGroupCmd(t,
			fmt.Sprintf("g-%d", i), []string{"a"})))
	}
	elapsed := time.Since(start)

	// Apply path must complete fast (well under sum of callback delays).
	require.Less(t, elapsed, 100*time.Millisecond,
		"apply path took %v — callback blocking?", elapsed)

	// Drain async callbacks.
	for i := 0; i < 8; i++ {
		select {
		case <-asyncDone:
		case <-time.After(2 * time.Second):
			require.Failf(t, "async callback did not complete", "callback index %d", i)
		}
	}
}

func TestMetaFSM_ShardGroups_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-0", "10.0.0.1:7001", 0)))
	require.NoError(t, f.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-0"})))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	assert.Len(t, f2.Nodes(), 1)

	groups := f2.ShardGroups()
	require.Len(t, groups, 1)
	assert.Equal(t, "group-0", groups[0].ID)
	assert.Equal(t, []string{"node-0"}, groups[0].PeerIDs)
}

func TestMetaFSM_Apply_UnknownType_Noop(t *testing.T) {
	f := NewMetaFSM()
	// MetaCmdType 255 is unknown — must not panic
	unknownCmd, err := encodeMetaCmd(255, nil)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(unknownCmd))
	assert.Empty(t, f.Nodes())
}

func makePutBucketAssignmentCmd(t *testing.T, bucket, groupID string) []byte {
	t.Helper()
	data, err := encodeMetaPutBucketAssignmentCmd(bucket, groupID)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_PutBucketAssignment(t *testing.T) {
	f := NewMetaFSM()
	err := f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0"))
	require.NoError(t, err)

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-0", assignments["photos"])
}

func TestMetaFSM_Apply_PutBucketAssignment_Overwrite(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))

	assignments := f.BucketAssignments()
	require.Len(t, assignments, 1)
	assert.Equal(t, "group-1", assignments["photos"])
}

func makePutObjectIndexCmd(t *testing.T, entry ObjectIndexEntry, preserveLatest bool) []byte {
	t.Helper()
	data, err := encodeMetaPutObjectIndexCmd(entry, preserveLatest)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, data)
	require.NoError(t, err)
	return cmd
}

func makeDeleteObjectIndexCmd(t *testing.T, bucket, key, versionID string) []byte {
	t.Helper()
	data, err := encodeMetaDeleteObjectIndexCmd(bucket, key, versionID)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeDeleteObjectIndex, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_ObjectIndexPutLatestAndVersion(t *testing.T) {
	f := NewMetaFSM()
	entry := ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v1",
		PlacementGroupID: "group-2",
		Size:             5,
		ETag:             "etag",
		ECData:           1,
		ECParity:         0,
		NodeIDs:          []string{"n1"},
	}
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, entry, false)))

	latest, ok := f.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.Equal(t, "group-2", latest.PlacementGroupID)
	require.Equal(t, []string{"n1"}, latest.NodeIDs)

	ver, ok := f.ObjectIndexVersion("b", "k", "v1")
	require.True(t, ok)
	require.Equal(t, latest, ver)
}

func TestMetaFSM_ObjectIndexRejectsEmptyPlacementGroupID(t *testing.T) {
	f := NewMetaFSM()
	entry := ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1"}
	err := f.applyCmd(makePutObjectIndexCmd(t, entry, false))
	require.ErrorContains(t, err, "empty placement_group_id")
}

func TestMetaFSM_ObjectIndexDeleteMarkerLatest(t *testing.T) {
	f := NewMetaFSM()
	entry := ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "del1",
		PlacementGroupID: "group-2",
		IsDeleteMarker:   true,
	}
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, entry, false)))
	latest, ok := f.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.True(t, latest.IsDeleteMarker)
}

func TestMetaFSM_ObjectIndexPreserveLatest(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "group-1",
	}, false)))
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v0", PlacementGroupID: "group-2",
	}, true)))

	latest, ok := f.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.Equal(t, "v1", latest.VersionID)

	older, ok := f.ObjectIndexVersion("b", "k", "v0")
	require.True(t, ok)
	require.Equal(t, "group-2", older.PlacementGroupID)
}

func TestMetaFSM_ObjectIndexDeleteVersionRecomputesLatest(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "group-1", ModTime: 10,
	}, false)))
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v2", PlacementGroupID: "group-2", ModTime: 20,
	}, false)))

	require.NoError(t, f.applyCmd(makeDeleteObjectIndexCmd(t, "b", "k", "v2")))

	_, ok := f.ObjectIndexVersion("b", "k", "v2")
	require.False(t, ok)
	latest, ok := f.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.Equal(t, "v1", latest.VersionID)

	require.NoError(t, f.applyCmd(makeDeleteObjectIndexCmd(t, "b", "k", "v1")))
	_, ok = f.ObjectIndexLatest("b", "k")
	require.False(t, ok)
}

func TestMetaFSM_ObjectIndexSnapshotRestore(t *testing.T) {
	f := NewMetaFSM()
	entry := ObjectIndexEntry{
		Bucket: "b", Key: "k", VersionID: "v1",
		PlacementGroupID: "group-2",
		Size:             5,
		ContentType:      "text/plain",
		ETag:             "etag",
		ModTime:          123,
		ECData:           2,
		ECParity:         1,
		NodeIDs:          []string{"n1", "n2", "n3"},
	}
	require.NoError(t, f.applyCmd(makePutObjectIndexCmd(t, entry, false)))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	latest, ok := f2.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.Equal(t, entry, latest)
}

func TestMetaFSM_BucketAssignments_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "videos", "group-1")))

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	assignments := f2.BucketAssignments()
	require.Len(t, assignments, 2)
	assert.Equal(t, "group-0", assignments["photos"])
	assert.Equal(t, "group-1", assignments["videos"])
}

func makeIcebergCreateNamespaceCmd(t *testing.T, requestID string, namespace []string, properties map[string]string) []byte {
	t.Helper()
	data, err := encodeMetaIcebergCreateNamespaceCmd(IcebergCreateNamespaceCmd{
		RequestID:  requestID,
		Namespace:  namespace,
		Properties: properties,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergCreateNamespace, data)
	require.NoError(t, err)
	return cmd
}

func makeIcebergCreateTableCmd(t *testing.T, requestID string, ident icebergcatalog.Identifier, metadataLocation string, properties map[string]string) []byte {
	t.Helper()
	data, err := encodeMetaIcebergCreateTableCmd(IcebergCreateTableCmd{
		RequestID:        requestID,
		Identifier:       ident,
		MetadataLocation: metadataLocation,
		Properties:       properties,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergCreateTable, data)
	require.NoError(t, err)
	return cmd
}

func makeIcebergDeleteNamespaceCmd(t *testing.T, requestID string, namespace []string) []byte {
	t.Helper()
	data, err := encodeMetaIcebergDeleteNamespaceCmd(IcebergDeleteNamespaceCmd{
		RequestID: requestID,
		Namespace: namespace,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergDeleteNamespace, data)
	require.NoError(t, err)
	return cmd
}

func makeIcebergCommitTableCmd(t *testing.T, requestID string, ident icebergcatalog.Identifier, expected, next string) []byte {
	t.Helper()
	data, err := encodeMetaIcebergCommitTableCmd(IcebergCommitTableCmd{
		RequestID:                requestID,
		Identifier:               ident,
		ExpectedMetadataLocation: expected,
		NewMetadataLocation:      next,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergCommitTable, data)
	require.NoError(t, err)
	return cmd
}

func makeIcebergDeleteTableCmd(t *testing.T, requestID string, ident icebergcatalog.Identifier) []byte {
	t.Helper()
	data, err := encodeMetaIcebergDeleteTableCmd(IcebergDeleteTableCmd{
		RequestID:  requestID,
		Identifier: ident,
	})
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeIcebergDeleteTable, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_IcebergCatalog_SnapshotRestoreStoresPointerOnly(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeIcebergCreateNamespaceCmd(t, "ns-1", []string{"analytics"}, map[string]string{"owner": "eng"})))
	require.NoError(t, f.applyCmd(makeIcebergCreateTableCmd(t, "tbl-1", icebergcatalog.Identifier{
		Namespace: []string{"analytics"},
		Name:      "events",
	}, "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json", map[string]string{"format-version": "2"})))

	ns, ok := f.IcebergNamespace("", []string{"analytics"})
	require.True(t, ok)
	require.Equal(t, []string{"analytics"}, ns.Namespace)
	require.Equal(t, "eng", ns.Properties["owner"])

	tbl, ok := f.IcebergTable("", icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"})
	require.True(t, ok)
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json", tbl.MetadataLocation)
	require.Equal(t, "2", tbl.Properties["format-version"])

	snap, err := f.Snapshot()
	require.NoError(t, err)
	require.NotContains(t, string(snap), "current-snapshot-id", "metadata JSON bodies must not be snapshotted into meta-Raft")

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))
	restored, ok := f2.IcebergTable("", icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"})
	require.True(t, ok)
	require.Equal(t, tbl.MetadataLocation, restored.MetadataLocation)
	require.Equal(t, tbl.Identifier, restored.Identifier)
}

func TestMetaFSM_IcebergApplyPublishesTypedResultWithoutReturningApplyError(t *testing.T) {
	f := NewMetaFSM()
	results := make(map[string]error)
	f.SetOnIcebergApplyResult(func(requestID string, err error) {
		results[requestID] = err
	})

	require.NoError(t, f.applyCmd(makeIcebergCreateNamespaceCmd(t, "first", []string{"analytics"}, nil)))
	require.NoError(t, f.applyCmd(makeIcebergCreateNamespaceCmd(t, "duplicate", []string{"analytics"}, nil)))

	require.NoError(t, results["first"])
	require.ErrorIs(t, results["duplicate"], icebergcatalog.ErrNamespaceExists)
}

func TestMetaFSM_IcebergCatalog_CommitDeleteAndTypedErrors(t *testing.T) {
	f := NewMetaFSM()
	results := make(map[string]error)
	f.SetOnIcebergApplyResult(func(requestID string, err error) {
		results[requestID] = err
	})
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}

	require.NoError(t, f.applyCmd(makeIcebergCreateTableCmd(t, "missing-ns-table", ident, "s3://bucket/warehouse/a/b/metadata/00000.json", nil)))
	require.ErrorIs(t, results["missing-ns-table"], icebergcatalog.ErrNamespaceNotFound)

	require.NoError(t, f.applyCmd(makeIcebergCreateNamespaceCmd(t, "create-ns", []string{"analytics"}, nil)))
	require.NoError(t, f.applyCmd(makeIcebergCreateTableCmd(t, "create-table", ident, "s3://bucket/warehouse/a/b/metadata/00000.json", nil)))
	require.NoError(t, f.applyCmd(makeIcebergCommitTableCmd(t, "commit-ok", ident, "s3://bucket/warehouse/a/b/metadata/00000.json", "s3://bucket/warehouse/a/b/metadata/00001.json")))
	require.NoError(t, results["commit-ok"])
	table, ok := f.IcebergTable("", ident)
	require.True(t, ok)
	require.Equal(t, "s3://bucket/warehouse/a/b/metadata/00001.json", table.MetadataLocation)

	require.NoError(t, f.applyCmd(makeIcebergCommitTableCmd(t, "commit-stale", ident, "s3://bucket/warehouse/a/b/metadata/00000.json", "s3://bucket/warehouse/a/b/metadata/00002.json")))
	require.ErrorIs(t, results["commit-stale"], icebergcatalog.ErrCommitFailed)
	table, ok = f.IcebergTable("", ident)
	require.True(t, ok)
	require.Equal(t, "s3://bucket/warehouse/a/b/metadata/00001.json", table.MetadataLocation)

	require.NoError(t, f.applyCmd(makeIcebergDeleteNamespaceCmd(t, "delete-ns-not-empty", []string{"analytics"})))
	require.ErrorIs(t, results["delete-ns-not-empty"], icebergcatalog.ErrNamespaceNotEmpty)

	require.NoError(t, f.applyCmd(makeIcebergDeleteTableCmd(t, "delete-table", ident)))
	require.NoError(t, results["delete-table"])
	_, ok = f.IcebergTable("", ident)
	require.False(t, ok)

	require.NoError(t, f.applyCmd(makeIcebergDeleteTableCmd(t, "delete-missing-table", ident)))
	require.ErrorIs(t, results["delete-missing-table"], icebergcatalog.ErrTableNotFound)
	require.NoError(t, f.applyCmd(makeIcebergDeleteNamespaceCmd(t, "delete-ns", []string{"analytics"})))
	require.NoError(t, results["delete-ns"])
	require.NoError(t, f.applyCmd(makeIcebergDeleteNamespaceCmd(t, "delete-missing-ns", []string{"analytics"})))
	require.ErrorIs(t, results["delete-missing-ns"], icebergcatalog.ErrNamespaceNotFound)
}

func TestMetaFSM_OnBucketAssigned_CallbackFired(t *testing.T) {
	f := NewMetaFSM()
	var cbBucket, cbGroup string
	f.SetOnBucketAssigned(func(bucket, groupID string) {
		cbBucket = bucket
		cbGroup = groupID
	})
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	assert.Equal(t, "photos", cbBucket)
	assert.Equal(t, "group-0", cbGroup)
}

func TestMetaFSM_Restore_FiresOnBucketAssignedCallback(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-0")))
	require.NoError(t, f.applyCmd(makePutBucketAssignmentCmd(t, "videos", "group-1")))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	got := make(map[string]string)
	f2.SetOnBucketAssigned(func(bucket, groupID string) {
		got[bucket] = groupID
	})
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	assert.Equal(t, map[string]string{"photos": "group-0", "videos": "group-1"}, got)
}

// --- PR-D: LoadSnapshot + RebalancePlan tests ---

func makeSetLoadSnapshotCmd(t *testing.T, entries []LoadStatEntry) []byte {
	t.Helper()
	data, err := encodeMetaSetLoadSnapshotCmd(entries)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeSetLoadSnapshot, data)
	require.NoError(t, err)
	return cmd
}

func makeProposeRebalancePlanCmd(t *testing.T, plan RebalancePlan) []byte {
	t.Helper()
	data, err := encodeMetaProposeRebalancePlanCmd(plan)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeProposeRebalancePlan, data)
	require.NoError(t, err)
	return cmd
}

func makeAbortPlanCmd(t *testing.T, planID string) []byte {
	t.Helper()
	return makeAbortPlanCmdWithReason(t, planID, clusterpb.AbortPlanReasonUnknown)
}

func makeAbortPlanCmdWithReason(t *testing.T, planID string, reason clusterpb.AbortPlanReason) []byte {
	t.Helper()
	data, err := encodeMetaAbortPlanCmd(planID, reason)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeAbortPlan, data)
	require.NoError(t, err)
	return cmd
}

func TestMetaFSM_Apply_SetLoadSnapshot(t *testing.T) {
	f := NewMetaFSM()
	entries := []LoadStatEntry{
		{NodeID: "n1", DiskUsedPct: 80.0, DiskAvailBytes: 1000},
		{NodeID: "n2", DiskUsedPct: 20.0, DiskAvailBytes: 9000},
	}
	require.NoError(t, f.applyCmd(makeSetLoadSnapshotCmd(t, entries)))

	snap := f.LoadSnapshot()
	require.Len(t, snap, 2)
	assert.InDelta(t, 80.0, snap["n1"].DiskUsedPct, 0.01)
}

func TestMetaFSM_Apply_ProposeRebalancePlan(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{
		PlanID:    "plan-1",
		GroupID:   "group-0",
		FromNode:  "n1",
		ToNode:    "n2",
		CreatedAt: time.Now(),
	}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	assert.Equal(t, "plan-1", f.ActivePlanID())
}

func TestMetaFSM_Apply_ProposeRebalancePlan_RejectsIfActive(t *testing.T) {
	f := NewMetaFSM()
	plan1 := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	plan2 := RebalancePlan{PlanID: "plan-2", GroupID: "g0", FromNode: "n1", ToNode: "n3", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan1)))
	require.ErrorContains(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan2)), "active plan")
}

func TestMetaFSM_Apply_AbortPlan(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	require.NoError(t, f.applyCmd(makeAbortPlanCmd(t, "plan-1")))
	assert.Empty(t, f.ActivePlanID())
}

func TestMetaFSM_Apply_AbortPlan_Idempotent(t *testing.T) {
	f := NewMetaFSM()
	// Aborting when no plan is active must be a no-op (not an error).
	require.NoError(t, f.applyCmd(makeAbortPlanCmd(t, "nonexistent")))
	assert.Empty(t, f.ActivePlanID())
}

// TestEncodeMetaAbortPlanCmd_ReasonRoundTrip verifies that every AbortPlanReason
// value survives a FlatBuffers encode → decode cycle intact.
func TestEncodeMetaAbortPlanCmd_ReasonRoundTrip(t *testing.T) {
	cases := []struct {
		reason clusterpb.AbortPlanReason
		name   string
	}{
		{clusterpb.AbortPlanReasonUnknown, "Unknown"},
		{clusterpb.AbortPlanReasonTimeout, "Timeout"},
		{clusterpb.AbortPlanReasonExecutionFailed, "ExecutionFailed"},
		{clusterpb.AbortPlanReasonCompleted, "Completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := encodeMetaAbortPlanCmd("plan-rt", tc.reason)
			require.NoError(t, err)
			cmd := clusterpb.GetRootAsMetaAbortPlanCmd(data, 0)
			assert.Equal(t, tc.reason, cmd.Reason())
			assert.Equal(t, tc.name, cmd.Reason().String())
		})
	}
}

func TestMetaFSM_OnRebalancePlan_CallbackFired(t *testing.T) {
	f := NewMetaFSM()
	var got *RebalancePlan
	f.SetOnRebalancePlan(func(p *RebalancePlan) { got = p })

	plan := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))
	require.NotNil(t, got)
	assert.Equal(t, "plan-1", got.PlanID)
}

func TestMetaFSM_LoadSnapshot_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	entries := []LoadStatEntry{{NodeID: "n1", DiskUsedPct: 75.0}}
	require.NoError(t, f.applyCmd(makeSetLoadSnapshotCmd(t, entries)))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))
	ls := f2.LoadSnapshot()
	assert.InDelta(t, 75.0, ls["n1"].DiskUsedPct, 0.01)
}

func TestMetaFSM_ActivePlan_Snapshot_Restore(t *testing.T) {
	f := NewMetaFSM()
	plan := RebalancePlan{PlanID: "plan-99", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, f.applyCmd(makeProposeRebalancePlanCmd(t, plan)))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))
	assert.Equal(t, "plan-99", f2.ActivePlanID())
}

// TestMetaFSM_Dispatch_KeyCreateScoped verifies that MetaCmdTypeIAMKeyCreateScoped (type 30)
// is present in the dispatch table. Without a configured IAM applier, applyIAM returns
// "IAM applier not configured" — proving dispatch reached the IAM path rather than
// falling through to the default (silent no-op) branch.
func TestMetaFSM_Dispatch_KeyCreateScoped(t *testing.T) {
	f := NewMetaFSM() // iamApplier is nil by default
	cmd, err := encodeMetaCmd(MetaCmdTypeIAMKeyCreateScoped, []byte{})
	require.NoError(t, err)

	applyErr := f.applyCmd(cmd)
	// If type 30 were not in the switch, default returns nil — this would fail.
	require.Error(t, applyErr, "type 30 must not fall through to silent default")
	assert.Contains(t, applyErr.Error(), "IAM applier not configured")
}

// TestMetaFSM_Dispatch_BucketUpstreamPut verifies that MetaCmdTypeIAMBucketUpstreamPut
// (type 32) is present in the dispatch table. Without a configured IAM applier,
// applyIAM returns "IAM applier not configured" — proving dispatch reached the
// IAM path rather than falling through to the default (silent no-op) branch.
func TestMetaFSM_Dispatch_BucketUpstreamPut(t *testing.T) {
	f := NewMetaFSM() // iamApplier is nil by default
	cmd, err := encodeMetaCmd(MetaCmdTypeIAMBucketUpstreamPut, []byte{})
	require.NoError(t, err)

	applyErr := f.applyCmd(cmd)
	require.Error(t, applyErr, "type 32 must not fall through to silent default")
	assert.Contains(t, applyErr.Error(), "IAM applier not configured")
}

// TestMetaFSM_Dispatch_BucketUpstreamDelete verifies that MetaCmdTypeIAMBucketUpstreamDelete
// (type 33) is present in the dispatch table.
func TestMetaFSM_Dispatch_BucketUpstreamDelete(t *testing.T) {
	f := NewMetaFSM() // iamApplier is nil by default
	cmd, err := encodeMetaCmd(MetaCmdTypeIAMBucketUpstreamDelete, []byte{})
	require.NoError(t, err)

	applyErr := f.applyCmd(cmd)
	require.Error(t, applyErr, "type 33 must not fall through to silent default")
	assert.Contains(t, applyErr.Error(), "IAM applier not configured")
}

// TestMetaFSM_Dispatch_UnknownCmd_GracefulNoOp is the rolling-upgrade gate test.
// A follower running an older binary (without knowledge of a new MetaCmdType) must
// not crash or return an error — it should apply the entry as a no-op and let raft
// advance. This mirrors how a v0.0.98.0 follower would handle type 30 before upgrading.
func TestMetaFSM_Dispatch_UnknownCmd_GracefulNoOp(t *testing.T) {
	f := NewMetaFSM()
	cmd, err := encodeMetaCmd(MetaCmdType(99), nil)
	require.NoError(t, err)
	before := testutil.ToFloat64(metrics.UnknownMetaCmdTotal.WithLabelValues("99"))

	// Unknown types must be silently ignored — no error, no panic.
	require.NoError(t, f.applyCmd(cmd), "unknown cmd must not fail (rolling-upgrade gate)")
	assert.Empty(t, f.Nodes(), "unknown cmd must not mutate state")
	after := testutil.ToFloat64(metrics.UnknownMetaCmdTotal.WithLabelValues("99"))
	assert.Equal(t, before+1, after, "unknown cmd must increment the rolling-upgrade telemetry counter")
}

func newTestLifecycleDB(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func openTestBadgerAt(t *testing.T, dir string) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	return db
}

func TestApplyBucketLifecyclePut_WritesStore(t *testing.T) {
	f := NewMetaFSM()
	store := lifecycle.NewStore(newTestLifecycleDB(t))
	f.SetLifecycle(store)

	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID><Status>Enabled</Status></Rule></LifecycleConfiguration>`)
	payload := lifecycle.EncodePutPayload("b1", raw)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeBucketLifecyclePut, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))

	got, err := store.Get("b1")
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestApplyBucketLifecycleDelete_RemovesStore(t *testing.T) {
	f := NewMetaFSM()
	store := lifecycle.NewStore(newTestLifecycleDB(t))
	f.SetLifecycle(store)
	require.NoError(t, store.PutRaw("b1", []byte(`<LifecycleConfiguration><Rule><ID>r1</ID></Rule></LifecycleConfiguration>`)))

	payload := lifecycle.EncodeDeletePayload("b1")
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeBucketLifecycleDelete, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))

	got, err := store.Get("b1")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestApplyNfsExportUpsert_WritesStore(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)

	cfg := nfsexport.Config{ReadOnly: true}
	payload, err := nfsexport.EncodeUpsertPayload("b1", cfg)
	require.NoError(t, err)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportUpsert, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))

	got, ok := store.Get("b1")
	require.True(t, ok)
	require.Equal(t, nfsexport.Config{ReadOnly: true, FsidMajor: 1, FsidMinor: 1, Generation: 1}, got)
}

func TestApplyNfsExportCreateRejectsExisting(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)

	payload, err := nfsexport.EncodeUpsertPayload("b1", nfsexport.Config{ReadOnly: false})
	require.NoError(t, err)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportCreate, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))

	payload, err = nfsexport.EncodeUpsertPayload("b1", nfsexport.Config{ReadOnly: true})
	require.NoError(t, err)
	data, err = encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportCreate, payload)
	require.NoError(t, err)
	require.ErrorIs(t, f.applyCmd(data), nfsexport.ErrExportExists)

	got, ok := store.Get("b1")
	require.True(t, ok)
	require.False(t, got.ReadOnly)
	require.Equal(t, uint64(1), got.Generation)
}

func TestApplyNfsExport_ChangeCallback(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)

	var calls int
	f.SetOnNfsExportChange(func() { calls++ })

	cfg := nfsexport.Config{FsidMajor: 1, FsidMinor: 2, Generation: 3}
	upsertPayload, err := nfsexport.EncodeUpsertPayload("b1", cfg)
	require.NoError(t, err)
	upsertData, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportUpsert, upsertPayload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(upsertData))

	deletePayload, err := nfsexport.EncodeDeletePayload("b1")
	require.NoError(t, err)
	deleteData, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportDelete, deletePayload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(deleteData))

	require.Equal(t, 2, calls)
}

func TestApplyNfsExportUpsert_AllocatorMonotonic(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)

	for i := 1; i <= 100; i++ {
		payload, err := nfsexport.EncodeUpsertPayload(fmt.Sprintf("b-%03d", i), nfsexport.Config{})
		require.NoError(t, err)
		data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportUpsert, payload)
		require.NoError(t, err)
		require.NoError(t, f.applyCmd(data))
		cfg, ok := store.Get(fmt.Sprintf("b-%03d", i))
		require.True(t, ok)
		require.Equal(t, uint64(i), cfg.FsidMinor)
		require.Equal(t, uint64(1), cfg.Generation)
	}
}

func TestApplyNfsExportUpsert_DeleteThenReuse(t *testing.T) {
	dir := t.TempDir()
	db := openTestBadgerAt(t, dir)
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	f := NewMetaFSM()
	f.SetExportStore(store)
	for _, bucket := range []string{"a", "b", "c"} {
		payload, err := nfsexport.EncodeUpsertPayload(bucket, nfsexport.Config{})
		require.NoError(t, err)
		data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportUpsert, payload)
		require.NoError(t, err)
		require.NoError(t, f.applyCmd(data))
	}
	deletePayload, err := nfsexport.EncodeDeletePayload("c")
	require.NoError(t, err)
	deleteData, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportDelete, deletePayload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(deleteData))
	require.NoError(t, db.Close())

	db = openTestBadgerAt(t, dir)
	defer db.Close()
	store, err = nfsexport.OpenStore(db)
	require.NoError(t, err)
	f = NewMetaFSM()
	f.SetExportStore(store)
	payload, err := nfsexport.EncodeUpsertPayload("d", nfsexport.Config{})
	require.NoError(t, err)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportUpsert, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))
	cfg, ok := store.Get("d")
	require.True(t, ok)
	require.Equal(t, uint64(4), cfg.FsidMinor)
}

func TestApplyNfsExportDelete_Idempotent(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)
	require.NoError(t, store.Put("b1", nfsexport.Config{FsidMinor: 1}))

	payload, err := nfsexport.EncodeDeletePayload("b1")
	require.NoError(t, err)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportDelete, payload)
	require.NoError(t, err)
	require.NoError(t, f.applyCmd(data))
	require.NoError(t, f.applyCmd(data))

	_, ok := store.Get("b1")
	require.False(t, ok)
}

func TestApplyNfsExportBucketDeleteCascadeDeletesExportAfterBucket(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)
	_, err = store.ApplyUpsert("b1", false, 1)
	require.NoError(t, err)

	payload, err := nfsexport.EncodeBucketDeleteCascadePayload("b1", true)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade, payload)
	require.NoError(t, err)

	require.NoError(t, f.applyCmd(cmd))
	_, ok := store.Get("b1")
	require.False(t, ok)
}

func TestMetaFSM_NfsExportsSnapshotRestore(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)
	_, err = store.ApplyUpsert("b1", true, 7)
	require.NoError(t, err)
	before, ok := store.Get("b1")
	require.True(t, ok)

	snap, err := f.Snapshot()
	require.NoError(t, err)

	restoredStore, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f2 := NewMetaFSM()
	f2.SetExportStore(restoredStore)
	calls := 0
	f2.SetOnNfsExportChange(func() { calls++ })
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))
	require.Equal(t, 1, calls)

	after, ok := restoredStore.Get("b1")
	require.True(t, ok)
	require.Equal(t, before, after)
}

func TestMetaFSM_RestoreLegacySnapshotKeepsNfsExports(t *testing.T) {
	f := NewMetaFSM()
	store, err := nfsexport.OpenStore(newTestLifecycleDB(t))
	require.NoError(t, err)
	f.SetExportStore(store)
	before, err := store.ApplyUpsert("b1", true, 7)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(64)
	clusterpb.MetaStateSnapshotStart(b)
	root := clusterpb.MetaStateSnapshotEnd(b)
	b.Finish(root)

	require.NoError(t, f.Restore(raft.SnapshotMeta{}, append([]byte(nil), b.FinishedBytes()...)))
	after, ok := store.Get("b1")
	require.True(t, ok)
	require.Equal(t, before, after)
}

func TestApplyNfsExportMissingStore_ReturnsError(t *testing.T) {
	f := NewMetaFSM()
	payload, err := nfsexport.EncodeDeletePayload("b1")
	require.NoError(t, err)
	data, err := encodeMetaCmd(clusterpb.MetaCmdTypeNfsExportDelete, payload)
	require.NoError(t, err)

	applyErr := f.applyCmd(data)
	require.Error(t, applyErr)
	require.Contains(t, applyErr.Error(), "NFS export store not wired")
}
