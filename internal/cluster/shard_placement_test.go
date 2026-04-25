package cluster

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardPlacementCmd_EncodeDecode(t *testing.T) {
	cases := []struct {
		name string
		cmd  PutShardPlacementCmd
	}{
		{
			name: "4+2",
			cmd: PutShardPlacementCmd{
				Bucket:  "bkt",
				Key:     "obj/a",
				NodeIDs: []string{"n0", "n1", "n2", "n3", "n4", "n5"},
			},
		},
		{
			name: "6+3",
			cmd: PutShardPlacementCmd{
				Bucket:  "bkt",
				Key:     "obj/b",
				NodeIDs: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"},
			},
		},
		{
			name: "minimal_1+1",
			cmd: PutShardPlacementCmd{
				Bucket:  "b",
				Key:     "k",
				NodeIDs: []string{"x", "y"},
			},
		},
		{
			name: "unicode_key",
			cmd: PutShardPlacementCmd{
				Bucket:  "버킷",
				Key:     "경로/객체",
				NodeIDs: []string{"n1", "n2"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := EncodeCommand(CmdPutShardPlacement, tc.cmd)
			require.NoError(t, err)
			cmd, err := DecodeCommand(raw)
			require.NoError(t, err)
			require.Equal(t, CmdPutShardPlacement, cmd.Type)
			decoded, err := decodePutShardPlacementCmd(cmd.Data)
			require.NoError(t, err)
			assert.Equal(t, tc.cmd, decoded)
		})
	}
}

func TestDeleteShardPlacementCmd_EncodeDecode(t *testing.T) {
	orig := DeleteShardPlacementCmd{Bucket: "b", Key: "k"}
	raw, err := EncodeCommand(CmdDeleteShardPlacement, orig)
	require.NoError(t, err)
	cmd, err := DecodeCommand(raw)
	require.NoError(t, err)
	require.Equal(t, CmdDeleteShardPlacement, cmd.Type)
	decoded, err := decodeDeleteShardPlacementCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, orig, decoded)
}

// CmdPutShardPlacement is now a no-op — FSM accepts it without error and
// does not write placement records (ring-derived placement has no BadgerDB footprint).
func TestFSM_PutShardPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	nodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	raw, err := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket:  "bkt",
		Key:     "obj",
		NodeIDs: nodes,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	// No placement written; LookupShardPlacement returns empty record.
	got, err := fsm.LookupShardPlacement("bkt", "obj")
	require.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, got)
}

func TestFSM_LookupShardPlacement_NotFound(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	got, err := fsm.LookupShardPlacement("ghost", "obj")
	assert.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, got)
}

func TestFSM_DeleteShardPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"a", "b"},
	})
	require.NoError(t, fsm.Apply(put))
	_, err := fsm.LookupShardPlacement("b", "k")
	require.NoError(t, err)

	del, _ := EncodeCommand(CmdDeleteShardPlacement, DeleteShardPlacementCmd{
		Bucket: "b", Key: "k",
	})
	require.NoError(t, fsm.Apply(del))

	nodes, err2 := fsm.LookupShardPlacement("b", "k")
	assert.NoError(t, err2)
	assert.Equal(t, PlacementRecord{}, nodes)
}

// CmdPutShardPlacement is a no-op; repeated applies don't cause errors.
func TestFSM_PutShardPlacement_Overwrite(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	v1, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"n0", "n1"},
	})
	require.NoError(t, fsm.Apply(v1))

	v2, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"m0", "m1", "m2"},
	})
	require.NoError(t, fsm.Apply(v2))

	// Both are no-ops; placement is derived from the ring.
	got, err := fsm.LookupShardPlacement("b", "k")
	require.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, got)
}

// CmdPutShardPlacement no-op: snapshot does not include placement rows.
func TestFSM_Snapshot_IncludesPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"n0", "n1", "n2", "n3"},
	})
	require.NoError(t, fsm.Apply(put))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	// Restore into a fresh FSM and verify no stale placement rows appear.
	freshDB := newTestDB(t)
	freshFSM := NewFSM(freshDB)
	require.NoError(t, freshFSM.Restore(snap))

	got, err := freshFSM.LookupShardPlacement("b", "k")
	require.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, got)
}

func TestFSM_DeleteObject_CascadesToPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Bucket + object meta
	cb, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(cb))
	po, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 1, ContentType: "text/plain", ETag: "e", ModTime: 1,
	})
	require.NoError(t, fsm.Apply(po))

	// Placement
	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"n0", "n1"},
	})
	require.NoError(t, fsm.Apply(put))

	// Delete object — placement should vanish too.
	del, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "k"})
	require.NoError(t, fsm.Apply(del))

	cascaded, err := fsm.LookupShardPlacement("b", "k")
	assert.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, cascaded, "DeleteObject should cascade to placement")
}

// TestFSM_DeleteObject_Tombstone_CascadesToVersionedPlacement verifies that a
// versioned delete (tombstone path) removes the placement record stored under
// key+"/"+prevVersionID, not just the bare key.
func TestFSM_DeleteObject_Tombstone_CascadesToVersionedPlacement(t *testing.T) {
	fsm := NewFSM(newTestDB(t))

	cb, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(cb))

	const prevVID = "v-prev-0001"
	po, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 1, ContentType: "text/plain",
		ETag: "e", ModTime: 1, VersionID: prevVID,
	})
	require.NoError(t, fsm.Apply(po))

	// Placement stored under versioned shardKey.
	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k/" + prevVID, NodeIDs: []string{"n0", "n1"},
	})
	require.NoError(t, fsm.Apply(put))

	// Tombstone — new versionID for the delete marker.
	del, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "k", VersionID: "v-del-0002"})
	require.NoError(t, fsm.Apply(del))

	// Versioned placement must be gone.
	gone, err := fsm.LookupShardPlacement("b", "k/"+prevVID)
	assert.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, gone, "tombstone must cascade to versioned placement")
}

// TestFSM_DeleteObjectVersion_CascadesToPlacement verifies that hard-deleting
// a specific version removes its versioned placement record.
func TestFSM_DeleteObjectVersion_CascadesToPlacement(t *testing.T) {
	fsm := NewFSM(newTestDB(t))

	cb, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(cb))

	const vid = "v-0001"
	po, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 1, ContentType: "text/plain",
		ETag: "e", ModTime: 1, VersionID: vid,
	})
	require.NoError(t, fsm.Apply(po))

	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k/" + vid, NodeIDs: []string{"n0", "n1"},
	})
	require.NoError(t, fsm.Apply(put))

	// Hard-delete specific version.
	dv, _ := EncodeCommand(CmdDeleteObjectVersion, DeleteObjectVersionCmd{Bucket: "b", Key: "k", VersionID: vid})
	require.NoError(t, fsm.Apply(dv))

	gone, err := fsm.LookupShardPlacement("b", "k/"+vid)
	assert.NoError(t, err)
	assert.Equal(t, PlacementRecord{}, gone, "DeleteObjectVersion must cascade to versioned placement")
}

func TestShardPlacementKey_Format(t *testing.T) {
	got := shardPlacementKey("b", "k")
	assert.Equal(t, []byte("placement:b/k"), got)
}

// Verify the placement prefix doesn't collide with existing FSM key namespaces.
func TestShardPlacementKey_NoPrefixCollision(t *testing.T) {
	placement := string(shardPlacementKey("x", "y"))
	assert.NotContains(t, placement, "bucket:")
	assert.NotContains(t, placement, "obj:")
	assert.NotContains(t, placement, "mpu:")
	assert.NotContains(t, placement, "policy:")
	assert.NotContains(t, placement, "pending-mig:")
}

// Placement with empty node_ids list should still round-trip (semantically
// equivalent to "no shards placed"). Not expected in production but codec
// must not crash.
func TestShardPlacementCmd_EmptyNodes(t *testing.T) {
	orig := PutShardPlacementCmd{Bucket: "b", Key: "k", NodeIDs: []string{}}
	raw, err := EncodeCommand(CmdPutShardPlacement, orig)
	require.NoError(t, err)
	cmd, err := DecodeCommand(raw)
	require.NoError(t, err)
	decoded, err := decodePutShardPlacementCmd(cmd.Data)
	require.NoError(t, err)
	assert.Equal(t, orig.Bucket, decoded.Bucket)
	assert.Equal(t, orig.Key, decoded.Key)
	assert.Empty(t, decoded.NodeIDs)

	// Applying it should succeed and LookupShardPlacement should return ok=true.
	db := newTestDB(t)
	fsm := NewFSM(db)
	require.NoError(t, fsm.Apply(raw))
	got, err := fsm.LookupShardPlacement("b", "k")
	require.NoError(t, err)
	assert.Empty(t, got.Nodes)
}

// CmdPutShardPlacement is now a no-op; verify FSM accepts it without error and
// that no placement keys are written (placement is derived from the ring instead).
func TestFSM_PlacementIsolation(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	p1, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k1", NodeIDs: []string{"n0", "n1"},
	})
	p2, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k2", NodeIDs: []string{"n2", "n3"},
	})
	require.NoError(t, fsm.Apply(p1))
	require.NoError(t, fsm.Apply(p2))

	// No placement keys should be written — ring-based placement has no BadgerDB footprint.
	count := 0
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte("placement:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
