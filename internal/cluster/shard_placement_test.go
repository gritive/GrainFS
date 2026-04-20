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

	got, ok := fsm.LookupShardPlacement("bkt", "obj")
	require.True(t, ok, "expected placement to exist")
	assert.Equal(t, nodes, got)
}

func TestFSM_LookupShardPlacement_NotFound(t *testing.T) {
	fsm := NewFSM(newTestDB(t))
	got, ok := fsm.LookupShardPlacement("ghost", "obj")
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestFSM_DeleteShardPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: []string{"a", "b"},
	})
	require.NoError(t, fsm.Apply(put))
	_, ok := fsm.LookupShardPlacement("b", "k")
	require.True(t, ok)

	del, _ := EncodeCommand(CmdDeleteShardPlacement, DeleteShardPlacementCmd{
		Bucket: "b", Key: "k",
	})
	require.NoError(t, fsm.Apply(del))

	_, ok = fsm.LookupShardPlacement("b", "k")
	assert.False(t, ok)
}

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

	got, ok := fsm.LookupShardPlacement("b", "k")
	require.True(t, ok)
	assert.Equal(t, []string{"m0", "m1", "m2"}, got, "latest placement should win")
}

func TestFSM_Snapshot_IncludesPlacement(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	nodes := []string{"n0", "n1", "n2", "n3"}
	put, _ := EncodeCommand(CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket: "b", Key: "k", NodeIDs: nodes,
	})
	require.NoError(t, fsm.Apply(put))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	// Restore into a fresh FSM and verify placement survives.
	freshDB := newTestDB(t)
	freshFSM := NewFSM(freshDB)
	require.NoError(t, freshFSM.Restore(snap))

	got, ok := freshFSM.LookupShardPlacement("b", "k")
	require.True(t, ok)
	assert.Equal(t, nodes, got)
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

	_, ok := fsm.LookupShardPlacement("b", "k")
	assert.False(t, ok, "DeleteObject should cascade to placement")
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
	got, ok := fsm.LookupShardPlacement("b", "k")
	require.True(t, ok)
	assert.Empty(t, got)
}

// Ensure encoding does not leak other keys' placements.
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

	got1, _ := fsm.LookupShardPlacement("b", "k1")
	got2, _ := fsm.LookupShardPlacement("b", "k2")
	assert.Equal(t, []string{"n0", "n1"}, got1)
	assert.Equal(t, []string{"n2", "n3"}, got2)

	// Verify badgerDB keyspace: only 2 placement keys.
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
	assert.Equal(t, 2, count)
}
