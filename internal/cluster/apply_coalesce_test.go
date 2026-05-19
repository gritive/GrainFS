package cluster

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// newCoalesceTestFSM constructs an FSM backed by a temporary BadgerDB.
func newCoalesceTestFSM(t *testing.T) *FSM {
	t.Helper()
	db := newTestDB(t)
	return NewFSM(db, newStateKeyspaceEmpty())
}

// coalesceWriteMeta marshals + persists an objectMeta directly via the FSM
// keyspace so tests can seed read-modify-write cycles without proposing.
func coalesceWriteMeta(t *testing.T, f *FSM, bucket, key string, m objectMeta) {
	t.Helper()
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		return f.setValue(txn, f.keys.ObjectMetaKey(bucket, key), raw)
	}))
}

// coalesceReadMeta reads back an objectMeta from the FSM's badger via the
// canonical openValue path (handles AES envelope).
func coalesceReadMeta(t *testing.T, f *FSM, bucket, key string) objectMeta {
	t.Helper()
	var got objectMeta
	require.NoError(t, f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(f.keys.ObjectMetaKey(bucket, key))
		if err != nil {
			return err
		}
		return item.Value(func(raw []byte) error {
			v, oerr := f.openValue(item.Key(), raw)
			if oerr != nil {
				return oerr
			}
			m, derr := unmarshalObjectMeta(v)
			if derr != nil {
				return derr
			}
			got = m
			return nil
		})
	}))
	return got
}

func TestApplyCoalesceSegmentsHappyPath(t *testing.T) {
	f := newCoalesceTestFSM(t)
	seed := objectMeta{
		Key: "k", Size: 30, IsAppendable: true,
		Segments: []storage.SegmentRef{
			{BlobID: "s1", Size: 10, Checksum: []byte("e1")},
			{BlobID: "s2", Size: 10, Checksum: []byte("e2")},
			{BlobID: "s3", Size: 10, Checksum: []byte("e3")},
		},
	}
	coalesceWriteMeta(t, f, "b", "k", seed)

	cmd := CoalesceSegmentsCmd{
		Bucket: "b", Key: "k",
		CoalescedID: "c1", ShardKey: "k/coalesced/c1",
		Size: 20, ETag: "etag-c1",
		ConsumedSegmentIDs: []string{"s1", "s2"},
	}
	raw, err := encodeCoalesceSegmentsCmd(cmd)
	require.NoError(t, err)
	require.NoError(t, f.applyCoalesceSegmentsFromCmd(raw))

	got := coalesceReadMeta(t, f, "b", "k")
	if len(got.Coalesced) != 1 || got.Coalesced[0].CoalescedID != "c1" {
		t.Fatalf("coalesced not added: %+v", got.Coalesced)
	}
	if len(got.Segments) != 1 || got.Segments[0].BlobID != "s3" {
		t.Fatalf("segments not pruned: %+v", got.Segments)
	}
	if got.Size != 30 {
		t.Fatalf("Size changed: %d", got.Size)
	}
}

func TestApplyCoalesceSegmentsIdempotentReplay(t *testing.T) {
	f := newCoalesceTestFSM(t)
	seed := objectMeta{
		Key: "k", Size: 30, IsAppendable: true,
		Segments:  []storage.SegmentRef{{BlobID: "s3", Size: 10, Checksum: []byte("e3")}},
		Coalesced: []CoalescedShardRef{{CoalescedID: "c1", ShardKey: "k/coalesced/c1", Size: 20, ETag: "etag-c1", Version: 1}},
	}
	coalesceWriteMeta(t, f, "b", "k", seed)

	cmd := CoalesceSegmentsCmd{
		Bucket: "b", Key: "k", CoalescedID: "c1",
		ShardKey: "k/coalesced/c1", Size: 20, ETag: "etag-c1",
		ConsumedSegmentIDs: []string{"s1", "s2"}, // already gone
	}
	raw, err := encodeCoalesceSegmentsCmd(cmd)
	require.NoError(t, err)
	require.NoError(t, f.applyCoalesceSegmentsFromCmd(raw))

	got := coalesceReadMeta(t, f, "b", "k")
	if len(got.Coalesced) != 1 {
		t.Fatalf("duplicate coalesced: %+v", got.Coalesced)
	}
}

func TestApplyCoalesceSegmentsRaceAppendPreserved(t *testing.T) {
	// Snapshot S = [s1, s2]; concurrent append added s3 before apply.
	f := newCoalesceTestFSM(t)
	seed := objectMeta{
		Key: "k", Size: 30, IsAppendable: true,
		Segments: []storage.SegmentRef{
			{BlobID: "s1", Size: 10, Checksum: []byte("e1")},
			{BlobID: "s2", Size: 10, Checksum: []byte("e2")},
			{BlobID: "s3", Size: 10, Checksum: []byte("e3")}, // raced in
		},
	}
	coalesceWriteMeta(t, f, "b", "k", seed)

	cmd := CoalesceSegmentsCmd{
		Bucket: "b", Key: "k", CoalescedID: "c1",
		ShardKey: "k/coalesced/c1", Size: 20, ETag: "etag",
		ConsumedSegmentIDs: []string{"s1", "s2"},
	}
	raw, err := encodeCoalesceSegmentsCmd(cmd)
	require.NoError(t, err)
	require.NoError(t, f.applyCoalesceSegmentsFromCmd(raw))

	got := coalesceReadMeta(t, f, "b", "k")
	if len(got.Segments) != 1 || got.Segments[0].BlobID != "s3" {
		t.Fatalf("raced segment lost: %+v", got.Segments)
	}
}
