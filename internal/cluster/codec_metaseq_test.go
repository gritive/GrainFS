package cluster

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
)

// TestPutObjectMetaCmd_MetaSeqRoundTrip verifies MetaSeq survives the
// PutObjectMetaCmd FlatBuffers encode/decode.
func TestPutObjectMetaCmd_MetaSeqRoundTrip(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket:    "b",
		Key:       "k",
		Size:      10,
		ETag:      "e",
		VersionID: "v1",
		MetaSeq:   7,
	}
	blob, err := encodePutObjectMetaCmd(cmd)
	require.NoError(t, err)
	got, err := decodePutObjectMetaCmd(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.MetaSeq)
}

// TestPutObjectMetaCmd_MetaSeqDefaultZero verifies a freshly-encoded blob with
// MetaSeq unset decodes to 0 — the FlatBuffers trailing-field default that
// guarantees legacy (absent-field) blobs read back as 0 (behavior-neutral).
func TestPutObjectMetaCmd_MetaSeqDefaultZero(t *testing.T) {
	blob, err := encodePutObjectMetaCmd(PutObjectMetaCmd{Bucket: "b", Key: "k"})
	require.NoError(t, err)
	got, err := decodePutObjectMetaCmd(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got.MetaSeq)
}

// TestObjectMeta_MetaSeqRoundTrip verifies MetaSeq survives the objectMeta
// FlatBuffers marshal/unmarshal (the FSM-stored record).
func TestObjectMeta_MetaSeqRoundTrip(t *testing.T) {
	m := objectMeta{Key: "k", Size: 10, ETag: "e", MetaSeq: 7}
	blob, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.MetaSeq)
}

// TestObjectMeta_MetaSeqDefaultZero verifies an objectMeta blob with MetaSeq
// unset decodes to 0 (trailing-field default; legacy back-compat).
func TestObjectMeta_MetaSeqDefaultZero(t *testing.T) {
	blob, err := marshalObjectMeta(objectMeta{Key: "k"})
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(blob)
	require.NoError(t, err)
	require.Equal(t, uint64(0), got.MetaSeq)
}

// TestFSM_PutObjectMetaSeqRoundTrip applies a PutObjectMetaCmd carrying
// MetaSeq:1 and reads the stored objectMeta back to confirm MetaSeq
// round-trips through persistPutObjectMetaUpdate (the live write path via
// writeQuorumMeta). CmdPutObjectMeta FSM apply is retired in Slice 2.
func TestFSM_PutObjectMetaSeqRoundTrip(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	cmd := PutObjectMetaCmd{
		Bucket:    "b",
		Key:       "k",
		Size:      1,
		ETag:      "e",
		VersionID: "v1",
		MetaSeq:   1,
	}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
	}))

	var stored objectMeta
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(fsm.keys.ObjectMetaKey("b", "k"))
		if err != nil {
			return err
		}
		plain, err := fsm.itemValueCopy(item)
		if err != nil {
			return err
		}
		stored, err = unmarshalObjectMeta(plain)
		return err
	}))
	require.Equal(t, uint64(1), stored.MetaSeq)
}
