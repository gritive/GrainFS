package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/badgermeta"
)

// newCoalesceTestFSM constructs an FSM backed by a temporary BadgerDB.
// Moved here from apply_coalesce_test.go after that file was deleted.
func newCoalesceTestFSM(t *testing.T) *FSM {
	t.Helper()
	db := newTestDB(t)
	return NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
}

// requirePersistObjectMetaForResolveTest directly writes raw objectMeta bytes
// to the given FSM key (bypassing marshaling abstraction) for test seeding.
// Moved here from object_meta_resolve_test.go after that file was deleted.
func requirePersistObjectMetaForResolveTest(t *testing.T, f *FSM, key []byte, meta objectMeta) {
	t.Helper()
	raw, err := marshalObjectMeta(meta)
	if err != nil {
		t.Fatalf("marshalObjectMeta: %v", err)
	}
	err = f.db.Update(func(txn MetadataTxn) error {
		return f.setValue(txn, key, raw)
	})
	if err != nil {
		t.Fatalf("persist object meta: %v", err)
	}
}

// requireSetLatestForResolveTest writes a latest-version pointer for the given
// (bucket, key) pair. Moved here from object_meta_resolve_test.go.
func requireSetLatestForResolveTest(t *testing.T, f *FSM, bucket, key, versionID string) {
	t.Helper()
	err := f.db.Update(func(txn MetadataTxn) error {
		return txn.Set(f.keys.LatestKey(bucket, key), []byte(versionID))
	})
	if err != nil {
		t.Fatalf("set latest: %v", err)
	}
}
