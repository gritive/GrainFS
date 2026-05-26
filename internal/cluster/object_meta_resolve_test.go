package cluster

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestResolveObjectMetaForCoalesceUsesLegacyWhenNoLatestPointer(t *testing.T) {
	f := newCoalesceTestFSM(t)
	seed := objectMeta{Key: "k", Size: 12}
	requirePersistObjectMetaForResolveTest(t, f, f.keys.ObjectMetaKey("b", "k"), seed)

	var resolved objectMetaForCoalesceUpdate
	err := f.db.View(func(txn *badger.Txn) error {
		var err error
		resolved, err = f.resolveObjectMetaForCoalesceUpdate(txn, "b", "k")
		return err
	})
	if err != nil {
		t.Fatalf("resolveObjectMetaForCoalesceUpdate: %v", err)
	}
	if !resolved.Found || resolved.VersionID != "" || string(resolved.MetaKey) != string(f.keys.ObjectMetaKey("b", "k")) {
		t.Fatalf("resolved = %+v", resolved)
	}
	if resolved.Meta.Size != 12 {
		t.Fatalf("resolved meta = %+v", resolved.Meta)
	}
}

func TestResolveObjectMetaForCoalesceUsesLatestVersion(t *testing.T) {
	f := newCoalesceTestFSM(t)
	legacy := objectMeta{Key: "k", Size: 12}
	versioned := objectMeta{Key: "k", Size: 34}
	requirePersistObjectMetaForResolveTest(t, f, f.keys.ObjectMetaKey("b", "k"), legacy)
	requirePersistObjectMetaForResolveTest(t, f, f.keys.ObjectMetaKeyV("b", "k", "v1"), versioned)
	requireSetLatestForResolveTest(t, f, "b", "k", "v1")

	var resolved objectMetaForCoalesceUpdate
	err := f.db.View(func(txn *badger.Txn) error {
		var err error
		resolved, err = f.resolveObjectMetaForCoalesceUpdate(txn, "b", "k")
		return err
	})
	if err != nil {
		t.Fatalf("resolveObjectMetaForCoalesceUpdate: %v", err)
	}
	if !resolved.Found || resolved.VersionID != "v1" || string(resolved.MetaKey) != string(f.keys.ObjectMetaKeyV("b", "k", "v1")) {
		t.Fatalf("resolved = %+v", resolved)
	}
	if resolved.Meta.Size != 34 {
		t.Fatalf("resolved meta = %+v", resolved.Meta)
	}
}

func TestResolveObjectMetaForCoalesceMissingReturnsNotFound(t *testing.T) {
	f := newCoalesceTestFSM(t)

	var resolved objectMetaForCoalesceUpdate
	err := f.db.View(func(txn *badger.Txn) error {
		var err error
		resolved, err = f.resolveObjectMetaForCoalesceUpdate(txn, "b", "missing")
		return err
	})
	if err != nil {
		t.Fatalf("resolveObjectMetaForCoalesceUpdate: %v", err)
	}
	if resolved.Found {
		t.Fatalf("resolved = %+v, want not found", resolved)
	}
}

func requirePersistObjectMetaForResolveTest(t *testing.T, f *FSM, key []byte, meta objectMeta) {
	t.Helper()
	raw, err := marshalObjectMeta(meta)
	if err != nil {
		t.Fatalf("marshalObjectMeta: %v", err)
	}
	err = f.db.Update(func(txn *badger.Txn) error {
		return f.setValue(txn, key, raw)
	})
	if err != nil {
		t.Fatalf("persist object meta: %v", err)
	}
}

func requireSetLatestForResolveTest(t *testing.T, f *FSM, bucket, key, versionID string) {
	t.Helper()
	err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(f.keys.LatestKey(bucket, key), []byte(versionID))
	})
	if err != nil {
		t.Fatalf("set latest: %v", err)
	}
}
