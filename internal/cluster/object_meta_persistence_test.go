package cluster

import (
	"errors"
	"fmt"
	"testing"
)

// persistPutObjectMetaUpdate is the canonical FSM write path for object metadata.
// It was extracted from the retired applyPutObjectMeta (data-plane raft-free Slice 2)
// and lives here as a test helper so tests can seed FSM state without going through
// the retired raft apply path.
func (f *FSM) persistPutObjectMetaUpdate(txn MetadataTxn, cmd PutObjectMetaCmd, meta objectMeta) error {
	out, err := marshalObjectMeta(meta)
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	if cmd.VersionID != "" {
		if err := f.setValue(txn, f.keys.ObjectMetaKeyV(cmd.Bucket, cmd.Key, cmd.VersionID), out); err != nil {
			return err
		}
		if cmd.PreserveLatest {
			return nil
		}
	}
	if cmd.IsDeleteMarker {
		if cmd.VersionID != "" {
			if err := txn.Set(f.keys.LatestKey(cmd.Bucket, cmd.Key), []byte(cmd.VersionID)); err != nil {
				return err
			}
		}
		if err := txn.Delete(f.keys.ObjectMetaKey(cmd.Bucket, cmd.Key)); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
		return nil
	}
	if err := f.setValue(txn, f.keys.ObjectMetaKey(cmd.Bucket, cmd.Key), out); err != nil {
		return err
	}
	if cmd.VersionID != "" {
		if err := txn.Set(f.keys.LatestKey(cmd.Bucket, cmd.Key), []byte(cmd.VersionID)); err != nil {
			return err
		}
	}
	return nil
}

func TestPersistPutObjectMetaUpdatePublishesVersionedObject(t *testing.T) {
	f := newCoalesceTestFSM(t)
	meta := objectMeta{Key: "k", Size: 12}

	err := f.db.Update(func(txn MetadataTxn) error {
		return f.persistPutObjectMetaUpdate(txn, PutObjectMetaCmd{
			Bucket:    "b",
			Key:       "k",
			VersionID: "v1",
		}, meta)
	})
	if err != nil {
		t.Fatalf("persistPutObjectMetaUpdate: %v", err)
	}

	legacy := readObjectMetaForPersistenceTest(t, f, f.keys.ObjectMetaKey("b", "k"))
	versioned := readObjectMetaForPersistenceTest(t, f, f.keys.ObjectMetaKeyV("b", "k", "v1"))
	if legacy.Size != 12 || versioned.Size != 12 {
		t.Fatalf("unexpected persisted meta: legacy=%+v versioned=%+v", legacy, versioned)
	}
	if got := readLatestForPersistenceTest(t, f, "b", "k"); got != "v1" {
		t.Fatalf("latest=%q want v1", got)
	}
}

func TestPersistPutObjectMetaUpdatePreserveLatestWritesOnlyVersion(t *testing.T) {
	f := newCoalesceTestFSM(t)
	previous := objectMeta{Key: "k", Size: 1}
	requirePersistObjectMetaForResolveTest(t, f, f.keys.ObjectMetaKey("b", "k"), previous)
	requireSetLatestForResolveTest(t, f, "b", "k", "v-current")

	err := f.db.Update(func(txn MetadataTxn) error {
		return f.persistPutObjectMetaUpdate(txn, PutObjectMetaCmd{
			Bucket:         "b",
			Key:            "k",
			VersionID:      "v-old",
			PreserveLatest: true,
		}, objectMeta{Key: "k", Size: 12})
	})
	if err != nil {
		t.Fatalf("persistPutObjectMetaUpdate: %v", err)
	}

	legacy := readObjectMetaForPersistenceTest(t, f, f.keys.ObjectMetaKey("b", "k"))
	versioned := readObjectMetaForPersistenceTest(t, f, f.keys.ObjectMetaKeyV("b", "k", "v-old"))
	if legacy.Size != 1 || versioned.Size != 12 {
		t.Fatalf("unexpected persisted meta: legacy=%+v versioned=%+v", legacy, versioned)
	}
	if got := readLatestForPersistenceTest(t, f, "b", "k"); got != "v-current" {
		t.Fatalf("latest=%q want v-current", got)
	}
}

func TestPersistPutObjectMetaUpdateDeleteMarkerPublishesLatestAndDeletesLegacy(t *testing.T) {
	f := newCoalesceTestFSM(t)
	requirePersistObjectMetaForResolveTest(t, f, f.keys.ObjectMetaKey("b", "k"), objectMeta{Key: "k", Size: 12})

	err := f.db.Update(func(txn MetadataTxn) error {
		return f.persistPutObjectMetaUpdate(txn, PutObjectMetaCmd{
			Bucket:         "b",
			Key:            "k",
			VersionID:      "del-v1",
			IsDeleteMarker: true,
		}, objectMeta{Key: "k", ETag: deleteMarkerETag})
	})
	if err != nil {
		t.Fatalf("persistPutObjectMetaUpdate: %v", err)
	}

	marker := readObjectMetaForPersistenceTest(t, f, f.keys.ObjectMetaKeyV("b", "k", "del-v1"))
	if marker.ETag != deleteMarkerETag {
		t.Fatalf("marker=%+v", marker)
	}
	if got := readLatestForPersistenceTest(t, f, "b", "k"); got != "del-v1" {
		t.Fatalf("latest=%q want del-v1", got)
	}
	if _, err := readObjectMetaMaybeForPersistenceTest(t, f, f.keys.ObjectMetaKey("b", "k")); !errors.Is(err, ErrMetaKeyNotFound) {
		t.Fatalf("legacy err=%v want ErrKeyNotFound", err)
	}
}

func readObjectMetaForPersistenceTest(t *testing.T, f *FSM, key []byte) objectMeta {
	t.Helper()
	meta, err := readObjectMetaMaybeForPersistenceTest(t, f, key)
	if err != nil {
		t.Fatalf("read object meta: %v", err)
	}
	return meta
}

func readObjectMetaMaybeForPersistenceTest(t *testing.T, f *FSM, key []byte) (objectMeta, error) {
	t.Helper()
	var meta objectMeta
	err := f.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(raw []byte) error {
			v, err := f.openValue(item.Key(), raw)
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(v)
			if err != nil {
				return err
			}
			meta = m
			return nil
		})
	})
	return meta, err
}

func readLatestForPersistenceTest(t *testing.T, f *FSM, bucket, key string) string {
	t.Helper()
	var latest string
	err := f.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(f.keys.LatestKey(bucket, key))
		if errors.Is(err, ErrMetaKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(raw []byte) error {
			latest = string(raw)
			return nil
		})
	})
	if err != nil {
		t.Fatalf("read latest: %v", err)
	}
	return latest
}
