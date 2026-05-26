package cluster

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type objectMetaForCoalesceUpdate struct {
	Found     bool
	MetaKey   []byte
	VersionID string
	Meta      objectMeta
}

func (f *FSM) resolveObjectMetaForCoalesceUpdate(txn *badger.Txn, bucket, key string) (objectMetaForCoalesceUpdate, error) {
	legacyKey := f.keys.ObjectMetaKey(bucket, key)
	metaKey := legacyKey
	versionID := ""

	if latItem, err := txn.Get(f.keys.LatestKey(bucket, key)); err == nil {
		if err := latItem.Value(func(v []byte) error {
			versionID = string(v)
			return nil
		}); err != nil {
			return objectMetaForCoalesceUpdate{}, err
		}
		if versionID != "" {
			metaKey = f.keys.ObjectMetaKeyV(bucket, key, versionID)
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return objectMetaForCoalesceUpdate{}, fmt.Errorf("get latest pointer: %w", err)
	}

	item, err := txn.Get(metaKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return objectMetaForCoalesceUpdate{}, nil
		}
		return objectMetaForCoalesceUpdate{}, fmt.Errorf("get objectMeta: %w", err)
	}
	var meta objectMeta
	if err := item.Value(func(raw []byte) error {
		v, err := f.openValue(item.Key(), raw)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(v)
		if err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
		meta = m
		return nil
	}); err != nil {
		return objectMetaForCoalesceUpdate{}, err
	}
	return objectMetaForCoalesceUpdate{
		Found:     true,
		MetaKey:   metaKey,
		VersionID: versionID,
		Meta:      meta,
	}, nil
}
