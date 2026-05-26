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

type objectMetaForAppendUpdate struct {
	Found             bool
	Existing          *objectMeta
	AlreadyApplied    bool
	ExistingVersionID string
}

func (f *FSM) resolveObjectMetaForAppendUpdate(txn *badger.Txn, bucket, key, blobID string) (objectMetaForAppendUpdate, error) {
	existing, err := f.readLegacyObjectMetaForUpdate(txn, bucket, key)
	if err != nil {
		return objectMetaForAppendUpdate{}, err
	}
	if existing == nil {
		return objectMetaForAppendUpdate{}, nil
	}
	if appendObjectCommandAlreadyApplied(existing, blobID) {
		return objectMetaForAppendUpdate{
			Found:          true,
			Existing:       existing,
			AlreadyApplied: true,
		}, nil
	}

	versionID, err := f.readLatestObjectVersionForAppend(txn, bucket, key)
	if err != nil {
		return objectMetaForAppendUpdate{}, err
	}
	return objectMetaForAppendUpdate{
		Found:             true,
		Existing:          existing,
		ExistingVersionID: versionID,
	}, nil
}

func (f *FSM) readLegacyObjectMetaForUpdate(txn *badger.Txn, bucket, key string) (*objectMeta, error) {
	metaKey := f.keys.ObjectMetaKey(bucket, key)
	item, err := txn.Get(metaKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get existing objectMeta: %w", err)
	}
	var meta objectMeta
	if err := item.Value(func(raw []byte) error {
		v, err := f.openValue(item.Key(), raw)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(v)
		if err != nil {
			return fmt.Errorf("unmarshal existing objectMeta: %w", err)
		}
		meta = m
		return nil
	}); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (f *FSM) readLatestObjectVersionForAppend(txn *badger.Txn, bucket, key string) (string, error) {
	item, err := txn.Get(f.keys.LatestKey(bucket, key))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("get latest version: %w", err)
	}
	versionID := ""
	if err := item.Value(func(raw []byte) error {
		if string(raw) != deleteMarkerETag {
			versionID = string(raw)
		}
		return nil
	}); err != nil {
		return "", err
	}
	return versionID, nil
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
