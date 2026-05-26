package cluster

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type objectMetaPersistencePolicy uint8

const (
	objectMetaPersistenceLegacyOnly objectMetaPersistencePolicy = iota
	objectMetaPersistencePublishLatest
	objectMetaPersistenceMirrorVersion
)

type objectMetaPersistenceInput struct {
	Bucket    string
	Key       string
	VersionID string
	Meta      objectMeta
	Policy    objectMetaPersistencePolicy
}

func (f *FSM) persistObjectMetaUpdate(txn *badger.Txn, in objectMetaPersistenceInput) error {
	out, err := marshalObjectMeta(in.Meta)
	if err != nil {
		return fmt.Errorf("marshal updated objectMeta: %w", err)
	}

	legacyKey := f.keys.ObjectMetaKey(in.Bucket, in.Key)
	switch in.Policy {
	case objectMetaPersistenceLegacyOnly:
		return f.setValue(txn, legacyKey, out)
	case objectMetaPersistencePublishLatest:
		if in.VersionID != "" {
			if err := f.setValue(txn, f.keys.ObjectMetaKeyV(in.Bucket, in.Key, in.VersionID), out); err != nil {
				return err
			}
			if err := txn.Set(f.keys.LatestKey(in.Bucket, in.Key), []byte(in.VersionID)); err != nil {
				return err
			}
		}
		return f.setValue(txn, legacyKey, out)
	case objectMetaPersistenceMirrorVersion:
		if in.VersionID != "" {
			if err := f.setValue(txn, f.keys.ObjectMetaKeyV(in.Bucket, in.Key, in.VersionID), out); err != nil {
				return err
			}
		}
		return f.setValue(txn, legacyKey, out)
	default:
		return fmt.Errorf("object meta persistence: unknown policy %d", in.Policy)
	}
}
