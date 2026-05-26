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

func (f *FSM) persistPutObjectMetaUpdate(txn *badger.Txn, cmd PutObjectMetaCmd, meta objectMeta) error {
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
		if err := txn.Delete(f.keys.ObjectMetaKey(cmd.Bucket, cmd.Key)); err != nil && err != badger.ErrKeyNotFound {
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
