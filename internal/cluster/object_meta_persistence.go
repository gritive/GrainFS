package cluster

import (
	"fmt"
)

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
