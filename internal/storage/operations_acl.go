package storage

import (
	"context"
	"fmt"
	"io"
)

func (o *Operations) PutObjectWithACL(ctx context.Context, bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	if o.plan.atomicACLPutter != nil {
		return o.plan.atomicACLPutter.PutObjectWithACL(bucket, key, r, contentType, acl)
	}
	if o.plan.aclSetter == nil {
		return nil, UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonNoAdapter}
	}

	obj, err := o.backend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := o.plan.aclSetter.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version id",
				UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed},
				err,
			)
		}
		if rollbackErr := o.rollbackPutObjectWithACL(bucket, key, obj.VersionID); rollbackErr != nil {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: %v",
				UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed},
				err,
				rollbackErr,
			)
		}
		return nil, err
	}
	return obj, nil
}

func (o *Operations) SetObjectACL(bucket, key string, acl uint8) error {
	if o.plan.aclSetter == nil {
		return UnsupportedOperationError{Op: "SetObjectACL", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.aclSetter.SetObjectACL(bucket, key, acl)
}

func (o *Operations) rollbackPutObjectWithACL(bucket, key, versionID string) error {
	if o.plan.deleteObjectVersionForUndo == nil {
		return UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}
	}
	return o.plan.deleteObjectVersionForUndo.DeleteObjectVersion(bucket, key, versionID)
}
