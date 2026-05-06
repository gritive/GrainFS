package storage

import (
	"context"
	"fmt"
	"io"
)

func (o *Operations) PutObjectWithACL(ctx context.Context, bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	plan := o.planForCall()
	if plan.atomicACLPutter != nil {
		return plan.atomicACLPutter.PutObjectWithACL(bucket, key, r, contentType, acl)
	}
	if plan.aclSetter == nil {
		return nil, UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonNoAdapter}
	}

	obj, err := o.backend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := plan.aclSetter.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version id",
				UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed},
				err,
			)
		}
		if rollbackErr := rollbackPutObjectWithACL(plan, bucket, key, obj.VersionID); rollbackErr != nil {
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

func (o *Operations) PutObjectWithACLResult(ctx context.Context, bucket, key string, r io.Reader, contentType string, acl uint8) (*PutObjectResult, error) {
	previous, err := o.previousObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := o.PutObjectWithACL(ctx, bucket, key, r, contentType, acl)
	if err != nil {
		return nil, err
	}
	return &PutObjectResult{Object: obj, Previous: previous}, nil
}

func putObjectWithACLOnBackend(ctx context.Context, backend Backend, bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	if atomic, ok := backend.(AtomicACLPutter); ok {
		return atomic.PutObjectWithACL(bucket, key, r, contentType, acl)
	}
	setter, ok := backend.(ACLSetter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonNoAdapter}
	}

	obj, err := backend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := setter.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version id",
				UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed},
				err,
			)
		}
		deleter, ok := backend.(ObjectVersionDeleter)
		if !ok {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version deleter",
				UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed},
				err,
			)
		}
		if rollbackErr := deleter.DeleteObjectVersion(bucket, key, obj.VersionID); rollbackErr != nil {
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
	plan := o.planForCall()
	if plan.aclSetter == nil {
		return UnsupportedOperationError{Op: "SetObjectACL", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.aclSetter.SetObjectACL(bucket, key, acl)
}

func rollbackPutObjectWithACL(plan operationsPlan, bucket, key, versionID string) error {
	if plan.deleteObjectVersionForUndo == nil {
		return UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}
	}
	return plan.deleteObjectVersionForUndo.DeleteObjectVersion(bucket, key, versionID)
}
