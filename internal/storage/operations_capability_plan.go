package storage

import (
	"context"
	"fmt"
	"io"
)

type aclCapabilityPlan struct {
	atomicPutter AtomicACLPutter
	aclSetter    ACLSetter
	rollback     ObjectVersionDeleter
}

func buildACLCapabilityPlan(backend Backend) aclCapabilityPlan {
	var plan aclCapabilityPlan
	allowAtomicBehindCurrent := true

	for b := backend; b != nil; b = unwrapOperationBackend(b) {
		next := unwrapOperationBackend(b)
		if plan.atomicPutter == nil && allowAtomicBehindCurrent {
			if atomic, ok := b.(AtomicACLPutter); ok {
				plan.atomicPutter = atomic
			}
		}
		if plan.aclSetter == nil {
			if setter, ok := b.(ACLSetter); ok {
				plan.aclSetter = setter
			}
		}
		if plan.rollback == nil {
			if _, blocksWrites := b.(*RecoveryWriteGate); !blocksWrites {
				if deleter, ok := b.(ObjectVersionDeleter); ok {
					plan.rollback = deleter
				}
			}
		}
		if next != nil && plan.atomicPutter == nil {
			allowAtomicBehindCurrent = false
		}
	}
	return plan
}

func executePutObjectWithACL(
	ctx context.Context,
	backend Backend,
	bucket, key string,
	r io.Reader,
	contentType string,
	acl uint8,
) (*Object, error) {
	plan := buildACLCapabilityPlan(backend)
	if plan.atomicPutter != nil {
		return plan.atomicPutter.PutObjectWithACL(bucket, key, r, contentType, acl)
	}
	if plan.aclSetter == nil {
		return nil, UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonNoAdapter}
	}

	obj, err := backend.PutObject(ctx, bucket, key, r, contentType)
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

func rollbackPutObjectWithACL(plan aclCapabilityPlan, bucket, key, versionID string) error {
	if plan.rollback == nil {
		return UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}
	}
	return plan.rollback.DeleteObjectVersion(bucket, key, versionID)
}
