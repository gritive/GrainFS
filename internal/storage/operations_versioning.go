package storage

import (
	"context"
	"io"
)

func (o *Operations) SetBucketVersioning(bucket, state string) error {
	plan := o.planForCall()
	if plan.bucketVersioner == nil {
		return UnsupportedOperationError{Op: "SetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.bucketVersioner.SetBucketVersioning(bucket, state)
}

func (o *Operations) GetBucketVersioning(bucket string) (string, error) {
	plan := o.planForCall()
	if plan.bucketVersioner == nil {
		return "", UnsupportedOperationError{Op: "GetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.bucketVersioner.GetBucketVersioning(bucket)
}

func (o *Operations) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	plan := o.planForCall()
	if plan.versionedGetter == nil {
		return nil, nil, UnsupportedOperationError{Op: "GetObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.versionedGetter.GetObjectVersion(bucket, key, versionID)
}

func (o *Operations) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	plan := o.planForCall()
	if plan.versionedHeader == nil {
		return nil, UnsupportedOperationError{Op: "HeadObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.versionedHeader.HeadObjectVersion(bucket, key, versionID)
}

func (o *Operations) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error) {
	plan := o.planForCall()
	if plan.objectVersionLister == nil {
		return nil, UnsupportedOperationError{Op: "ListObjectVersions", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.objectVersionLister.ListObjectVersions(bucket, prefix, maxKeys)
}

func (o *Operations) DeleteObjectVersion(bucket, key, versionID string) error {
	plan := o.planForCall()
	if plan.objectVersionDeleter == nil {
		return UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.objectVersionDeleter.DeleteObjectVersion(bucket, key, versionID)
}

func (o *Operations) DeleteObjectReturningMarker(ctx context.Context, bucket, key string) (string, error) {
	plan := o.planForCall()
	if plan.versionedSoftDeleter != nil {
		return plan.versionedSoftDeleter.DeleteObjectReturningMarker(bucket, key)
	}
	return "", o.backend.DeleteObject(ctx, bucket, key)
}
