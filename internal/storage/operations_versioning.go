package storage

import (
	"context"
	"io"
)

func (o *Operations) SetBucketVersioning(bucket, state string) error {
	if o.plan.bucketVersioner == nil {
		return UnsupportedOperationError{Op: "SetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.bucketVersioner.SetBucketVersioning(bucket, state)
}

func (o *Operations) GetBucketVersioning(bucket string) (string, error) {
	if o.plan.bucketVersioner == nil {
		return "", UnsupportedOperationError{Op: "GetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.bucketVersioner.GetBucketVersioning(bucket)
}

func (o *Operations) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	if o.plan.versionedGetter == nil {
		return nil, nil, UnsupportedOperationError{Op: "GetObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.versionedGetter.GetObjectVersion(bucket, key, versionID)
}

func (o *Operations) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	if o.plan.versionedHeader == nil {
		return nil, UnsupportedOperationError{Op: "HeadObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.versionedHeader.HeadObjectVersion(bucket, key, versionID)
}

func (o *Operations) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error) {
	if o.plan.objectVersionLister == nil {
		return nil, UnsupportedOperationError{Op: "ListObjectVersions", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.objectVersionLister.ListObjectVersions(bucket, prefix, maxKeys)
}

func (o *Operations) DeleteObjectVersion(bucket, key, versionID string) error {
	if o.plan.objectVersionDeleter == nil {
		return UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.objectVersionDeleter.DeleteObjectVersion(bucket, key, versionID)
}

func (o *Operations) DeleteObjectReturningMarker(ctx context.Context, bucket, key string) (string, error) {
	if o.plan.versionedSoftDeleter != nil {
		return o.plan.versionedSoftDeleter.DeleteObjectReturningMarker(bucket, key)
	}
	return "", o.backend.DeleteObject(ctx, bucket, key)
}
