package storage

import (
	"context"
	"errors"
	"io"
)

var ErrRecoveryWriteDisabled = errors.New("recovered cluster is write-disabled until recover cluster verify --mark-writable")

type RecoveryWriteGate struct {
	Backend
	err error
}

type policyBackend interface {
	GetBucketPolicy(bucket string) ([]byte, error)
	SetBucketPolicy(bucket string, policyJSON []byte) error
	DeleteBucketPolicy(bucket string) error
}

type bucketVersioner interface {
	SetBucketVersioning(bucket, state string) error
	GetBucketVersioning(bucket string) (string, error)
}

type versionedGetter interface {
	GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error)
}

type versionedHeader interface {
	HeadObjectVersion(bucket, key, versionID string) (*Object, error)
}

type objectVersionLister interface {
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error)
}

func NewRecoveryWriteGate(inner Backend, err error) *RecoveryWriteGate {
	if err == nil {
		err = ErrRecoveryWriteDisabled
	}
	return &RecoveryWriteGate{Backend: inner, err: err}
}

// Mutating methods blocked here: bucket create/delete, object put/delete,
// multipart create/upload/complete/abort, copy, ACL/policy/versioning setters,
// truncate, versioned deletes, and snapshot restore helpers. Read/list/head
// methods delegate through Backend.
func (g *RecoveryWriteGate) CreateBucket(context.Context, string) error { return g.err }
func (g *RecoveryWriteGate) DeleteBucket(context.Context, string) error { return g.err }
func (g *RecoveryWriteGate) PutObject(context.Context, string, string, io.Reader, string) (*Object, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) DeleteObject(context.Context, string, string) error { return g.err }
func (g *RecoveryWriteGate) CreateMultipartUpload(context.Context, string, string, string) (*MultipartUpload, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) UploadPart(context.Context, string, string, string, int, io.Reader) (*Part, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) CompleteMultipartUpload(context.Context, string, string, string, []Part) (*Object, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) AbortMultipartUpload(context.Context, string, string, string) error {
	return g.err
}
func (g *RecoveryWriteGate) CopyObject(string, string, string, string) (*Object, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) SetObjectACL(string, string, uint8) error { return g.err }
func (g *RecoveryWriteGate) PutObjectWithACL(string, string, io.Reader, string, uint8) (*Object, error) {
	return nil, g.err
}
func (g *RecoveryWriteGate) Truncate(context.Context, string, string, int64) error { return g.err }

func (g *RecoveryWriteGate) GetBucketPolicy(bucket string) ([]byte, error) {
	pb, ok := g.Backend.(policyBackend)
	if !ok {
		return nil, ErrSnapshotNotSupported
	}
	return pb.GetBucketPolicy(bucket)
}

func (g *RecoveryWriteGate) SetBucketPolicy(string, []byte) error { return g.err }
func (g *RecoveryWriteGate) DeleteBucketPolicy(string) error      { return g.err }

func (g *RecoveryWriteGate) GetBucketVersioning(bucket string) (string, error) {
	v, ok := g.Backend.(bucketVersioner)
	if !ok {
		return "", ErrSnapshotNotSupported
	}
	return v.GetBucketVersioning(bucket)
}

func (g *RecoveryWriteGate) SetBucketVersioning(string, string) error { return g.err }

func (g *RecoveryWriteGate) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	v, ok := g.Backend.(versionedGetter)
	if !ok {
		return nil, nil, ErrSnapshotNotSupported
	}
	return v.GetObjectVersion(bucket, key, versionID)
}

func (g *RecoveryWriteGate) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	v, ok := g.Backend.(versionedHeader)
	if !ok {
		return nil, ErrSnapshotNotSupported
	}
	return v.HeadObjectVersion(bucket, key, versionID)
}

func (g *RecoveryWriteGate) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error) {
	v, ok := g.Backend.(objectVersionLister)
	if !ok {
		return nil, ErrSnapshotNotSupported
	}
	return v.ListObjectVersions(bucket, prefix, maxKeys)
}

func (g *RecoveryWriteGate) DeleteObjectVersion(string, string, string) error { return g.err }
func (g *RecoveryWriteGate) DeleteObjectReturningMarker(string, string) (string, error) {
	return "", g.err
}

func (g *RecoveryWriteGate) ListAllObjects() ([]SnapshotObject, error) {
	snap, ok := g.Backend.(Snapshotable)
	if !ok {
		return nil, ErrSnapshotNotSupported
	}
	return snap.ListAllObjects()
}

func (g *RecoveryWriteGate) RestoreObjects([]SnapshotObject) (int, []StaleBlob, error) {
	return 0, nil, g.err
}

func (g *RecoveryWriteGate) ListAllBuckets() ([]SnapshotBucket, error) {
	snap, ok := g.Backend.(BucketSnapshotable)
	if !ok {
		return nil, ErrSnapshotNotSupported
	}
	return snap.ListAllBuckets()
}

func (g *RecoveryWriteGate) RestoreBuckets([]SnapshotBucket) error { return g.err }
