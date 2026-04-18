package wal

import (
	"io"
	"log/slog"

	"github.com/gritive/GrainFS/internal/storage"
)

// Backend wraps a storage.Backend and appends WAL entries for mutations.
// WAL writes are advisory: failures are logged but do not block S3 operations.
type Backend struct {
	storage.Backend
	w *WAL
}

// NewBackend creates a WALBackend wrapping inner.
func NewBackend(inner storage.Backend, w *WAL) *Backend {
	return &Backend{Backend: inner, w: w}
}

// WALOffset returns the sequence number of the last WAL entry written.
// Used by snapshot.Manager to record the WAL anchor point at snapshot time.
func (b *Backend) WALOffset() uint64 { return b.w.CurrentSeq() }

// Unwrap returns the inner backend for interface chaining (ECPolicySetter, etc).
func (b *Backend) Unwrap() storage.Backend { return b.Backend }

func (b *Backend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	obj, err := b.Backend.PutObject(bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	b.w.AppendAsync(Entry{Op: OpPut, Bucket: bucket, Key: key, ETag: obj.ETag, ContentType: obj.ContentType, Size: obj.Size})
	return obj, nil
}

func (b *Backend) DeleteObject(bucket, key string) error {
	if err := b.Backend.DeleteObject(bucket, key); err != nil {
		return err
	}
	b.w.AppendAsync(Entry{Op: OpDelete, Bucket: bucket, Key: key})
	return nil
}

func (b *Backend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	obj, err := b.Backend.CompleteMultipartUpload(bucket, key, uploadID, parts)
	if err != nil {
		return nil, err
	}
	b.w.AppendAsync(Entry{Op: OpPut, Bucket: bucket, Key: key, ETag: obj.ETag, ContentType: obj.ContentType, Size: obj.Size})
	return obj, nil
}

// ListAllObjects implements storage.Snapshotable by delegating to inner.
func (b *Backend) ListAllObjects() ([]storage.SnapshotObject, error) {
	snap, ok := b.Backend.(storage.Snapshotable)
	if !ok {
		return nil, storage.ErrSnapshotNotSupported
	}
	return snap.ListAllObjects()
}

// RestoreObjects implements storage.Snapshotable by delegating to inner.
// Does NOT write WAL entries for restore operations.
func (b *Backend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	snap, ok := b.Backend.(storage.Snapshotable)
	if !ok {
		return 0, nil, storage.ErrSnapshotNotSupported
	}
	restored, stale, err := snap.RestoreObjects(objects)
	if err != nil {
		return restored, stale, err
	}
	// Flush WAL after restore so the anchor point is clean
	if ferr := b.w.Flush(); ferr != nil {
		slog.Warn("wal: flush after restore failed", "err", ferr)
	}
	return restored, stale, nil
}

// ListAllBuckets implements storage.BucketSnapshotable by delegating to inner.
func (b *Backend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	bs, ok := b.Backend.(storage.BucketSnapshotable)
	if !ok {
		return nil, nil
	}
	return bs.ListAllBuckets()
}

// RestoreBuckets implements storage.BucketSnapshotable by delegating to inner.
func (b *Backend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	bs, ok := b.Backend.(storage.BucketSnapshotable)
	if !ok {
		return nil
	}
	return bs.RestoreBuckets(buckets)
}
