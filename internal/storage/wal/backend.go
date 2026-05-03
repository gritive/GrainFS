package wal

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

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

// Unwrap returns the inner backend for interface chaining.
func (b *Backend) Unwrap() storage.Backend { return b.Backend }

func (b *Backend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	obj, err := b.Backend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	b.w.AppendAsync(Entry{
		Op:          OpPut,
		Bucket:      bucket,
		Key:         key,
		ETag:        obj.ETag,
		ContentType: obj.ContentType,
		Size:        obj.Size,
		VersionID:   obj.VersionID,
	})
	return obj, nil
}

// PutObjectAsync delegates to the inner backend's write-back path and appends
// the WAL entry inside the commitFn so PITR records only committed objects.
func (b *Backend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	type asyncPutter interface {
		PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error)
	}
	ap, ok := b.Backend.(asyncPutter)
	if !ok {
		obj, err := b.PutObject(ctx, bucket, key, r, contentType)
		return obj, func() error { return nil }, err
	}
	obj, innerCommit, err := ap.PutObjectAsync(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, nil, err
	}
	commitFn := func() error {
		if err := innerCommit(); err != nil {
			return err
		}
		b.w.AppendAsync(Entry{
			Op:          OpPut,
			Bucket:      bucket,
			Key:         key,
			ETag:        obj.ETag,
			ContentType: obj.ContentType,
			Size:        obj.Size,
			VersionID:   obj.VersionID,
		})
		return nil
	}
	return obj, commitFn, nil
}

// WriteAt is a pass-through for pwrite-based partial writes on internal
// buckets (NFS4, VFS). No WAL entry is written: internal buckets are ephemeral
// and not subject to PITR replay.
func (b *Backend) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	wa, ok := b.Backend.(storage.PartialIO)
	if !ok {
		return nil, fmt.Errorf("wal: inner backend does not support WriteAt")
	}
	return wa.WriteAt(ctx, bucket, key, offset, data)
}

// ReadAt is a pass-through for pread-based partial reads on internal buckets.
// No WAL entry: reads are not mutations.
func (b *Backend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	ra, ok := b.Backend.(storage.PartialIO)
	if !ok {
		return 0, fmt.Errorf("wal: inner backend does not support ReadAt")
	}
	return ra.ReadAt(ctx, bucket, key, offset, buf)
}

// Truncate is a pass-through for internal bucket size changes used by NFS
// SETATTR. No WAL entry is written: internal buckets are ephemeral and not
// subject to PITR replay.
func (b *Backend) Truncate(ctx context.Context, bucket, key string, size int64) error {
	tr, ok := b.Backend.(storage.Truncatable)
	if !ok {
		return fmt.Errorf("wal: inner backend does not support Truncate")
	}
	return tr.Truncate(ctx, bucket, key, size)
}

func (b *Backend) DeleteObject(ctx context.Context, bucket, key string) error {
	if sd, ok := b.Backend.(interface {
		DeleteObjectReturningMarker(bucket, key string) (string, error)
	}); ok {
		markerID, err := sd.DeleteObjectReturningMarker(bucket, key)
		if err != nil {
			return err
		}
		b.w.AppendAsync(Entry{Op: OpDelete, Bucket: bucket, Key: key, VersionID: markerID})
		return nil
	}
	if err := b.Backend.DeleteObject(ctx, bucket, key); err != nil {
		return err
	}
	// VersionID is unknown at this layer — backends that model deletes as
	// tombstone versions (DistributedBackend) allocate the ID internally and
	// don't expose it through Backend.DeleteObject. Replay treats an empty
	// VersionID as a "latest-pointer delete," which is correct for either
	// tombstone or hard-delete semantics on read-paths that walk versions.
	b.w.AppendAsync(Entry{Op: OpDelete, Bucket: bucket, Key: key})
	return nil
}

func (b *Backend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	sd, ok := b.Backend.(interface {
		DeleteObjectReturningMarker(bucket, key string) (string, error)
	})
	if !ok {
		if err := b.DeleteObject(context.Background(), bucket, key); err != nil {
			return "", err
		}
		return "", nil
	}
	markerID, err := sd.DeleteObjectReturningMarker(bucket, key)
	if err != nil {
		return "", err
	}
	b.w.AppendAsync(Entry{Op: OpDelete, Bucket: bucket, Key: key, VersionID: markerID})
	return markerID, nil
}

func (b *Backend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	obj, err := b.Backend.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
	if err != nil {
		return nil, err
	}
	b.w.AppendAsync(Entry{
		Op:          OpPut,
		Bucket:      bucket,
		Key:         key,
		ETag:        obj.ETag,
		ContentType: obj.ContentType,
		Size:        obj.Size,
		VersionID:   obj.VersionID,
	})
	return obj, nil
}

// DeleteObjectVersion is a pass-through that records a WAL entry for
// version-specific hard deletes (lifecycle / scrubber paths). Without this
// hook the server's unwrap chain reaches the inner backend directly and the
// deletion would not appear in the WAL stream used for PITR replay.
func (b *Backend) DeleteObjectVersion(bucket, key, versionID string) error {
	type versionDeleter interface {
		DeleteObjectVersion(bucket, key, versionID string) error
	}
	vd, ok := b.Backend.(versionDeleter)
	if !ok {
		return fmt.Errorf("wal: inner backend does not support DeleteObjectVersion")
	}
	if err := vd.DeleteObjectVersion(bucket, key, versionID); err != nil {
		return err
	}
	b.w.AppendAsync(Entry{
		Op:        OpDeleteVersion,
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
	return nil
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
		log.Warn().Err(ferr).Msg("wal: flush after restore failed")
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
