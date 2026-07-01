// TEST-FIXTURE ONLY — not part of the production storage path.
//
// LocalBackend has zero production callers (audited 2026-05-28). It is a unit-test
// fixture used by 60+ external *_test.go files and many in-package tests. The
// production storage path is ClusterCoordinator → DistributedBackend (see
// boot_phases_storage_runtime.go). Do not add a non-test caller for any symbol
// declared in this file or in its companions (local.go, multipart.go, append.go,
// encrypted_badger.go) without revisiting ADR-0015.
//
// See: docs/adr/0015-localbackend-test-fixture-only.md

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/uuidutil"
)

// MaxAppendSegments caps appendable segments per object.
// var (not const) to allow test-time override. AWS S3 Express AppendObject 한계 = 10000.
var MaxAppendSegments = 10000

var (
	ErrAppendOffsetMismatch = errors.New("append: write offset does not match object size")
	ErrAppendCapExceeded    = errors.New("append: segment cap reached")
	ErrAppendNotSupported   = errors.New("append: object is not appendable")
	ErrAppendObjectTooLarge = errors.New("append: object total size cap reached")
)

// AppendObjecter is the optional interface for S3 Express AppendObject support.
// Server-side HTTP handler probes via type assertion; backends without support
// get a clean 501 NotImplemented. Mirrors existing optional-interface pattern
// (ACLSetter, RequestPutter, UserMetadataPutter).
type AppendObjecter interface {
	AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error)
}

type appendBase struct {
	object     *Object
	summary    appendSummary
	hasSummary bool
}

// AppendObject appends data to an existing appendable object, or creates a new
// appendable object when expectedOffset == 0 and the object does not exist.
//
// Returns ErrAppendOffsetMismatch if expectedOffset != current object size,
// ErrAppendCapExceeded if the object already has MaxAppendSegments segments.
func (b *LocalBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error) {
	base, err := b.readAppendBase(ctx, bucket, key)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, fmt.Errorf("head append base: %w", err)
	}
	if base.object == nil {
		if expectedOffset != 0 {
			return nil, ErrAppendOffsetMismatch
		}
		return b.appendNew(ctx, bucket, key, r)
	}
	existing := base.object
	if existing.Size != expectedOffset {
		return nil, ErrAppendOffsetMismatch
	}
	segmentCount := len(existing.Segments)
	if base.hasSummary {
		segmentCount = appendSummaryLogicalAppendCount(base.summary)
	}
	if segmentCount >= MaxAppendSegments {
		return nil, ErrAppendCapExceeded
	}
	return b.appendExisting(ctx, bucket, key, base, r)
}

func (b *LocalBackend) appendNew(ctx context.Context, bucket, key string, r io.Reader) (*Object, error) {
	seg, err := b.WriteSegmentBlob(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	// TODO(Task 3.1): replace segment-checksum-as-MD5-proxy with the real
	// AppendObject call payload MD5 captured via TeeReader at the API boundary.
	// Stopgap mirrors the cluster path (internal/cluster/apply.go).
	state, count, err := appendETagStateAppend(nil, 0, seg.Checksum)
	if err != nil {
		return nil, err
	}
	etag, err := compositeETagFromState(state, count)
	if err != nil {
		return nil, err
	}
	obj := &Object{
		Key:          key,
		Size:         seg.Size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Segments:     []SegmentRef{seg},
		IsAppendable: true,
	}
	summary := appendSummary{Size: obj.Size, SegmentCount: 1, ETagPartCount: count, ETagDigestState: state}
	if err := b.putAppendSideRecordObject(ctx, bucket, key, obj, summary, []SegmentRef{seg}); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return obj, nil
}

func (b *LocalBackend) appendExisting(ctx context.Context, bucket, key string, base appendBase, r io.Reader) (*Object, error) {
	existing := base.object
	existing, err := b.ensureAppendableBase(ctx, bucket, key, existing)
	if err != nil {
		return nil, err
	}
	seg, err := b.WriteSegmentBlob(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	segs := append(existing.Segments, seg)
	obj := *existing
	obj.Segments = segs
	obj.Size = existing.Size + seg.Size
	obj.IsAppendable = true
	obj.LastModified = time.Now().Unix()

	var persistErr error
	if base.hasSummary || len(obj.Coalesced) == 0 {
		state, count, err := appendETagStateForAppend(base.summary, base.hasSummary, existing, seg.Checksum)
		if err != nil {
			return nil, err
		}
		obj.AppendCallMD5s = nil
		obj.ETag, err = compositeETagFromState(state, count)
		if err != nil {
			return nil, err
		}
		summary := appendSummary{Size: obj.Size, SegmentCount: len(segs), ETagPartCount: count, ETagDigestState: state}
		if base.hasSummary {
			summary.Size = base.summary.Size + seg.Size
			summary.SegmentCount = base.summary.SegmentCount + 1
			summary.CompactedPrefixCount = base.summary.CompactedPrefixCount
			persistErr = b.putAppendSideRecordAppend(ctx, bucket, key, &obj, summary, seg)
		} else {
			persistErr = b.putAppendSideRecordObject(ctx, bucket, key, &obj, summary, segs)
		}
	} else if len(existing.Segments) > 0 || len(existing.Coalesced) > 0 {
		// TODO(Task 3.1): replace segment-checksum-as-MD5-proxy with the real
		// AppendObject call payload MD5 captured via TeeReader at the API boundary.
		// Stopgap mirrors the cluster path (internal/cluster/apply.go).
		callMD5s := appendCallMD5History(existing)
		callMD5s = append(callMD5s, append([]byte(nil), seg.Checksum...))
		obj.AppendCallMD5s = callMD5s
		obj.ETag = CompositeETag(callMD5s)
		persistErr = b.putObjectRecordAppend(ctx, bucket, key, &obj, []string{ParseLocator(seg.BlobID).String()})
	} else {
		// TODO(Task 3.1): replace segment-checksum-as-MD5-proxy with the real
		// AppendObject call payload MD5 captured via TeeReader at the API boundary.
		// Stopgap mirrors the cluster path (internal/cluster/apply.go).
		callMD5s := appendCallMD5History(existing)
		callMD5s = append(callMD5s, append([]byte(nil), seg.Checksum...))
		obj.AppendCallMD5s = callMD5s
		obj.ETag = CompositeETag(callMD5s)
		persistErr = b.PutObjectRecord(ctx, bucket, key, &obj)
	}
	if persistErr != nil {
		return nil, fmt.Errorf("persist: %w", persistErr)
	}
	return &obj, nil
}

func (b *LocalBackend) readAppendBase(ctx context.Context, bucket, key string) (appendBase, error) {
	_ = ctx
	var base appendBase
	err := b.db.View(func(txn *badger.Txn) error {
		raw, err := b.readObjectInTxn(txn, b.objectMetaKey(bucket, key))
		if err == nil {
			if raw.IsAppendable && raw.Size > 0 && len(raw.Segments) == 0 {
				summary, hasSummary, err := b.readAppendSummaryForObjectInTxn(txn, bucket, key, raw)
				if err != nil {
					return err
				}
				if hasSummary && summary.SegmentCount > 0 && summary.ETagPartCount == 0 && len(summary.ETagDigestState) == 0 {
					if err := b.loadAppendSideSegmentsInTxn(txn, bucket, key, raw); err != nil {
						return err
					}
				}
				base.summary = summary
				base.hasSummary = hasSummary
			}
			base.object = raw
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		if _, berr := txn.Get(b.bucketKey(bucket)); berr == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		} else if berr != nil {
			return berr
		}
		return ErrObjectNotFound
	})
	return base, err
}

func (b *LocalBackend) ensureAppendableBase(ctx context.Context, bucket, key string, existing *Object) (*Object, error) {
	if existing.IsAppendable {
		return existing, nil
	}
	if len(existing.Segments) > 0 || len(existing.Coalesced) > 0 || existing.Size == 0 {
		obj := *existing
		obj.IsAppendable = true
		return &obj, nil
	}

	rc, _, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("read base object: %w", err)
	}
	defer rc.Close()
	seg, err := b.WriteSegmentBlob(bucket, key, rc)
	if err != nil {
		return nil, fmt.Errorf("write base segment: %w", err)
	}
	obj := *existing
	obj.Segments = []SegmentRef{seg}
	obj.IsAppendable = true
	return &obj, nil
}

// WriteSegmentBlob writes one segment blob to disk under
// objects/<bucket>/<key>_segments/<blobID>. Returns SegmentRef with
// BlobID (UUIDv7) + Size + xxhash3-128 Checksum of the plaintext bytes.
// Exported so cluster.DistributedBackend.AppendObject can call directly.
//
// AAD ISOLATION: the encryption AAD includes the segment's unique
// blob_id (via segmentFileAADFields(bucket, key, blobID)).
// Because blobID is a fresh UUIDv7 per segment, segments belonging to the
// same object cannot be successfully decrypted under each other's AAD,
// even if their plaintext or ciphertext happens to be identical. The
// regression test TestEncryptedSegment_PerSegmentAADIsolation locks in
// this contract — do not collapse the domain to a per-object value.
func (b *LocalBackend) WriteSegmentBlob(bucket, key string, r io.Reader) (SegmentRef, error) {
	blobID := uuidutil.MustNewV7()
	path := b.segmentPath(bucket, key, blobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return SegmentRef{}, err
	}
	checksumHasher := NewChecksumHasher()
	var (
		size int64
		err  error
	)
	if b.segEnc != nil {
		// Encrypted writer tees plaintext through checksumHasher.
		size, err = writeEncryptedObjectFile(path, b.segEnc, segmentFileAADFields(bucket, key, blobID), r, checksumHasher)
	} else {
		var f *os.File
		f, err = os.Create(path)
		if err == nil {
			tr := io.TeeReader(r, checksumHasher)
			size, err = io.Copy(f, tr)
			cerr := f.Close()
			if err == nil {
				err = cerr
			}
		}
	}
	if err != nil {
		os.Remove(path)
		return SegmentRef{}, err
	}
	return SegmentRef{
		BlobID:   blobID,
		Size:     size,
		Checksum: checksumHasher.Sum(),
	}, nil
}

func (b *LocalBackend) segmentPath(bucket, key, blobID string) string {
	return filepath.Join(b.objectPath(bucket, key)+"_segments", blobID)
}

func appendETagStateForAppend(summary appendSummary, hasSummary bool, existing *Object, digest []byte) ([]byte, int, error) {
	if hasSummary && len(summary.ETagDigestState) > 0 {
		return appendETagStateAppend(summary.ETagDigestState, summary.ETagPartCount, digest)
	}
	state, count, err := appendETagStateFromDigests(appendCallMD5History(existing))
	if err != nil {
		return nil, 0, err
	}
	return appendETagStateAppend(state, count, digest)
}

func appendCallMD5History(obj *Object) [][]byte {
	if len(obj.AppendCallMD5s) > 0 {
		callMD5s := make([][]byte, 0, len(obj.AppendCallMD5s))
		for _, digest := range obj.AppendCallMD5s {
			callMD5s = append(callMD5s, append([]byte(nil), digest...))
		}
		return callMD5s
	}
	callMD5s := make([][]byte, 0, len(obj.Segments))
	for _, seg := range obj.Segments {
		callMD5s = append(callMD5s, append([]byte(nil), seg.Checksum...))
	}
	return callMD5s
}

// PutObjectRecord persists Object into the backend's BadgerDB. Self-managed txn.
func (b *LocalBackend) PutObjectRecord(ctx context.Context, bucket, key string, obj *Object) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return b.PutObjectRecordInTxn(txn, bucket, key, obj)
	})
}

func (b *LocalBackend) putObjectRecordAppend(ctx context.Context, bucket, key string, obj *Object, newChunkLocators []string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return b.putObjectRecordAppendInTxn(txn, bucket, key, obj, newChunkLocators)
	})
}

func (b *LocalBackend) putObjectRecordAppendInTxn(txn *badger.Txn, bucket, key string, obj *Object, newChunkLocators []string) error {
	mk := b.objectMetaKey(bucket, key)
	if _, err := txn.Get(mk); errors.Is(err, badger.ErrKeyNotFound) {
		newChunkLocators = obj.ChunkLocators()
	} else if err != nil {
		return err
	}
	data, err := marshalObject(obj)
	if err != nil {
		return err
	}
	if err := setBadgerValue(txn, mk, data); err != nil {
		return err
	}
	store := NewChunkRefStore(txn)
	m := chunkref.ObjectVersionID(bucket, key, obj.VersionID)
	for _, c := range newChunkLocators {
		if err := store.AddRef(m, chunkref.ChunkID(c)); err != nil {
			return err
		}
	}
	return nil
}

// PutObjectRecordInTxn persists Object within a caller-provided txn and
// atomically updates chunk refcounts. Any prior record at the same meta key is
// read first; its chunk refs are removed before the new refs are added, so an
// overwrite never leaves stale refcounts.
func (b *LocalBackend) PutObjectRecordInTxn(txn *badger.Txn, bucket, key string, obj *Object) error {
	mk := b.objectMetaKey(bucket, key)
	store := NewChunkRefStore(txn)
	m := chunkref.ObjectVersionID(bucket, key, obj.VersionID)

	// Remove stale chunk refs from the prior record (if any).
	if prev, err := b.readObjectInTxn(txn, mk); err == nil && prev != nil {
		prevM := chunkref.ObjectVersionID(bucket, key, prev.VersionID)
		now := time.Now()
		for _, c := range prev.ChunkLocators() {
			if err := store.RemoveRef(prevM, chunkref.ChunkID(c), now); err != nil {
				return err
			}
		}
		if err := b.deleteAppendSideRecordsInTxn(txn, bucket, key, prev.VersionID, now); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	data, err := marshalObject(obj)
	if err != nil {
		return err
	}
	if err := setBadgerValue(txn, mk, data); err != nil {
		return err
	}
	for _, c := range obj.ChunkLocators() {
		if err := store.AddRef(m, chunkref.ChunkID(c)); err != nil {
			return err
		}
	}
	return nil
}

// readObjectInTxn reads and decodes the Object stored at mk within txn.
// Returns (nil, badger.ErrKeyNotFound) when no record exists.
func (b *LocalBackend) readObjectInTxn(txn *badger.Txn, mk []byte) (*Object, error) {
	plain, err := getBadgerValue(txn, mk)
	if err != nil {
		return nil, err
	}
	var obj Object
	if err := unmarshalObjectInto(plain, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}
