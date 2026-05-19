package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
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
// (Truncatable, ACLSetter, RequestPutter, UserMetadataPutter).
type AppendObjecter interface {
	AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error)
}

// AppendObject appends data to an existing appendable object, or creates a new
// appendable object when expectedOffset == 0 and the object does not exist.
//
// Returns ErrAppendOffsetMismatch if expectedOffset != current object size,
// ErrAppendCapExceeded if the object already has MaxAppendSegments segments,
// ErrAppendNotSupported if the existing object is not appendable.
func (b *LocalBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error) {
	existing, err := b.HeadObject(ctx, bucket, key)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, fmt.Errorf("head: %w", err)
	}
	if existing == nil {
		if expectedOffset != 0 {
			return nil, ErrAppendOffsetMismatch
		}
		return b.appendNew(ctx, bucket, key, r)
	}
	if !existing.IsAppendable {
		return nil, ErrAppendNotSupported
	}
	if existing.Size != expectedOffset {
		return nil, ErrAppendOffsetMismatch
	}
	if len(existing.Segments) >= MaxAppendSegments {
		return nil, ErrAppendCapExceeded
	}
	return b.appendExisting(ctx, bucket, key, existing, r)
}

func (b *LocalBackend) appendNew(ctx context.Context, bucket, key string, r io.Reader) (*Object, error) {
	seg, err := b.WriteSegmentBlob(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	// TODO(Task 3.1): capture per-call MD5 during WriteSegmentBlob and pass
	// it here so Object.ETag is computed from AppendCallMD5s.
	obj := &Object{
		Key:          key,
		Size:         seg.Size,
		ContentType:  "application/octet-stream",
		ETag:         CompositeETag(nil),
		LastModified: time.Now().Unix(),
		Segments:     []SegmentRef{seg},
		IsAppendable: true,
	}
	if err := b.PutObjectRecord(ctx, bucket, key, obj); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return obj, nil
}

func (b *LocalBackend) appendExisting(ctx context.Context, bucket, key string, existing *Object, r io.Reader) (*Object, error) {
	seg, err := b.WriteSegmentBlob(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	segs := append(existing.Segments, seg)
	obj := *existing
	obj.Segments = segs
	obj.Size = existing.Size + seg.Size
	// TODO(Task 3.1): recompute Object.ETag from AppendCallMD5s.
	obj.ETag = CompositeETag(obj.AppendCallMD5s)
	obj.LastModified = time.Now().Unix()
	if err := b.PutObjectRecord(ctx, bucket, key, &obj); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return &obj, nil
}

// WriteSegmentBlob writes one segment blob to disk under
// objects/<bucket>/<key>_segments/<blobID>. Returns SegmentRef with
// BlobID (UUIDv7) + Size + xxhash3-128 Checksum of the plaintext bytes.
// Exported so cluster.DistributedBackend.AppendObject can call directly.
func (b *LocalBackend) WriteSegmentBlob(bucket, key string, r io.Reader) (SegmentRef, error) {
	blobID := uuid.Must(uuid.NewV7()).String()
	path := b.segmentPath(bucket, key, blobID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return SegmentRef{}, err
	}
	checksumHasher := NewChecksumHasher()
	domain := encryptedObjectFileDomain(bucket, key+"/segments/"+blobID)
	var (
		size int64
		err  error
	)
	if b.encryptor != nil {
		// Encrypted writer tees plaintext through checksumHasher.
		size, err = writeEncryptedObjectFile(path, b.encryptor, domain, r, checksumHasher)
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

// CompositeETag returns the S3-multipart-style composite ETag:
// md5(concat(callMD5s)) + "-<N>".
//
// callMD5s is one raw 16-byte MD5 digest per AppendObject call (S3
// AppendObject semantics). nil/empty input is allowed: it produces the
// stable "<md5 of empty>-0" placeholder used by Task 3.1 wire-up until
// per-call MD5s are captured.
func CompositeETag(callMD5s [][]byte) string {
	h := md5.New()
	for _, d := range callMD5s {
		h.Write(d)
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(callMD5s))
}

// PutObjectRecord persists Object into the backend's BadgerDB. Self-managed txn.
func (b *LocalBackend) PutObjectRecord(ctx context.Context, bucket, key string, obj *Object) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return b.PutObjectRecordInTxn(txn, bucket, key, obj)
	})
}

// PutObjectRecordInTxn persists Object within a caller-provided txn.
// Cluster FSM apply will use this for atomic write within a larger txn.
func (b *LocalBackend) PutObjectRecordInTxn(txn *badger.Txn, bucket, key string, obj *Object) error {
	data, err := marshalObject(obj)
	if err != nil {
		return err
	}
	return setBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(bucket, key), data)
}
