package storage

import (
	"bytes"
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

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/storage/datawal"
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
// ErrAppendCapExceeded if the object already has MaxAppendSegments segments.
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
	// TODO(Task 3.1): replace segment-checksum-as-MD5-proxy with the real
	// AppendObject call payload MD5 captured via TeeReader at the API boundary.
	// Stopgap mirrors the cluster path (internal/cluster/apply.go).
	callMD5s := [][]byte{append([]byte(nil), seg.Checksum...)}
	obj := &Object{
		Key:            key,
		Size:           seg.Size,
		ContentType:    "application/octet-stream",
		ETag:           CompositeETag(callMD5s),
		LastModified:   time.Now().Unix(),
		Segments:       []SegmentRef{seg},
		AppendCallMD5s: callMD5s,
		IsAppendable:   true,
	}
	if err := b.PutObjectRecord(ctx, bucket, key, obj); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return obj, nil
}

func (b *LocalBackend) appendExisting(ctx context.Context, bucket, key string, existing *Object, r io.Reader) (*Object, error) {
	existing, err := b.ensureAppendableBase(ctx, bucket, key, existing)
	if err != nil {
		return nil, err
	}
	seg, err := b.WriteSegmentBlob(bucket, key, r)
	if err != nil {
		return nil, fmt.Errorf("write segment: %w", err)
	}
	segs := append(existing.Segments, seg)
	// TODO(Task 3.1): replace segment-checksum-as-MD5-proxy with the real
	// AppendObject call payload MD5 captured via TeeReader at the API boundary.
	// Stopgap mirrors the cluster path (internal/cluster/apply.go).
	callMD5s := append(append([][]byte(nil), existing.AppendCallMD5s...), append([]byte(nil), seg.Checksum...))
	obj := *existing
	obj.Segments = segs
	obj.Size = existing.Size + seg.Size
	obj.IsAppendable = true
	obj.AppendCallMD5s = callMD5s
	obj.ETag = CompositeETag(callMD5s)
	obj.LastModified = time.Now().Unix()
	if err := b.PutObjectRecord(ctx, bucket, key, &obj); err != nil {
		return nil, fmt.Errorf("persist: %w", err)
	}
	return &obj, nil
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
	blobID := uuid.Must(uuid.NewV7()).String()
	path := b.segmentPath(bucket, key, blobID)
	if b.dataWAL != nil {
		payload, err := io.ReadAll(io.LimitReader(r, datawal.MaxPayloadBytes+1))
		if err != nil {
			return SegmentRef{}, err
		}
		if len(payload) > datawal.MaxPayloadBytes {
			return SegmentRef{}, fmt.Errorf("datawal: payload too large: %d", len(payload))
		}
		_, err = b.dataWAL.AppendReader(context.Background(), datawal.Record{
			Op:     datawal.OpSegmentPut,
			Bucket: bucket,
			Key:    key,
			Target: blobID,
			Size:   int64(len(payload)),
		}, bytes.NewReader(payload))
		if err != nil {
			return SegmentRef{}, err
		}
		if err := b.dataWAL.Flush(); err != nil {
			return SegmentRef{}, err
		}
		r = bytes.NewReader(payload)
	}
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
	} else if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	data, err := marshalObject(obj)
	if err != nil {
		return err
	}
	if err := setBadgerValue(txn, b.encryptor, badgerDomainObject, mk, data); err != nil {
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
	plain, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, mk)
	if err != nil {
		return nil, err
	}
	var obj Object
	if err := unmarshalObjectInto(plain, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}
