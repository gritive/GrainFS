package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// localTraceEnabled activates per-stage PutObject/HeadObject latency logging.
// Enable with GRAINFS_VOLUME_TRACE=1.
var localTraceEnabled = os.Getenv("GRAINFS_VOLUME_TRACE") == "1"

// LocalBackend stores objects as flat files on disk with BadgerDB for metadata.
type LocalBackend struct {
	root       string
	db         *badger.DB
	encryptor  *encrypt.Encryptor
	dataWAL    DataWAL
	dataWALDir string
}

type DataWAL interface {
	Append(context.Context, datawal.Record) (uint64, error)
	AppendReader(context.Context, datawal.Record, io.Reader) (uint64, error)
	Flush() error
	Dir() string
}

var (
	_ Backend            = (*LocalBackend)(nil)
	_ UserMetadataPutter = (*LocalBackend)(nil)
	_ PartialIO          = (*LocalBackend)(nil)
	_ Truncatable        = (*LocalBackend)(nil)
)

// DB exposes the underlying BadgerDB for shared use (lifecycle, events).
func (b *LocalBackend) DB() *badger.DB { return b.db }

// NewLocalBackend creates a new local storage backend.
func NewLocalBackend(root string) (*LocalBackend, error) {
	return newLocalBackend(root, nil)
}

func NewEncryptedLocalBackend(root string, enc *encrypt.Encryptor) (*LocalBackend, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted local backend requires encryptor")
	}
	return newLocalBackend(root, enc)
}

func NewLocalBackendWithDataWAL(root string, dwal DataWAL) (*LocalBackend, error) {
	if dwal == nil {
		return nil, fmt.Errorf("local backend data wal requires wal")
	}
	b, err := newLocalBackend(root, nil)
	if err != nil {
		return nil, err
	}
	b.dataWAL = dwal
	b.dataWALDir = dwal.Dir()
	return b, nil
}

func NewEncryptedLocalBackendWithDataWAL(root string, enc *encrypt.Encryptor, dwal DataWAL) (*LocalBackend, error) {
	if enc == nil {
		return nil, fmt.Errorf("encrypted local backend requires encryptor")
	}
	if dwal == nil {
		return nil, fmt.Errorf("encrypted local backend data wal requires wal")
	}
	b, err := newLocalBackend(root, enc)
	if err != nil {
		return nil, err
	}
	b.dataWAL = dwal
	b.dataWALDir = dwal.Dir()
	return b, nil
}

func newLocalBackend(root string, enc *encrypt.Encryptor) (*LocalBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbDir := filepath.Join(root, "meta")
	opts := badgerutil.SmallOptions(dbDir)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	return &LocalBackend{root: root, db: db, encryptor: enc, dataWALDir: filepath.Join(root, "datawal")}, nil
}

// Close closes the metadata database.
func (b *LocalBackend) Close() error {
	return b.db.Close()
}

func (b *LocalBackend) bucketKey(bucket string) []byte {
	return []byte("bucket:" + bucket)
}

func (b *LocalBackend) objectMetaKey(bucket, key string) []byte {
	return []byte("obj:" + bucket + "/" + key)
}

func (b *LocalBackend) bucketDir(bucket string) string {
	return filepath.Join(b.root, "data", bucket)
}

func (b *LocalBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.root, "data", bucket, key)
}

func encryptedObjectFileDomain(bucket, key string) string {
	return "local-object-file:" + bucket + "/" + key
}

// OpenLocalReplica returns a ReadCloser for the locally-stored copy of an
// object. It does NOT fall back to peers (there are none in solo mode) and
// returns os.ErrNotExist when the file is missing — the contract scrubber
// verifiers rely on.
func (b *LocalBackend) OpenLocalReplica(bucket, key string) (io.ReadCloser, error) {
	obj, err := b.HeadObject(context.Background(), bucket, key)
	if err != nil {
		return nil, err
	}
	if obj.Segments != nil {
		rc, _, err := b.GetObject(context.Background(), bucket, key)
		return rc, err
	}
	objPath := b.objectPath(bucket, key)
	if b.encryptor != nil {
		return openEncryptedObjectFile(objPath, b.encryptor, encryptedObjectFileDomain(bucket, key), obj.Size)
	}
	return os.Open(objPath)
}

func (b *LocalBackend) CreateBucket(ctx context.Context, bucket string) error {
	_ = ctx
	return b.db.Update(func(txn *badger.Txn) error {
		bk := b.bucketKey(bucket)
		_, err := txn.Get(bk)
		if err == nil {
			return ErrBucketAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
			return fmt.Errorf("create bucket dir: %w", err)
		}
		return setBadgerValue(txn, b.encryptor, badgerDomainBucket, bk, []byte(`{}`))
	})
}

func (b *LocalBackend) HeadBucket(ctx context.Context, bucket string) error {
	_ = ctx
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		}
		return err
	})
}

func (b *LocalBackend) DeleteBucket(ctx context.Context, bucket string) error {
	_ = ctx
	return b.db.Update(func(txn *badger.Txn) error {
		bk := b.bucketKey(bucket)
		_, err := txn.Get(bk)
		if err == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		}
		if err != nil {
			return err
		}

		prefix := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return ErrBucketNotEmpty
		}

		if err := os.RemoveAll(b.bucketDir(bucket)); err != nil {
			return fmt.Errorf("remove bucket dir: %w", err)
		}
		if err := txn.Delete(bk); err != nil {
			return err
		}
		// Delete policy key if present; bucket recreation must not inherit a stale policy.
		if err := txn.Delete(b.policyKey(bucket)); err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("delete policy key: %w", err)
		}
		return nil
	})
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
func (b *LocalBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	if err := b.WalkObjects(ctx, bucket, "", func(obj *Object) error {
		return b.DeleteObject(ctx, bucket, obj.Key)
	}); err != nil {
		return fmt.Errorf("force delete: walk objects: %w", err)
	}
	return b.DeleteBucket(ctx, bucket)
}

func (b *LocalBackend) ListBuckets(ctx context.Context) ([]string, error) {
	_ = ctx
	var buckets []string
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("bucket:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			name := strings.TrimPrefix(key, "bucket:")
			buckets = append(buckets, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(buckets)
	return buckets, nil
}

func (b *LocalBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return b.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (b *LocalBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*Object, error) {
	return b.PutObjectWithRequest(ctx, PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (b *LocalBackend) PutObjectWithRequest(ctx context.Context, req PutObjectRequest) (*Object, error) {
	bucket, key := req.Bucket, req.Key
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	var tStart time.Time
	if localTraceEnabled {
		tStart = time.Now()
	}

	// Every object — any size, encrypted or plain — flows through
	// SegmentWriter. The pipeline chunks the input, fans out segment writes
	// across workers, and computes ETag = md5(plaintext) via TeeReader. On
	// error the partially-written segment blobs become orphans and are
	// reclaimed by the scrubber.
	w := NewSegmentWriter(localBackendAdapter{b})
	obj, err := w.Write(ctx, bucket, key, req.ContentType, req.Body)
	if err != nil {
		return nil, fmt.Errorf("write segments: %w", err)
	}

	// SegmentWriter sets Key/Size/ContentType/ETag/LastModified/Segments.
	// Layer on per-request metadata before persisting.
	obj.UserMetadata = cloneStringMap(req.UserMetadata)
	obj.SSEAlgorithm = req.SystemMetadata.SSEAlgorithm
	if req.ACL != nil {
		obj.ACL = *req.ACL
	}

	if err := b.PutObjectRecord(ctx, bucket, key, obj); err != nil {
		// Segments are now orphans; scrubber sweep reclaims them.
		return nil, fmt.Errorf("commit object meta: %w", err)
	}

	if localTraceEnabled {
		log.Debug().Dur("total", time.Since(tStart)).Int64("size", obj.Size).Int("segments", len(obj.Segments)).Str("bucket", bucket).Msg("PutObject trace")
	}

	return obj, nil
}

func (b *LocalBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	// Backend boundary readamp: every disk-touching GetObject feeds
	// the simulator. CachedBackend sits in front of us, so callers
	// that hit the object cache never reach this point. The hit-rate
	// curve at this tracker therefore answers exactly what UBC would
	// have caught beyond the existing object cache.
	readamp.RecordBackendObject(bucket, key)
	obj, err := b.HeadObject(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}

	// Every object — including zero-byte ones (which have one empty
	// trailing segment) — is now segment-backed.
	if obj.Segments != nil {
		// Fast path: single-segment objects (everything under the 16 MiB
		// chunk threshold) stream directly from the segment file. For
		// unencrypted segments this returns *os.File, which Hertz's
		// SetBodyStream upgrades to sendfile(2). For encrypted segments
		// the AEAD reader avoids the SegmentReader fan-out overhead.
		if len(obj.Segments) == 1 {
			seg := obj.Segments[0]
			segPath := b.segmentPath(bucket, key, seg.BlobID)
			if b.encryptor != nil {
				domain := encryptedObjectFileDomain(bucket, key+"/segments/"+seg.BlobID)
				rc, err := openEncryptedObjectFile(segPath, b.encryptor, domain, seg.Size)
				if err != nil {
					return nil, nil, fmt.Errorf("open encrypted segment: %w", err)
				}
				return rc, obj, nil
			}
			f, err := os.Open(segPath)
			if err != nil {
				return nil, nil, fmt.Errorf("open segment: %w", err)
			}
			return f, obj, nil
		}
		// Multi-segment: stream via the parallel SegmentReader.
		store := localSegmentStore{b: b, bucket: bucket, key: key}
		return io.NopCloser(NewSegmentReader(store, obj.Segments)), obj, nil
	}

	// Legacy single-file path for objects predating segments (e.g.
	// __grainfs_volumes Volume Device blocks written via WriteAt). Range
	// GETs and Volume Device reads keep using ReadAt directly.
	if b.encryptor != nil {
		rc, err := openEncryptedObjectFile(b.objectPath(bucket, key), b.encryptor, encryptedObjectFileDomain(bucket, key), obj.Size)
		if err != nil {
			return nil, nil, fmt.Errorf("open encrypted object: %w", err)
		}
		return rc, obj, nil
	}

	f, err := os.Open(b.objectPath(bucket, key))
	if err != nil {
		return nil, nil, fmt.Errorf("open object: %w", err)
	}

	return f, obj, nil
}

func (b *LocalBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	_ = ctx

	var tHead time.Time
	if localTraceEnabled {
		tHead = time.Now()
	}

	// One Badger View serves both checks. Happy path is a single Get on the
	// object meta key. When that misses, we use the same transaction to
	// distinguish ErrBucketNotFound from ErrObjectNotFound — the prior code
	// always did a separate db.View for the bucket lookup, paying Badger's
	// per-View overhead (getMemTables alloc cluster) twice on every call.
	var obj Object
	err := b.db.View(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(bucket, key))
		if err == nil {
			return unmarshalObjectInto(val, &obj)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		// Object missing: probe the bucket so we return the right error.
		if _, berr := txn.Get(b.bucketKey(bucket)); berr == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		} else if berr != nil {
			return berr
		}
		return ErrObjectNotFound
	})

	if localTraceEnabled {
		log.Debug().Dur("badger_view", time.Since(tHead)).Str("bucket", bucket).Msg("HeadObject trace")
	}

	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// SetObjectACL satisfies storage.ACLSetter. Updates the ACL on the stored object metadata.
func (b *LocalBackend) SetObjectACL(bucket, key string, acl uint8) error {
	mk := b.objectMetaKey(bucket, key)
	return b.db.Update(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, mk)
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		var obj Object
		if err := unmarshalObjectInto(val, &obj); err != nil {
			return err
		}
		obj.ACL = acl
		newVal, err := marshalObject(&obj)
		if err != nil {
			return err
		}
		return setBadgerValue(txn, b.encryptor, badgerDomainObject, mk, newVal)
	})
}

// SetObjectTags satisfies storage.ObjectTagsSetter. Replaces the tag set on
// the target version. versionID="" targets the current version. Passing nil
// clears all tags. Does not modify ETag, LastModified, or blob bytes —
// matches AWS S3 semantics.
func (b *LocalBackend) SetObjectTags(bucket, key, versionID string, tags []Tag) error {
	if versionID != "" {
		return UnsupportedOperationError{Op: "SetObjectTags", Reason: UnsupportedReasonNoAdapter}
	}
	mk := b.objectMetaKey(bucket, key)
	return b.db.Update(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, mk)
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		var obj Object
		if err := unmarshalObjectInto(val, &obj); err != nil {
			return err
		}
		if len(tags) == 0 {
			obj.Tags = nil
		} else {
			cp := make([]Tag, len(tags))
			copy(cp, tags)
			obj.Tags = cp
		}
		newVal, err := marshalObject(&obj)
		if err != nil {
			return err
		}
		return setBadgerValue(txn, b.encryptor, badgerDomainObject, mk, newVal)
	})
}

// GetObjectTags satisfies storage.ObjectTagsGetter. Returns a defensive copy
// of the tag set on the target version. versionID="" targets the current version.
func (b *LocalBackend) GetObjectTags(bucket, key, versionID string) ([]Tag, error) {
	if versionID != "" {
		return nil, UnsupportedOperationError{Op: "GetObjectTags", Reason: UnsupportedReasonNoAdapter}
	}
	mk := b.objectMetaKey(bucket, key)
	var result []Tag
	err := b.db.View(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, mk)
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		var obj Object
		if err := unmarshalObjectInto(val, &obj); err != nil {
			return err
		}
		if len(obj.Tags) > 0 {
			result = make([]Tag, len(obj.Tags))
			copy(result, obj.Tags)
		}
		return nil
	})
	return result, err
}

// Truncate implements storage.Truncatable.
func (b *LocalBackend) Truncate(ctx context.Context, bucket, key string, size int64) error {
	obj, err := b.HeadObject(ctx, bucket, key)
	if err == nil && obj.Segments != nil {
		_, err = b.rewriteSegmentedObject(ctx, bucket, key, obj, func(w io.Writer) error {
			rc, _, err := b.GetObject(ctx, bucket, key)
			if err != nil {
				return err
			}
			defer rc.Close()
			if size <= obj.Size {
				_, err = io.CopyN(w, rc, size)
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			if _, err := io.Copy(w, rc); err != nil {
				return err
			}
			return writeZeros(w, size-obj.Size)
		})
		return err
	}
	if err != nil {
		if !errors.Is(err, ErrObjectNotFound) {
			return err
		}
		if b.encryptor != nil {
			return err
		}
	}
	objPath := b.objectPath(bucket, key)
	var currentSize int64
	if b.encryptor != nil {
		currentSize = obj.Size
		if _, err := truncateEncryptedObjectFile(objPath, b.encryptor, encryptedObjectFileDomain(bucket, key), currentSize, size); err != nil {
			return fmt.Errorf("truncate encrypted object: %w", err)
		}
	} else {
		if err := os.Truncate(objPath, size); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}
	}
	mk := b.objectMetaKey(bucket, key)
	return b.db.Update(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainObject, mk)
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		var obj Object
		if err := unmarshalObjectInto(val, &obj); err != nil {
			return err
		}
		obj.Size = size
		newVal, err := marshalObject(&obj)
		if err != nil {
			return err
		}
		return setBadgerValue(txn, b.encryptor, badgerDomainObject, mk, newVal)
	})
}

// WriteAt patches [offset, offset+len(data)) of the stored object.
// The file is created if it does not exist; it is extended if the write exceeds the
// current size. Bytes outside the written range are preserved, and writes before the
// first byte produce a sparse hole filled with zeros.
//
// Unencrypted writes use pwrite(2) and are O(len(data)). Encrypted writes preserve
// semantics by rewriting the encrypted object, so callers should check PreferWriteAt
// before using this as a hot-path optimization.
func (b *LocalBackend) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error) {
	var tStart, tStage time.Time
	if localTraceEnabled {
		tStart = time.Now()
		tStage = tStart
	}

	existing, existingErr := b.HeadObject(ctx, bucket, key)
	if existingErr == nil && existing.Segments != nil {
		return b.writeAtSegmentedObject(ctx, bucket, key, existing, offset, data)
	}
	if existingErr != nil && !errors.Is(existingErr, ErrObjectNotFound) {
		return nil, existingErr
	}

	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}
	if localTraceEnabled {
		log.Debug().Dur("mkdir", time.Since(tStage)).Str("bucket", bucket).Msg("WriteAt trace")
		tStage = time.Now()
	}

	if b.encryptor != nil {
		currentSize := int64(0)
		if existing, err := b.HeadObject(context.Background(), bucket, key); err == nil {
			currentSize = existing.Size
		} else if !errors.Is(err, ErrObjectNotFound) {
			return nil, err
		}
		size, etag, err := writeAtEncryptedObjectFile(objPath, b.encryptor, encryptedObjectFileDomain(bucket, key), offset, data, currentSize)
		if err != nil {
			return nil, fmt.Errorf("encrypted writeat: %w", err)
		}
		now := time.Now().Unix()
		obj := &Object{
			Key:          key,
			Size:         size,
			ContentType:  "application/octet-stream",
			ETag:         etag,
			LastModified: now,
		}
		meta, err := marshalObject(obj)
		if err != nil {
			return nil, fmt.Errorf("marshal metadata: %w", err)
		}
		if err := b.db.Update(func(txn *badger.Txn) error {
			return setBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(bucket, key), meta)
		}); err != nil {
			return nil, err
		}
		return obj, nil
	}

	// O_CREATE|O_RDWR: create if new, open in-place if existing.
	f, err := os.OpenFile(objPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()
	if localTraceEnabled {
		log.Debug().Dur("open", time.Since(tStage)).Msg("WriteAt trace")
		tStage = time.Now()
	}

	if _, err := f.WriteAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("pwrite: %w", err)
	}
	if localTraceEnabled {
		log.Debug().Dur("pwrite", time.Since(tStage)).Int("bytes", len(data)).Msg("WriteAt trace")
		tStage = time.Now()
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	size := fi.Size()
	if localTraceEnabled {
		log.Debug().Dur("stat", time.Since(tStage)).Int64("size", size).Msg("WriteAt trace")
		tStage = time.Now()
	}

	// ETag = xxhash3(file). Corruption-detection oracle for Volume scrub.
	// Internal buckets (WriteAt callers) use xxhash3 (~37x faster than MD5).
	// For partial writes (offset>0 or len(data)<size) we re-read the file so
	// the stored ETag matches on-disk bytes.
	var etag string
	if offset == 0 && int64(len(data)) == size {
		etag = InternalETag(data)
	} else {
		xh := GetXXH3Hasher()
		buf := make([]byte, 64*1024)
		var off int64
		for off < size {
			n, rerr := f.ReadAt(buf, off)
			if n > 0 {
				_, _ = xh.Write(buf[:n])
				off += int64(n)
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				PutXXH3Hasher(xh)
				return nil, fmt.Errorf("xxh3 readback: %w", rerr)
			}
		}
		var hbuf [8]byte
		binary.BigEndian.PutUint64(hbuf[:], xh.Sum64())
		etag = hex.EncodeToString(hbuf[:])
		PutXXH3Hasher(xh)
	}
	if localTraceEnabled {
		log.Debug().Dur("etag", time.Since(tStage)).Msg("WriteAt trace")
		tStage = time.Now()
	}

	now := time.Now().Unix()
	obj := &Object{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: now,
	}
	meta, err := marshalObject(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(bucket, key), meta)
	}); err != nil {
		return nil, err
	}
	if localTraceEnabled {
		log.Debug().Dur("badger_update", time.Since(tStage)).Dur("total", time.Since(tStart)).Msg("WriteAt trace")
	}
	return obj, nil
}

func (b *LocalBackend) writeAtSegmentedObject(ctx context.Context, bucket, key string, obj *Object, offset uint64, data []byte) (*Object, error) {
	return b.rewriteSegmentedObject(ctx, bucket, key, obj, func(w io.Writer) error {
		rc, _, err := b.GetObject(ctx, bucket, key)
		if err != nil {
			return err
		}
		defer rc.Close()

		writeOffset := int64(offset)
		prefix := writeOffset
		if prefix > obj.Size {
			prefix = obj.Size
		}
		if prefix > 0 {
			if _, err := io.CopyN(w, rc, prefix); err != nil {
				return err
			}
		}
		if writeOffset > obj.Size {
			if err := writeZeros(w, writeOffset-obj.Size); err != nil {
				return err
			}
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if writeOffset < obj.Size {
			skip := int64(len(data))
			remaining := obj.Size - writeOffset
			if skip > remaining {
				skip = remaining
			}
			if skip > 0 {
				if _, err := io.CopyN(io.Discard, rc, skip); err != nil {
					return err
				}
			}
			if _, err := io.Copy(w, rc); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *LocalBackend) rewriteSegmentedObject(ctx context.Context, bucket, key string, obj *Object, write func(io.Writer) error) (*Object, error) {
	if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
		return nil, fmt.Errorf("create bucket dir: %w", err)
	}
	tmp, err := os.CreateTemp(b.bucketDir(bucket), ".rewrite-*")
	if err != nil {
		return nil, fmt.Errorf("create rewrite temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	if err := write(tmp); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek rewrite temp: %w", err)
	}
	acl := obj.ACL
	next, err := b.PutObjectWithRequest(ctx, PutObjectRequest{
		Bucket:         bucket,
		Key:            key,
		Body:           tmp,
		ContentType:    obj.ContentType,
		UserMetadata:   obj.UserMetadata,
		SystemMetadata: ObjectSystemMetadata{SSEAlgorithm: obj.SSEAlgorithm},
		ACL:            &acl,
	})
	if err != nil {
		return nil, err
	}
	if len(obj.Tags) > 0 {
		tags := make([]Tag, len(obj.Tags))
		copy(tags, obj.Tags)
		next.Tags = tags
		if err := b.PutObjectRecord(ctx, bucket, key, next); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func writeZeros(w io.Writer, n int64) error {
	var zeros [32 * 1024]byte
	for n > 0 {
		chunk := int64(len(zeros))
		if n < chunk {
			chunk = n
		}
		if _, err := w.Write(zeros[:chunk]); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// ReadAt reads up to len(buf) bytes from the object at the given offset.
//
// Segment-backed objects (every object produced by PutObject since Task 1.6)
// walk obj.Segments and dispatch a per-segment pread for each overlapping
// segment. Legacy single-file objects (Volume Device blocks via WriteAt) still
// pread the flat backing file.
func (b *LocalBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	_ = ctx
	if len(buf) == 0 {
		return 0, nil
	}
	obj, err := b.HeadObject(context.Background(), bucket, key)
	if err != nil {
		// Preserve the os.Open-style "file not found" contract that
		// callers (and tests) rely on via os.IsNotExist. The legacy ReadAt
		// returned *os.PathError from os.Open; emit one here so wrappers
		// like CachedBackend continue to satisfy os.IsNotExist.
		if errors.Is(err, ErrObjectNotFound) {
			return 0, &os.PathError{Op: "open", Path: b.objectPath(bucket, key), Err: os.ErrNotExist}
		}
		return 0, err
	}

	// Legacy single-file path: pre-segment objects (Volume Device blocks).
	if obj.Segments == nil {
		objPath := b.objectPath(bucket, key)
		if b.encryptor != nil {
			return readAtEncryptedObjectFile(objPath, b.encryptor, encryptedObjectFileDomain(bucket, key), obj.Size, offset, buf)
		}
		f, err := os.Open(objPath)
		if err != nil {
			return 0, err
		}
		defer f.Close()
		return f.ReadAt(buf, offset)
	}

	// Segment-backed: match os.File.ReadAt semantics — out-of-range offset
	// returns (0, io.EOF) so callers (e.g. readAtRangeReader) don't loop.
	if offset < 0 {
		return 0, fmt.Errorf("ReadAt: negative offset")
	}
	if offset >= obj.Size {
		return 0, io.EOF
	}

	want := int64(len(buf))
	if offset+want > obj.Size {
		want = obj.Size - offset
	}

	var (
		written    int
		cumulative int64
	)
	for _, seg := range obj.Segments {
		segStart := cumulative
		segEnd := cumulative + seg.Size
		cumulative = segEnd

		if segEnd <= offset {
			continue // segment is entirely before the requested range
		}
		if segStart >= offset+want {
			break // segment is entirely past the requested range
		}

		// Intra-segment offset and length.
		var intraOff int64
		if offset > segStart {
			intraOff = offset - segStart
		}
		intraEnd := seg.Size
		if offset+want < segEnd {
			intraEnd = offset + want - segStart
		}
		chunkLen := int(intraEnd - intraOff)
		if chunkLen <= 0 {
			continue
		}

		segPath := b.segmentPath(bucket, key, seg.BlobID)
		dst := buf[written : written+chunkLen]
		if b.encryptor != nil {
			domain := encryptedObjectFileDomain(bucket, key+"/segments/"+seg.BlobID)
			n, rerr := readAtEncryptedObjectFile(segPath, b.encryptor, domain, seg.Size, intraOff, dst)
			written += n
			if rerr != nil && rerr != io.EOF {
				return written, rerr
			}
			if n < chunkLen {
				// short read inside the segment is unexpected; treat as EOF
				return written, io.EOF
			}
		} else {
			f, oerr := os.Open(segPath)
			if oerr != nil {
				return written, oerr
			}
			n, rerr := f.ReadAt(dst, intraOff)
			_ = f.Close()
			written += n
			if rerr != nil && rerr != io.EOF {
				return written, rerr
			}
			if n < chunkLen {
				return written, io.EOF
			}
		}
	}

	if int64(written) < int64(len(buf)) {
		// Caller asked for more than the object holds.
		return written, io.EOF
	}
	return written, nil
}

func (b *LocalBackend) PreferWriteAt(bucket string) bool {
	return b.encryptor == nil && IsInternalBucket(bucket)
}

func (b *LocalBackend) RecoverDataWAL(ctx context.Context) error {
	return datawal.Recover(ctx, b.dataWALDir, 0, b.encryptor, localDataWALMaterializer{b: b})
}

type localDataWALMaterializer struct {
	b *LocalBackend
}

func (m localDataWALMaterializer) HasReplacement(ctx context.Context, rec datawal.Record) (bool, error) {
	_ = ctx
	if rec.Op != datawal.OpSegmentPut {
		return false, nil
	}

	path := m.b.segmentPath(rec.Bucket, rec.Key, rec.Target)
	if m.b.encryptor == nil {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return info.Size() == rec.Size, nil
	}

	rc, err := openEncryptedObjectFile(path, m.b.encryptor, encryptedObjectFileDomain(rec.Bucket, rec.Key+"/segments/"+rec.Target), rec.Size)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		return false, err
	}
	return bytes.Equal(got, rec.Payload), nil
}

func (m localDataWALMaterializer) Materialize(ctx context.Context, rec datawal.Record) error {
	switch rec.Op {
	case datawal.OpSegmentPut:
		path := m.b.segmentPath(rec.Bucket, rec.Key, rec.Target)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if m.b.encryptor != nil {
			_, err := writeEncryptedObjectFile(path, m.b.encryptor, encryptedObjectFileDomain(rec.Bucket, rec.Key+"/segments/"+rec.Target), bytes.NewReader(rec.Payload), io.Discard)
			return err
		}
		return os.WriteFile(path, rec.Payload, 0o644)
	case datawal.OpObjectWriteAt:
		_, err := m.b.WriteAt(ctx, rec.Bucket, rec.Key, uint64(rec.Offset), rec.Payload)
		return err
	case datawal.OpObjectTruncate:
		return m.b.Truncate(ctx, rec.Bucket, rec.Key, rec.Size)
	default:
		return nil
	}
}

// Sync implements storage.Syncable.
func (b *LocalBackend) Sync(bucket, key string) error {
	if b.dataWAL != nil {
		return b.dataWAL.Flush()
	}
	obj, err := b.HeadObject(context.Background(), bucket, key)
	if err != nil {
		return err
	}
	if obj.Segments != nil {
		for _, seg := range obj.Segments {
			if err := syncFile(b.segmentPath(bucket, key, seg.BlobID)); err != nil {
				return err
			}
		}
		return nil
	}
	objPath := b.objectPath(bucket, key)
	return syncFile(objPath)
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("sync open: %w", err)
	}
	defer f.Close()
	return f.Sync()
}

func (b *LocalBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}

	// Legacy single-file blob (Volume Device / pre-segment objects).
	os.Remove(b.objectPath(bucket, key))
	// Segment blobs from segmented PUTs live under <key>_segments/.
	os.RemoveAll(b.objectPath(bucket, key) + "_segments")

	return b.db.Update(func(txn *badger.Txn) error {
		mk := b.objectMetaKey(bucket, key)
		_, err := txn.Get(mk)
		if err == badger.ErrKeyNotFound {
			return nil // S3: delete nonexistent is not an error
		}
		if err != nil {
			return err
		}
		return txn.Delete(mk)
	})
}

func (b *LocalBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	var objects []*Object
	err := b.db.View(func(txn *badger.Txn) error {
		pfx := []byte("obj:" + bucket + "/" + prefix)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		count := 0
		for it.Seek(pfx); it.ValidForPrefix([]byte("obj:" + bucket + "/")); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			if count >= maxKeys {
				break
			}
			item := it.Item()
			itemKey := item.KeyCopy(nil)
			var obj Object
			err := item.Value(func(val []byte) error {
				plain, err := openBadgerValue(b.encryptor, badgerDomainObject, itemKey, val)
				if err != nil {
					return err
				}
				return unmarshalObjectInto(plain, &obj)
			})
			if err != nil {
				return err
			}
			objects = append(objects, &obj)
			count++
		}
		return nil
	})
	return objects, err
}

// ListObjectsPage returns one S3 ListObjects page from the badger-backed
// object key space. Entries with key strictly greater than `marker` are
// returned, capped at `maxKeys`. `truncated` is true when more entries
// match beyond the returned slice. The iterator seeks past `marker` so
// pagination doesn't have to materialise the skipped prefix in memory.
func (b *LocalBackend) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*Object, bool, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, false, err
	}
	var (
		objects   []*Object
		truncated bool
	)
	err := b.db.View(func(txn *badger.Txn) error {
		pfx := []byte("obj:" + bucket + "/" + prefix)
		seek := pfx
		if marker != "" {
			// Resume strictly after `marker`. Append NUL so the iterator
			// lands on the first key whose suffix sorts after marker — the
			// "obj:bucket/" prefix is shared with `pfx`, so the seek key is
			// "obj:bucket/<marker>\x00".
			seek = append([]byte("obj:"+bucket+"/"+marker), 0)
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(seek); it.ValidForPrefix(pfx); it.Next() {
			if len(objects) >= maxKeys {
				truncated = true
				break
			}
			item := it.Item()
			itemKey := item.KeyCopy(nil)
			var obj Object
			if err := item.Value(func(val []byte) error {
				plain, err := openBadgerValue(b.encryptor, badgerDomainObject, itemKey, val)
				if err != nil {
					return err
				}
				return unmarshalObjectInto(plain, &obj)
			}); err != nil {
				return err
			}
			objects = append(objects, &obj)
		}
		return nil
	})
	return objects, truncated, err
}

func (b *LocalBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		pfx := []byte("obj:" + bucket + "/" + prefix)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			item := it.Item()
			itemKey := item.KeyCopy(nil)
			var obj Object
			if err := item.Value(func(val []byte) error {
				plain, err := openBadgerValue(b.encryptor, badgerDomainObject, itemKey, val)
				if err != nil {
					return err
				}
				return unmarshalObjectInto(plain, &obj)
			}); err != nil {
				return err
			}
			if err := fn(&obj); err != nil {
				return err
			}
		}
		return nil
	})
}

// WalkObjectKeys iterates object keys without unmarshalling object metadata.
func (b *LocalBackend) WalkObjectKeys(ctx context.Context, bucket, prefix string, fn func(string) error) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := []byte("obj:" + bucket + "/" + prefix)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := string(it.Item().Key()[len(rawBucketPfx):])
			if err := fn(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// ScanObjectsGrouped emits one ObjectKeyGroup per object key in the bucket.
// LocalBackend is unversioned: each group has exactly one version
// (IsLatest=true, VersionID="", IsDeleteMarker=false). Versioned cluster
// deployments use DistributedBackend.ScanObjectsGrouped which has its own
// version-aware implementation.
//
// Iteration order matches WalkObjects (badger key order, lexicographic).
// The channel is buffered (16) and closed when iteration completes or an
// error aborts the scan.
func (b *LocalBackend) ScanObjectsGrouped(bucket string) (<-chan ObjectKeyGroup, error) {
	if err := b.HeadBucket(context.Background(), bucket); err != nil {
		return nil, err
	}
	out := make(chan ObjectKeyGroup, 16)
	go func() {
		defer close(out)
		_ = b.db.View(func(txn *badger.Txn) error {
			pfx := []byte("obj:" + bucket + "/")
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
				item := it.Item()
				itemKey := item.KeyCopy(nil)
				var obj Object
				if err := item.Value(func(val []byte) error {
					plain, err := openBadgerValue(b.encryptor, badgerDomainObject, itemKey, val)
					if err != nil {
						return err
					}
					return unmarshalObjectInto(plain, &obj)
				}); err != nil {
					return err
				}
				out <- ObjectKeyGroup{
					Bucket: bucket,
					Key:    obj.Key,
					Versions: []ObjectVersionRecord{{
						VersionID:      "",
						IsLatest:       true,
						IsDeleteMarker: false,
						LastModified:   obj.LastModified,
						Size:           obj.Size,
						ETag:           obj.ETag,
						Tags:           obj.Tags,
					}},
				}
			}
			return nil
		})
	}()
	return out, nil
}

// ScanLocalMultipartUploads enumerates in-progress multipart uploads in the
// given bucket on THIS node. Per the lifecycle split execution model, each
// cluster node aborts its own stranded uploads — there is no cross-node MPU
// view.
//
// Cluster deployments call this on each node (via DistributedBackend delegate,
// Task 6); single-node deployments call it directly. The channel is buffered
// (16) and closed when iteration completes or an error aborts the scan.
func (b *LocalBackend) ScanLocalMultipartUploads(bucket string) (<-chan MultipartUploadRecord, error) {
	if err := b.HeadBucket(context.Background(), bucket); err != nil {
		return nil, err
	}
	out := make(chan MultipartUploadRecord, 16)
	go func() {
		defer close(out)
		_ = b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			mpuPrefix := []byte("mpu:")
			for it.Seek(mpuPrefix); it.ValidForPrefix(mpuPrefix); it.Next() {
				item := it.Item()
				itemKey := item.KeyCopy(nil)
				if err := item.Value(func(val []byte) error {
					plain, err := openBadgerValue(b.encryptor, badgerDomainMultipart, itemKey, val)
					if err != nil {
						return err
					}
					meta, err := unmarshalMultipartMeta(plain)
					if err != nil {
						return err
					}
					if meta.Bucket != bucket {
						return nil
					}
					out <- MultipartUploadRecord{
						Bucket:      meta.Bucket,
						Key:         meta.Key,
						UploadID:    meta.UploadID,
						InitiatedAt: meta.CreatedAt,
					}
					return nil
				}); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	return out, nil
}

// CopyObject copies an object by reading the source and writing to the destination.
func (b *LocalBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error) {
	ctx := context.Background()
	rc, obj, err := b.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	if b.encryptor == nil && obj.Segments != nil {
		if err := b.HeadBucket(ctx, dstBucket); err != nil {
			return nil, err
		}
		segs := make([]SegmentRef, len(obj.Segments))
		for i, seg := range obj.Segments {
			segs[i] = cloneSegmentRef(seg)
			srcPath := b.segmentPath(srcBucket, srcKey, seg.BlobID)
			dstPath := b.segmentPath(dstBucket, dstKey, seg.BlobID)
			if err := copyFile(dstPath, srcPath); err != nil {
				os.RemoveAll(b.objectPath(dstBucket, dstKey) + "_segments")
				return nil, err
			}
		}
		copied := *obj
		copied.Key = dstKey
		copied.LastModified = time.Now().Unix()
		copied.UserMetadata = cloneStringMap(obj.UserMetadata)
		copied.Segments = segs
		copied.Coalesced = nil
		if len(obj.Tags) > 0 {
			copied.Tags = make([]Tag, len(obj.Tags))
			copy(copied.Tags, obj.Tags)
		}
		if err := b.PutObjectRecord(ctx, dstBucket, dstKey, &copied); err != nil {
			os.RemoveAll(b.objectPath(dstBucket, dstKey) + "_segments")
			return nil, err
		}
		return &copied, nil
	}

	return b.PutObjectWithRequest(ctx, PutObjectRequest{
		Bucket:         dstBucket,
		Key:            dstKey,
		Body:           rc,
		ContentType:    obj.ContentType,
		UserMetadata:   obj.UserMetadata,
		SystemMetadata: ObjectSystemMetadata{SSEAlgorithm: obj.SSEAlgorithm},
	})
}

func cloneSegmentRef(seg SegmentRef) SegmentRef {
	out := seg
	if len(seg.Checksum) > 0 {
		out.Checksum = append([]byte(nil), seg.Checksum...)
	}
	if len(seg.NodeIDs) > 0 {
		out.NodeIDs = append([]string(nil), seg.NodeIDs...)
	}
	return out
}

func copyFile(dstPath, srcPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	if _, err := io.CopyBuffer(dst, src, make([]byte, 64*1024)); err != nil {
		dst.Close()
		os.Remove(dstPath)
		return err
	}
	if err := dst.Close(); err != nil {
		os.Remove(dstPath)
		return err
	}
	return nil
}

func (b *LocalBackend) policyKey(bucket string) []byte {
	return []byte("policy:" + bucket)
}

// GetBucketPolicy returns the raw policy JSON for a bucket.
func (b *LocalBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	var data []byte
	err := b.db.View(func(txn *badger.Txn) error {
		val, err := getBadgerValue(txn, b.encryptor, badgerDomainPolicy, b.policyKey(bucket))
		if err == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		data = val
		return nil
	})
	return data, err
}

// SetBucketPolicy stores the raw policy JSON for a bucket.
func (b *LocalBackend) SetBucketPolicy(bucket string, policyJSON []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return setBadgerValue(txn, b.encryptor, badgerDomainPolicy, b.policyKey(bucket), policyJSON)
	})
}

// DeleteBucketPolicy removes the policy for a bucket.
func (b *LocalBackend) DeleteBucketPolicy(bucket string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(b.policyKey(bucket))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// ListAllObjects implements Snapshotable: scans all object metadata across all buckets.
func (b *LocalBackend) ListAllObjects() ([]SnapshotObject, error) {
	var objs []SnapshotObject
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("obj:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			rawKey := string(it.Item().Key())
			rest := rawKey[len("obj:"):]
			slashIdx := len(rest)
			for i, c := range rest {
				if c == '/' {
					slashIdx = i
					break
				}
			}
			bucket := rest[:slashIdx]
			key := rest[slashIdx+1:]

			item := it.Item()
			itemKey := item.KeyCopy(nil)
			var obj Object
			if err := item.Value(func(val []byte) error {
				plain, err := openBadgerValue(b.encryptor, badgerDomainObject, itemKey, val)
				if err != nil {
					return err
				}
				return unmarshalObjectInto(plain, &obj)
			}); err != nil {
				return err
			}
			var segments []SegmentRef
			if len(obj.Segments) > 0 {
				segments = append(segments, obj.Segments...)
			}
			objs = append(objs, SnapshotObject{
				Bucket:       bucket,
				Key:          key,
				ETag:         obj.ETag,
				Size:         obj.Size,
				ContentType:  obj.ContentType,
				Modified:     obj.LastModified,
				SSEAlgorithm: obj.SSEAlgorithm,
				Segments:     segments,
				Tags:         obj.Tags,
			})
		}
		return nil
	})
	return objs, err
}

// RestoreObjects implements Snapshotable: replaces current object metadata with snapshot state.
// Objects whose blobs no longer exist on disk are reported as stale.
func (b *LocalBackend) RestoreObjects(objects []SnapshotObject) (int, []StaleBlob, error) {
	// Build lookup set of (bucket/key) from snapshot
	inSnapshot := make(map[string]bool, len(objects))
	for _, o := range objects {
		inSnapshot["obj:"+o.Bucket+"/"+o.Key] = true
	}

	// Delete metadata for objects not in snapshot
	if err := b.db.Update(func(txn *badger.Txn) error {
		prefix := []byte("obj:")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		var toDelete [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if !inSnapshot[string(it.Item().Key())] {
				cp := make([]byte, len(it.Item().Key()))
				copy(cp, it.Item().Key())
				toDelete = append(toDelete, cp)
			}
		}
		it.Close()
		for _, k := range toDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, nil, fmt.Errorf("remove obsolete objects: %w", err)
	}

	var stale []StaleBlob
	var count int
	ctx := context.Background()
	for _, snap := range objects {
		// Ensure bucket exists
		if err := b.CreateBucket(ctx, snap.Bucket); err != nil && !errors.Is(err, ErrBucketAlreadyExists) {
			return count, stale, fmt.Errorf("ensure bucket %s: %w", snap.Bucket, err)
		}
		// Check blob(s) exist on disk. Phase 1.6 routes every object through
		// SegmentWriter, so chunked objects live under <key>_segments/<blob_id>
		// rather than at objectPath. We honor three cases in priority order:
		//   (a) snap.Segments non-empty → verify every segment blob path.
		//   (b) Legacy single-file path (pre-Phase-1 snapshot or size==0
		//       degenerate restore) → verify objectPath.
		//   (c) snap.Size == 0 and no segments → metadata-only, tolerate.
		isStale := false
		switch {
		case len(snap.Segments) > 0:
			for _, seg := range snap.Segments {
				if _, err := os.Stat(b.segmentPath(snap.Bucket, snap.Key, seg.BlobID)); os.IsNotExist(err) {
					isStale = true
					break
				}
			}
		case snap.Size > 0:
			if _, err := os.Stat(b.objectPath(snap.Bucket, snap.Key)); os.IsNotExist(err) {
				isStale = true
			}
		}
		if isStale {
			stale = append(stale, StaleBlob{Bucket: snap.Bucket, Key: snap.Key, ExpectedETag: snap.ETag})
			continue
		}
		// Restore metadata, including segment refs so GetObject can locate the blobs.
		var segments []SegmentRef
		if len(snap.Segments) > 0 {
			segments = append(segments, snap.Segments...)
		}
		obj := &Object{Key: snap.Key, Size: snap.Size, ContentType: snap.ContentType, ETag: snap.ETag, LastModified: snap.Modified, SSEAlgorithm: snap.SSEAlgorithm, Segments: segments, Tags: snap.Tags}
		meta, err := marshalObject(obj)
		if err != nil {
			return count, stale, fmt.Errorf("marshal %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		if err := b.db.Update(func(txn *badger.Txn) error {
			return setBadgerValue(txn, b.encryptor, badgerDomainObject, b.objectMetaKey(snap.Bucket, snap.Key), meta)
		}); err != nil {
			return count, stale, fmt.Errorf("restore %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++
	}
	return count, stale, nil
}
