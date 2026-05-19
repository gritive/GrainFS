package packblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// indexEntry tracks a small object stored in a blob file.
type indexEntry struct {
	Location     BlobLocation
	Refcount     atomic.Int64
	OriginalSize int64 // uncompressed byte count
	ContentType  string
	ETag         string
	LastModified int64
	UserMetadata map[string]string
	SSEAlgorithm string
}

// packedKey identifies a packed object by its (bucket, key) tuple. It is the
// in-memory key type for PackedBackend.index, replacing an earlier string
// concatenation ("bucket/key") whose every lookup leaked a heap allocation
// onto the S3 GET hot path. The string form is reconstructed only at
// persistence boundaries (SaveIndex/LoadIndex, blob storage interop).
type packedKey struct {
	bucket string
	key    string
}

// String returns the legacy "bucket/key" form used by the on-disk index JSON
// and the underlying BlobStore entries.
func (k packedKey) String() string { return k.bucket + "/" + k.key }

// parsePackedKey splits a legacy "bucket/key" string back into its tuple
// form. The split is at the first '/' — safe because S3 bucket names cannot
// contain '/' (DNS-style restrictions). Returns false if the input has no
// separator (corrupt index entry; caller should skip).
func parsePackedKey(s string) (packedKey, bool) {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return packedKey{}, false
	}
	return packedKey{bucket: s[:i], key: s[i+1:]}, true
}

// PackedBackend wraps a storage.Backend and packs small objects into blob files.
// Objects below the threshold are stored in append-only blob files.
// Objects at or above the threshold pass through to the inner backend.
//
// The index is a sync.Map keyed by packedKey → *indexEntry. Reads
// (GetObject/HeadObject) are lock-free; writes (PutObject/DeleteObject)
// publish via sync.Map.Swap / LoadAndDelete and decrement displaced
// entries' refcounts atomically. CopyObject preserves the previous
// transactional semantics through a CAS-based refcount increment plus
// a re-validation Load (see CopyObjectWithRequest). Range scans
// (ListObjects, WalkObjects, bucket teardown, SaveIndex) are weakly
// consistent — acceptable for listing-style operations.
// Audit follow-up: docs/architecture/lock-free-audit.md → "PackedBackend.mu
// protects the packed-object index. If packed small object reads become a
// hot-path bottleneck, convert this to the same immutable snapshot pattern
// used by CachedBackend." PR #392 mixed-workload mutex profile attributed
// 91.7% of remaining delay (44.81s / 48.86s) to PackedBackend.PutObject's
// RWMutex.Unlock — the trigger condition was hit.
type PackedBackend struct {
	inner     storage.Backend
	blobStore *BlobStore
	blobDir   string
	threshold int64 // objects below this size go into blobs

	index sync.Map // packedKey → *indexEntry

	stopSave chan struct{} // signals the background index-save goroutine to stop
}

var _ storage.Backend = (*PackedBackend)(nil)
var _ storage.Snapshotable = (*PackedBackend)(nil)
var _ storage.BucketSnapshotable = (*PackedBackend)(nil)
var _ interface {
	CopyObjectWithRequest(context.Context, storage.CopyObjectAccelerationRequest) (*storage.Object, error)
} = (*PackedBackend)(nil)

// PackedBackendOptions configures optional behavior for PackedBackend.
type PackedBackendOptions struct {
	Compress  bool // enable zstd compression for packed (small) objects
	Encryptor *encrypt.Encryptor
}

// NewPackedBackend creates a packed backend wrapping inner.
// Objects smaller than threshold bytes are packed into blob files at blobDir.
// Compression is enabled by default; use NewPackedBackendWithOptions to opt out.
func NewPackedBackend(inner storage.Backend, blobDir string, threshold int64) (*PackedBackend, error) {
	return NewPackedBackendWithOptions(inner, blobDir, threshold, PackedBackendOptions{Compress: true})
}

// NewPackedBackendWithOptions creates a packed backend with optional configuration.
func NewPackedBackendWithOptions(inner storage.Backend, blobDir string, threshold int64, opts PackedBackendOptions) (*PackedBackend, error) {
	var (
		bs  *BlobStore
		err error
	)
	if opts.Encryptor != nil {
		bs, err = NewEncryptedBlobStore(blobDir, 256*1024*1024, opts.Encryptor)
	} else {
		bs, err = NewBlobStore(blobDir, 256*1024*1024)
	}
	if err != nil {
		return nil, fmt.Errorf("create blob store: %w", err)
	}
	if opts.Compress {
		bs.EnableCompression()
	}

	pb := &PackedBackend{
		inner:     inner,
		blobStore: bs,
		blobDir:   blobDir,
		threshold: threshold,
		stopSave:  make(chan struct{}),
	}

	// Recover the in-memory index from disk (index.bin or, if missing,
	// rebuild via blob-file scan). Without this, packed objects PUT before
	// a restart become invisible to LIST/GET — the in-memory index starts
	// empty and PackedBackend.{ListObjectsPage, HeadObject, GetObject} all
	// consult it as the authoritative reference to blob_*.bin contents.
	if err := pb.LoadIndex(); err != nil {
		_ = bs.Close()
		return nil, fmt.Errorf("load packed index: %w", err)
	}

	go pb.periodicSave(30 * time.Second)

	return pb, nil
}

// periodicSave calls SaveIndex on a fixed interval until Close() is called.
// This limits index data loss to at most one interval in crash (kill -9) scenarios.
func (pb *PackedBackend) periodicSave(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := pb.SaveIndex(); err != nil {
				log.Warn().Err(err).Msg("periodic index save failed")
			}
		case <-pb.stopSave:
			return
		}
	}
}

// Close stops the periodic save goroutine, persists the index, then closes the blob store.
func (pb *PackedBackend) Close() error {
	close(pb.stopSave)
	if err := pb.SaveIndex(); err != nil {
		log.Warn().Err(err).Msg("failed to save packed blob index on close")
	}
	return pb.blobStore.Close()
}

func (pb *PackedBackend) Unwrap() storage.Backend { return pb.inner }

func (pb *PackedBackend) bucketHasPackedObjects(bucket string) bool {
	found := false
	pb.index.Range(func(k, v any) bool {
		if k.(packedKey).bucket == bucket && v.(*indexEntry).Refcount.Load() > 0 {
			found = true
			return false
		}
		return true
	})
	return found
}

func (pb *PackedBackend) deleteBucketIndex(bucket string) {
	pb.index.Range(func(k, _ any) bool {
		if k.(packedKey).bucket == bucket {
			pb.index.Delete(k)
		}
		return true
	})
}

// SaveIndex persists the in-memory index to {blobDir}/index.bin (FlatBuffers).
// Range produces a weakly-consistent snapshot; entries inserted or deleted
// concurrently may or may not appear, which is acceptable for the
// periodic on-disk checkpoint (recovery rebuilds from blob scan if the
// snapshot is incomplete).
func (pb *PackedBackend) SaveIndex() error {
	m := make(map[packedKey]*indexEntry)
	pb.index.Range(func(k, v any) bool {
		pk := k.(packedKey)
		e := v.(*indexEntry)
		copyE := &indexEntry{
			Location:     e.Location,
			OriginalSize: e.OriginalSize,
			ContentType:  e.ContentType,
			ETag:         e.ETag,
			LastModified: e.LastModified,
			UserMetadata: cloneStringMap(e.UserMetadata),
			SSEAlgorithm: e.SSEAlgorithm,
		}
		copyE.Refcount.Store(e.Refcount.Load())
		m[pk] = copyE
		return true
	})

	data, err := encodeIndex(m)
	if err != nil {
		return fmt.Errorf("encode index: %w", err)
	}
	tmp := filepath.Join(pb.blobDir, "index.bin.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write index tmp: %w", err)
	}
	return os.Rename(tmp, filepath.Join(pb.blobDir, "index.bin"))
}

// LoadIndex loads the index from {blobDir}/index.bin.
// If the file doesn't exist, rebuilds via blob scan (existing fallback).
// Malformed FB bytes surface as a wrapped error via defer-recover in
// decodeIndexStorage — fail-fast, no rebuild attempt on corrupt input.
func (pb *PackedBackend) LoadIndex() error {
	indexFile := filepath.Join(pb.blobDir, "index.bin")
	data, err := os.ReadFile(indexFile)
	if err == nil {
		entries, decErr := decodeIndexStorage(data)
		if decErr != nil {
			return fmt.Errorf("load index: %w", decErr)
		}
		for pk, e := range entries {
			pb.index.Store(pk, e)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("read index file: %w", err)
	}

	// Rebuild from blob files (unchanged from JSON era).
	locs, err := pb.blobStore.ScanAll()
	if err != nil {
		return fmt.Errorf("scan blobs: %w", err)
	}
	for k, loc := range locs {
		pk, ok := parsePackedKey(k)
		if !ok {
			return fmt.Errorf("corrupt blob entry key: %q has no bucket/key separator", k)
		}
		data, readErr := pb.blobStore.Read(loc)
		if readErr != nil {
			return fmt.Errorf("read rebuilt blob entry %s: %w", k, readErr)
		}
		h := md5.Sum(data)
		e := &indexEntry{
			Location:     loc,
			OriginalSize: int64(len(data)),
			ETag:         hex.EncodeToString(h[:]),
			LastModified: time.Now().Unix(),
		}
		e.Refcount.Store(1)
		pb.index.Store(pk, e)
	}
	return nil
}

// --- Bucket operations (pass through to inner) ---

func (pb *PackedBackend) CreateBucket(ctx context.Context, bucket string) error {
	return pb.inner.CreateBucket(ctx, bucket)
}

func (pb *PackedBackend) HeadBucket(ctx context.Context, bucket string) error {
	return pb.inner.HeadBucket(ctx, bucket)
}

func (pb *PackedBackend) DeleteBucket(ctx context.Context, bucket string) error {
	if err := pb.inner.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	if pb.bucketHasPackedObjects(bucket) {
		return storage.ErrBucketNotEmpty
	}
	return pb.inner.DeleteBucket(ctx, bucket)
}

func (pb *PackedBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := pb.inner.ForceDeleteBucket(ctx, bucket); err != nil {
		return err
	}
	pb.deleteBucketIndex(bucket)
	return nil
}

func (pb *PackedBackend) ListBuckets(ctx context.Context) ([]string, error) {
	return pb.inner.ListBuckets(ctx)
}

// --- Object operations ---

func (pb *PackedBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return pb.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

// AppendObject forwards S3 Express AppendObject to the inner backend (the
// packblob fast-path only handles small whole-object PUT/GET; appendable
// objects always live on the routed/cluster path).
func (pb *PackedBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	ap, ok := pb.inner.(storage.AppendObjecter)
	if !ok {
		return nil, storage.ErrAppendNotSupported
	}
	return ap.AppendObject(ctx, bucket, key, expectedOffset, r)
}

func (pb *PackedBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	return pb.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (pb *PackedBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	bucket, key := req.Bucket, req.Key
	if err := pb.inner.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	data, large, err := readPackedCandidate(req.Body, pb.threshold)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	// Large objects pass through to inner backend
	if large {
		req.Body = io.MultiReader(bytes.NewReader(data), req.Body)
		return putInnerWithRequest(ctx, pb.inner, req)
	}

	// Small object → pack into blob. The blob storage layer keys entries by
	// the legacy "bucket/key" string (its on-disk format), so we serialize
	// the tuple here. The in-memory index uses the typed packedKey below.
	pk := packedKey{bucket: bucket, key: key}
	loc, err := pb.blobStore.Append(pk.String(), data)
	if err != nil {
		return nil, fmt.Errorf("blob append: %w", err)
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	e := &indexEntry{
		Location:     loc,
		OriginalSize: int64(len(data)),
		ContentType:  req.ContentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: cloneStringMap(req.UserMetadata),
		SSEAlgorithm: req.SystemMetadata.SSEAlgorithm,
	}
	e.Refcount.Store(1)
	// Atomic publish: Swap returns the previous value if one was loaded.
	// Decrement displaced entry's refcount so blob scrubbing can reclaim
	// space when no live entry references it.
	if prev, loaded := pb.index.Swap(pk, e); loaded {
		prev.(*indexEntry).Refcount.Add(-1)
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  req.ContentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: cloneStringMap(req.UserMetadata),
		SSEAlgorithm: req.SystemMetadata.SSEAlgorithm,
	}, nil
}

func readPackedCandidate(r io.Reader, threshold int64) ([]byte, bool, error) {
	if threshold <= 0 {
		return nil, true, nil
	}
	maxInt := int64(int(^uint(0) >> 1))
	if threshold > maxInt {
		return nil, false, fmt.Errorf("packed threshold %d exceeds max int", threshold)
	}
	buf := make([]byte, int(threshold))
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil:
		return buf, true, nil
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], false, nil
	default:
		return nil, false, err
	}
}

func putInnerWithUserMetadata(ctx context.Context, inner storage.Backend, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	if len(userMetadata) == 0 {
		return inner.PutObject(ctx, bucket, key, r, contentType)
	}
	putter, ok := inner.(storage.UserMetadataPutter)
	if !ok {
		return nil, storage.UnsupportedOperationError{Op: "PutObjectWithUserMetadata", Reason: storage.UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
}

func putInnerWithRequest(ctx context.Context, inner storage.Backend, req storage.PutObjectRequest) (*storage.Object, error) {
	if req.ACL == nil && req.SystemMetadata.SSEAlgorithm == "" {
		return putInnerWithUserMetadata(ctx, inner, req.Bucket, req.Key, req.Body, req.ContentType, req.UserMetadata)
	}
	putter, ok := inner.(storage.RequestPutter)
	if !ok {
		return nil, storage.UnsupportedOperationError{Op: "PutObjectWithRequest", Reason: storage.UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithRequest(ctx, req)
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// packedReader is the io.ReadCloser returned by GetObject for packed (small)
// objects. It embeds bytes.Reader so a pooled instance can be reused across
// requests; Close releases the underlying data slice and returns the
// reader to packedReaderPool. Combining the reader and closer into a single
// pooled struct eliminates the io.NopCloser + bytes.NewReader allocation
// pair on every packed GetObject.
type packedReader struct {
	bytes.Reader
}

func (r *packedReader) Close() error {
	r.Reader.Reset(nil)
	packedReaderPool.Put(r)
	return nil
}

var packedReaderPool = sync.Pool{
	New: func() any { return new(packedReader) },
}

// newPackedReader returns a pooled *packedReader positioned at the start of
// data. The caller transfers ownership: Close releases the reader back to
// the pool, and reusing the returned reader after Close is undefined.
func newPackedReader(data []byte) *packedReader {
	r := packedReaderPool.Get().(*packedReader)
	r.Reader.Reset(data)
	return r
}

func packedObjectFromEntry(key string, entry *indexEntry, data []byte) *storage.Object {
	size := entry.OriginalSize
	if size == 0 && data != nil {
		size = int64(len(data))
	}
	etag := entry.ETag
	if etag == "" && data != nil {
		h := md5.Sum(data)
		etag = hex.EncodeToString(h[:])
	}
	modified := entry.LastModified
	if modified == 0 {
		modified = time.Now().Unix()
	}
	return &storage.Object{
		Key:          key,
		Size:         size,
		ContentType:  entry.ContentType,
		ETag:         etag,
		LastModified: modified,
		UserMetadata: cloneStringMap(entry.UserMetadata),
		SSEAlgorithm: entry.SSEAlgorithm,
	}
}

func (pb *PackedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	v, packed := pb.index.Load(packedKey{bucket: bucket, key: key})
	var entry *indexEntry
	if packed {
		entry = v.(*indexEntry)
	}

	if packed && entry.Refcount.Load() > 0 {
		data, err := pb.blobStore.Read(entry.Location)
		if err != nil {
			return nil, nil, fmt.Errorf("read packed object: %w", err)
		}

		obj := packedObjectFromEntry(key, entry, data)
		return newPackedReader(data), obj, nil
	}

	// Fall through to inner backend
	return pb.inner.GetObject(ctx, bucket, key)
}

func (pb *PackedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	if v, packed := pb.index.Load(packedKey{bucket: bucket, key: key}); packed {
		entry := v.(*indexEntry)
		if entry.Refcount.Load() > 0 {
			return packedObjectFromEntry(key, entry, nil), nil
		}
	}

	return pb.inner.HeadObject(ctx, bucket, key)
}

func (pb *PackedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	pk := packedKey{bucket: bucket, key: key}
	if v, loaded := pb.index.Load(pk); loaded {
		entry := v.(*indexEntry)
		if entry.Refcount.Add(-1) <= 0 {
			// Compare-and-delete only if the entry is still the same
			// pointer — protects against a concurrent Swap publishing a
			// new entry under the same key.
			pb.index.CompareAndDelete(pk, entry)
		}
		return nil
	}

	return pb.inner.DeleteObject(ctx, bucket, key)
}

func (pb *PackedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	// Get list from inner backend
	objects, err := pb.inner.ListObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		return nil, err
	}

	// Supplement with packed objects (weakly-consistent Range — listing
	// semantics tolerate concurrent inserts/deletes appearing or not).
	pb.index.Range(func(rk, rv any) bool {
		pk := rk.(packedKey)
		entry := rv.(*indexEntry)
		if entry.Refcount.Load() <= 0 {
			return true
		}
		if pk.bucket != bucket || !strings.HasPrefix(pk.key, prefix) {
			return true
		}
		obj := packedObjectFromEntry(pk.key, entry, nil)
		for _, o := range objects {
			if o.Key == pk.key {
				*o = *obj
				return true
			}
		}
		objects = append(objects, obj)
		return true
	})

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	return objects, nil
}

// ListObjectsPage mirrors ListObjects but honors a marker (S3 V1 Marker / V2
// ContinuationToken) and reports truncation. Required for
// Operations.ListObjectsPage's walk-and-find-pager logic — without it, the
// caller skips the packed-index supplementation and packed-only objects
// disappear from List on the single-node packblob fast path.
func (pb *PackedBackend) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	if maxKeys < 0 {
		maxKeys = 0
	}
	inner, innerTruncated, err := pb.innerListObjectsPage(ctx, bucket, prefix, marker, maxKeys+1)
	if err != nil {
		return nil, false, err
	}
	// Collect packed-index matches past marker. Sort happens after merge so
	// inner ordering and packed-only entries interleave correctly by key.
	packed := make(map[string]*storage.Object)
	pb.index.Range(func(rk, rv any) bool {
		pk := rk.(packedKey)
		entry := rv.(*indexEntry)
		if entry.Refcount.Load() <= 0 {
			return true
		}
		if pk.bucket != bucket || !strings.HasPrefix(pk.key, prefix) {
			return true
		}
		if marker != "" && pk.key <= marker {
			return true
		}
		packed[pk.key] = packedObjectFromEntry(pk.key, entry, nil)
		return true
	})

	// Merge: packed entries override inner when same key, plus packed-only.
	seen := make(map[string]bool, len(inner))
	merged := make([]*storage.Object, 0, len(inner)+len(packed))
	for _, o := range inner {
		if p, ok := packed[o.Key]; ok {
			merged = append(merged, p)
			seen[o.Key] = true
			continue
		}
		merged = append(merged, o)
	}
	for k, p := range packed {
		if !seen[k] {
			merged = append(merged, p)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })

	truncated := innerTruncated
	if maxKeys > 0 && len(merged) > maxKeys {
		merged = merged[:maxKeys]
		truncated = true
	}
	return merged, truncated, nil
}

// innerListObjectsPage prefers inner's native pager when available, falling
// back to ListObjects + in-process marker filtering for backends without a
// pager (PartialIO-only test stubs).
func (pb *PackedBackend) innerListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	type pager interface {
		ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error)
	}
	if p, ok := pb.inner.(pager); ok {
		return p.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
	}
	objects, err := pb.inner.ListObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		return nil, false, err
	}
	if marker == "" {
		return objects, false, nil
	}
	filtered := objects[:0]
	for _, o := range objects {
		if o.Key > marker {
			filtered = append(filtered, o)
		}
	}
	return filtered, false, nil
}

func (pb *PackedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	// Collect packed-index keys that match so we can fix sizes / emit extras.
	packed := make(map[string]*indexEntry)
	pb.index.Range(func(rk, rv any) bool {
		pk := rk.(packedKey)
		entry := rv.(*indexEntry)
		if entry.Refcount.Load() <= 0 {
			return true
		}
		if pk.bucket == bucket && strings.HasPrefix(pk.key, prefix) {
			packed[pk.key] = entry
		}
		return true
	})

	seen := make(map[string]bool)
	if err := pb.inner.WalkObjects(ctx, bucket, prefix, func(obj *storage.Object) error {
		seen[obj.Key] = true
		if entry, ok := packed[obj.Key]; ok {
			*obj = *packedObjectFromEntry(obj.Key, entry, nil)
		}
		return fn(obj)
	}); err != nil {
		return err
	}

	// Emit packed-only entries not found in inner.
	for k, entry := range packed {
		if seen[k] {
			continue
		}
		if err := fn(packedObjectFromEntry(k, entry, nil)); err != nil {
			return err
		}
	}
	return nil
}

// --- Copy operations (Copier interface) ---

// CopyObject performs a metadata-only copy for packed objects.
// For flat-file objects, it falls back to read+write.
func (pb *PackedBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*storage.Object, error) {
	ctx := context.Background()
	srcObj, err := pb.HeadObject(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	return pb.CopyObjectWithRequest(ctx, storage.CopyObjectAccelerationRequest{
		SourceRef:      storage.ObjectRef{Bucket: srcBucket, Key: srcKey},
		DestinationRef: storage.ObjectRef{Bucket: dstBucket, Key: dstKey},
		SourceObject:   srcObj,
		ContentType:    srcObj.ContentType,
		UserMetadata:   cloneStringMap(srcObj.UserMetadata),
	})
}

func (pb *PackedBackend) CopyObjectWithRequest(ctx context.Context, req storage.CopyObjectAccelerationRequest) (*storage.Object, error) {
	srcBucket, srcKey := req.SourceRef.Bucket, req.SourceRef.Key
	dstBucket, dstKey := req.DestinationRef.Bucket, req.DestinationRef.Key
	srcIKey := packedKey{bucket: srcBucket, key: srcKey}
	dstIKey := packedKey{bucket: dstBucket, key: dstKey}

	v, packed := pb.index.Load(srcIKey)
	if packed {
		srcEntry := v.(*indexEntry)
		if srcEntry.Refcount.Load() > 0 {
			loc := srcEntry.Location

			data, err := pb.blobStore.Read(loc)
			if err != nil {
				return nil, err
			}

			// Atomic refcount increment with liveness check. If a concurrent
			// DeleteObject drove Refcount to 0 (and may have removed the
			// index entry), the CAS fails and we abort. Guards against
			// saturation at MaxInt64-1.
			incremented := false
			for {
				cur := srcEntry.Refcount.Load()
				if cur <= 0 {
					return nil, fmt.Errorf("source object changed during copy")
				}
				if cur >= math.MaxInt64-1 {
					return nil, fmt.Errorf("refcount overflow: cannot copy object with maximum refcount")
				}
				if srcEntry.Refcount.CompareAndSwap(cur, cur+1) {
					incremented = true
					break
				}
			}

			// Re-validate that srcEntry is still the canonical entry for
			// srcIKey. A concurrent Put could have Swap'd a new entry
			// under the same key (with a different blob location); in
			// that case our incremented entry is no longer referenced by
			// the index and we must release our refcount.
			v2, stillPacked := pb.index.Load(srcIKey)
			if !stillPacked || v2.(*indexEntry) != srcEntry {
				if incremented {
					srcEntry.Refcount.Add(-1)
				}
				return nil, fmt.Errorf("source object changed during copy")
			}

			dst := &indexEntry{Location: loc}
			dst.Refcount.Store(1)
			dst.OriginalSize = srcEntry.OriginalSize
			dst.ContentType = req.ContentType
			dst.ETag = srcEntry.ETag
			dst.LastModified = time.Now().Unix()
			dst.UserMetadata = cloneStringMap(req.UserMetadata)
			if prev, loaded := pb.index.Swap(dstIKey, dst); loaded {
				prev.(*indexEntry).Refcount.Add(-1)
			}

			h := md5.Sum(data)
			return &storage.Object{
				Key:          dstKey,
				Size:         int64(len(data)),
				ContentType:  req.ContentType,
				ETag:         hex.EncodeToString(h[:]),
				LastModified: time.Now().Unix(),
				UserMetadata: cloneStringMap(req.UserMetadata),
			}, nil
		}
	}

	// Flat file fallback: read source, write to dest
	rc, obj, err := pb.inner.GetObject(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	contentType := req.ContentType
	if contentType == "" && obj != nil {
		contentType = obj.ContentType
	}
	return pb.PutObjectWithUserMetadata(ctx, dstBucket, dstKey, rc, contentType, req.UserMetadata)
}

// --- Multipart operations (pass through to inner) ---

func (pb *PackedBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return pb.inner.CreateMultipartUpload(ctx, bucket, key, contentType)
}

func (pb *PackedBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return pb.inner.UploadPart(ctx, bucket, key, uploadID, partNumber, r)
}

func (pb *PackedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return pb.inner.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (pb *PackedBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return pb.inner.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

func (pb *PackedBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	return pb.inner.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
}

func (pb *PackedBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	return pb.inner.ListParts(ctx, bucket, key, uploadID, maxParts)
}

func (pb *PackedBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	snap, ok := pb.inner.(storage.Snapshotable)
	if !ok {
		return nil, storage.ErrSnapshotNotSupported
	}
	objects, err := snap.ListAllObjects()
	if err != nil {
		return nil, err
	}
	seen := make(map[packedKey]struct{}, len(objects))
	for _, obj := range objects {
		seen[packedKey{bucket: obj.Bucket, key: obj.Key}] = struct{}{}
	}
	pb.index.Range(func(rk, rv any) bool {
		pk := rk.(packedKey)
		entry := rv.(*indexEntry)
		if entry.Refcount.Load() <= 0 {
			return true
		}
		if _, ok := seen[pk]; ok {
			return true
		}
		objects = append(objects, storage.SnapshotObject{
			Bucket:      pk.bucket,
			Key:         pk.key,
			ETag:        entry.ETag,
			Size:        entry.OriginalSize,
			ContentType: entry.ContentType,
			Modified:    entry.LastModified,
		})
		return true
	})
	return objects, nil
}

// RestoreObjects restores the packed in-memory index from surviving on-disk
// blob files for any snapshot entries whose (bucket,key) tuple is present in
// the blob store, and delegates remaining entries to the inner backend.
//
// Why the split: small objects PUT via the packblob fast path live ONLY in
// blob_*.bin (the cluster meta/EC path is bypassed at PutObject time). The
// previous behavior — wipe in-memory index, delegate everything to inner —
// orphaned packed data: LIST returned 0 because PackedBackend.ListObjectsPage
// supplements from the cleared index, while CC.ListObjectsPage reads the
// meta-FSM ObjectIndex which never received those PUTs either. The fix
// rebuilds the packed index from disk for entries the snapshot kept, so both
// LIST (via index supplementation) and GET (via blobStore.Read) recover.
//
// Stale on-disk entries (PUT after the snapshot point — present in
// blob_*.bin but not in the snapshot list) are not re-indexed. They become
// inaccessible to LIST/GET, with on-disk space recovered by background blob
// orphan sweep on a best-effort cadence (see compaction).
func (pb *PackedBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	snap, ok := pb.inner.(storage.Snapshotable)
	if !ok {
		return 0, nil, storage.ErrSnapshotNotSupported
	}

	diskLocs, err := pb.blobStore.ScanAll()
	if err != nil {
		return 0, nil, fmt.Errorf("packblob restore: scan blob store: %w", err)
	}
	diskByPK := make(map[packedKey]BlobLocation, len(diskLocs))
	for k, loc := range diskLocs {
		if pk, ok := parsePackedKey(k); ok {
			diskByPK[pk] = loc
		}
	}

	// Partition snapshot entries into packed-origin (surviving on disk) vs
	// cluster-origin (delegate to inner). The packed branch wins on collision
	// because that's where the data physically resides.
	type packedRestore struct {
		key packedKey
		loc BlobLocation
		obj storage.SnapshotObject
	}
	var clusterObjs []storage.SnapshotObject
	var packedRestores []packedRestore
	for _, obj := range objects {
		pk := packedKey{bucket: obj.Bucket, key: obj.Key}
		if loc, ok := diskByPK[pk]; ok {
			packedRestores = append(packedRestores, packedRestore{key: pk, loc: loc, obj: obj})
			continue
		}
		clusterObjs = append(clusterObjs, obj)
	}

	// Delegate non-packed entries first. Inner restore also reconciles its
	// own orphans (cluster objects no longer in the snapshot) before we
	// touch the packed index, so a mid-flight failure leaves the cluster
	// side consistent.
	restored, stale, err := snap.RestoreObjects(clusterObjs)
	if err != nil {
		return restored, stale, err
	}

	// Rebuild packed index. Existing in-memory entries are dropped first;
	// then snapshot-surviving packed entries are restored using metadata
	// from the snapshot (ETag/ContentType/Modified) rather than recomputing
	// from disk (cheaper, and preserves the snapshot's view of those fields).
	pb.index.Range(func(k, _ any) bool {
		pb.index.Delete(k)
		return true
	})
	for _, pr := range packedRestores {
		e := &indexEntry{
			Location:     pr.loc,
			OriginalSize: pr.obj.Size,
			ContentType:  pr.obj.ContentType,
			ETag:         pr.obj.ETag,
			LastModified: pr.obj.Modified,
			SSEAlgorithm: pr.obj.SSEAlgorithm,
		}
		e.Refcount.Store(1)
		pb.index.Store(pr.key, e)
		restored++
	}

	return restored, stale, nil
}

func (pb *PackedBackend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	snap, ok := pb.inner.(storage.BucketSnapshotable)
	if !ok {
		return nil, nil
	}
	return snap.ListAllBuckets()
}

func (pb *PackedBackend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	snap, ok := pb.inner.(storage.BucketSnapshotable)
	if !ok {
		return nil
	}
	return snap.RestoreBuckets(buckets)
}
