package packblob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
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
}

// indexEntryJSON is a serializable form of indexEntry.
type indexEntryJSON struct {
	Location     BlobLocation      `json:"location"`
	Refcount     int64             `json:"refcount"`
	OriginalSize int64             `json:"original_size"`
	ContentType  string            `json:"content_type,omitempty"`
	ETag         string            `json:"etag,omitempty"`
	LastModified int64             `json:"last_modified,omitempty"`
	UserMetadata map[string]string `json:"user_metadata,omitempty"`
}

// PackedBackend wraps a storage.Backend and packs small objects into blob files.
// Objects below the threshold are stored in append-only blob files.
// Objects at or above the threshold pass through to the inner backend.
type PackedBackend struct {
	inner     storage.Backend
	blobStore *BlobStore
	blobDir   string
	threshold int64 // objects below this size go into blobs

	mu    sync.RWMutex
	index map[string]*indexEntry // "bucket/key" → blob location + refcount

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
		index:     make(map[string]*indexEntry),
		stopSave:  make(chan struct{}),
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

func (pb *PackedBackend) indexKey(bucket, key string) string {
	return bucket + "/" + key
}

func (pb *PackedBackend) bucketHasPackedObjectsLocked(bucket string) bool {
	pfx := bucket + "/"
	for ikey, entry := range pb.index {
		if strings.HasPrefix(ikey, pfx) && entry.Refcount.Load() > 0 {
			return true
		}
	}
	return false
}

func (pb *PackedBackend) deleteBucketIndexLocked(bucket string) {
	pfx := bucket + "/"
	for ikey := range pb.index {
		if strings.HasPrefix(ikey, pfx) {
			delete(pb.index, ikey)
		}
	}
}

// SaveIndex persists the in-memory index to {blobDir}/index.json.
func (pb *PackedBackend) SaveIndex() error {
	pb.mu.RLock()
	m := make(map[string]indexEntryJSON, len(pb.index))
	for k, e := range pb.index {
		m[k] = indexEntryJSON{
			Location:     e.Location,
			Refcount:     e.Refcount.Load(),
			OriginalSize: e.OriginalSize,
			ContentType:  e.ContentType,
			ETag:         e.ETag,
			LastModified: e.LastModified,
			UserMetadata: cloneStringMap(e.UserMetadata),
		}
	}
	pb.mu.RUnlock()

	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal index: %w", err)
	}
	// Write to temp file then rename for atomic update (prevents corrupt index on crash).
	tmp := filepath.Join(pb.blobDir, "index.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write index tmp: %w", err)
	}
	return os.Rename(tmp, filepath.Join(pb.blobDir, "index.json"))
}

// LoadIndex loads the index from {blobDir}/index.json.
// If the file doesn't exist, it rebuilds the index by scanning all blob files.
func (pb *PackedBackend) LoadIndex() error {
	indexFile := filepath.Join(pb.blobDir, "index.json")
	data, err := os.ReadFile(indexFile)
	if err == nil {
		var m map[string]indexEntryJSON
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("unmarshal index: %w", err)
		}
		pb.mu.Lock()
		for k, v := range m {
			e := &indexEntry{
				Location:     v.Location,
				OriginalSize: v.OriginalSize,
				ContentType:  v.ContentType,
				ETag:         v.ETag,
				LastModified: v.LastModified,
				UserMetadata: cloneStringMap(v.UserMetadata),
			}
			e.Refcount.Store(v.Refcount)
			pb.index[k] = e
		}
		pb.mu.Unlock()
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("read index file: %w", err)
	}

	// Rebuild from blob files
	locs, err := pb.blobStore.ScanAll()
	if err != nil {
		return fmt.Errorf("scan blobs: %w", err)
	}
	pb.mu.Lock()
	for k, loc := range locs {
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
		pb.index[k] = e
	}
	pb.mu.Unlock()
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
	pb.mu.RLock()
	hasPacked := pb.bucketHasPackedObjectsLocked(bucket)
	pb.mu.RUnlock()
	if hasPacked {
		return storage.ErrBucketNotEmpty
	}
	return pb.inner.DeleteBucket(ctx, bucket)
}

func (pb *PackedBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := pb.inner.ForceDeleteBucket(ctx, bucket); err != nil {
		return err
	}
	pb.mu.Lock()
	pb.deleteBucketIndexLocked(bucket)
	pb.mu.Unlock()
	return nil
}

func (pb *PackedBackend) ListBuckets(ctx context.Context) ([]string, error) {
	return pb.inner.ListBuckets(ctx)
}

// --- Object operations ---

func (pb *PackedBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return pb.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (pb *PackedBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	if err := pb.inner.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	// Read all data to determine size
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	// Large objects pass through to inner backend
	if int64(len(data)) >= pb.threshold {
		return putInnerWithUserMetadata(ctx, pb.inner, bucket, key, bytes.NewReader(data), contentType, userMetadata)
	}

	// Small object → pack into blob
	ikey := pb.indexKey(bucket, key)
	loc, err := pb.blobStore.Append(ikey, data)
	if err != nil {
		return nil, fmt.Errorf("blob append: %w", err)
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	pb.mu.Lock()
	// If replacing an existing packed entry, decrement old refcount
	if old, ok := pb.index[ikey]; ok {
		old.Refcount.Add(-1)
	}
	e := &indexEntry{
		Location:     loc,
		OriginalSize: int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: cloneStringMap(userMetadata),
	}
	e.Refcount.Store(1)
	pb.index[ikey] = e
	pb.mu.Unlock()

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: cloneStringMap(userMetadata),
	}, nil
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
	}
}

func (pb *PackedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	ikey := pb.indexKey(bucket, key)

	pb.mu.RLock()
	entry, packed := pb.index[ikey]
	pb.mu.RUnlock()

	if packed && entry.Refcount.Load() > 0 {
		data, err := pb.blobStore.Read(entry.Location)
		if err != nil {
			return nil, nil, fmt.Errorf("read packed object: %w", err)
		}

		obj := packedObjectFromEntry(key, entry, data)
		return io.NopCloser(bytes.NewReader(data)), obj, nil
	}

	// Fall through to inner backend
	return pb.inner.GetObject(ctx, bucket, key)
}

func (pb *PackedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	ikey := pb.indexKey(bucket, key)

	pb.mu.RLock()
	entry, packed := pb.index[ikey]
	pb.mu.RUnlock()

	if packed && entry.Refcount.Load() > 0 {
		return packedObjectFromEntry(key, entry, nil), nil
	}

	return pb.inner.HeadObject(ctx, bucket, key)
}

func (pb *PackedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	ikey := pb.indexKey(bucket, key)

	pb.mu.Lock()
	if entry, ok := pb.index[ikey]; ok {
		if entry.Refcount.Add(-1) <= 0 {
			delete(pb.index, ikey)
		}
		pb.mu.Unlock()
		return nil
	}
	pb.mu.Unlock()

	return pb.inner.DeleteObject(ctx, bucket, key)
}

func (pb *PackedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	// Get list from inner backend
	objects, err := pb.inner.ListObjects(ctx, bucket, prefix, maxKeys)
	if err != nil {
		return nil, err
	}

	// Supplement with packed objects
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	pfx := bucket + "/" + prefix
	for ikey, entry := range pb.index {
		if entry.Refcount.Load() <= 0 {
			continue
		}
		if len(ikey) >= len(pfx) && ikey[:len(pfx)] == pfx {
			// Extract the key part
			k := ikey[len(bucket)+1:]

			obj := packedObjectFromEntry(k, entry, nil)
			found := false
			for _, o := range objects {
				if o.Key == k {
					*o = *obj
					found = true
					break
				}
			}
			if !found {
				objects = append(objects, obj)
			}
		}
	}

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	return objects, nil
}

func (pb *PackedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	// Collect packed-index keys that match so we can fix sizes / emit extras.
	pb.mu.RLock()
	pfx := bucket + "/" + prefix
	packed := make(map[string]*indexEntry)
	for ikey, entry := range pb.index {
		if entry.Refcount.Load() <= 0 {
			continue
		}
		if len(ikey) >= len(pfx) && ikey[:len(pfx)] == pfx {
			k := ikey[len(bucket)+1:]
			packed[k] = entry
		}
	}
	pb.mu.RUnlock()

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
	srcIKey := pb.indexKey(srcBucket, srcKey)
	dstIKey := pb.indexKey(dstBucket, dstKey)

	pb.mu.Lock()
	srcEntry, packed := pb.index[srcIKey]
	if packed && srcEntry.Refcount.Load() > 0 {
		// Check for overflow before incrementing (guard at MaxInt64-1 to prevent saturation)
		if srcEntry.Refcount.Load() >= math.MaxInt64-1 {
			pb.mu.Unlock()
			return nil, fmt.Errorf("refcount overflow: cannot copy object with maximum refcount")
		}

		// Read data before mutating index so a concurrent delete can't create a ghost entry.
		loc := srcEntry.Location
		pb.mu.Unlock()

		data, err := pb.blobStore.Read(loc)
		if err != nil {
			return nil, err
		}

		pb.mu.Lock()
		// Re-check after read: source may have been deleted while we were reading.
		srcEntry, stillPacked := pb.index[srcIKey]
		if !stillPacked || srcEntry.Refcount.Load() <= 0 || srcEntry.Location != loc {
			pb.mu.Unlock()
			return nil, fmt.Errorf("source object changed during copy")
		}
		srcEntry.Refcount.Add(1)
		oldDst := pb.index[dstIKey]
		if oldDst != nil {
			oldDst.Refcount.Add(-1)
		}
		dst := &indexEntry{Location: loc}
		dst.Refcount.Store(1)
		dst.OriginalSize = srcEntry.OriginalSize
		dst.ContentType = req.ContentType
		dst.ETag = srcEntry.ETag
		dst.LastModified = time.Now().Unix()
		dst.UserMetadata = cloneStringMap(req.UserMetadata)
		pb.index[dstIKey] = dst
		pb.mu.Unlock()

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
	pb.mu.Unlock()

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
	seen := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		seen[pb.indexKey(obj.Bucket, obj.Key)] = struct{}{}
	}
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	for ikey, entry := range pb.index {
		if entry.Refcount.Load() <= 0 {
			continue
		}
		if _, ok := seen[ikey]; ok {
			continue
		}
		bucket, key, ok := strings.Cut(ikey, "/")
		if !ok {
			continue
		}
		objects = append(objects, storage.SnapshotObject{
			Bucket:      bucket,
			Key:         key,
			ETag:        entry.ETag,
			Size:        entry.OriginalSize,
			ContentType: entry.ContentType,
			Modified:    entry.LastModified,
		})
	}
	return objects, nil
}

func (pb *PackedBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	snap, ok := pb.inner.(storage.Snapshotable)
	if !ok {
		return 0, nil, storage.ErrSnapshotNotSupported
	}
	pb.mu.Lock()
	pb.index = make(map[string]*indexEntry)
	pb.mu.Unlock()
	return snap.RestoreObjects(objects)
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
