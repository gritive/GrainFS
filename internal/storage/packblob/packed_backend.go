package packblob

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// indexEntry tracks a small object stored in a blob file.
type indexEntry struct {
	Location     BlobLocation
	Refcount     atomic.Int64
	OriginalSize int64 // uncompressed byte count
}

// indexEntryJSON is a serializable form of indexEntry.
type indexEntryJSON struct {
	Location     BlobLocation `json:"location"`
	Refcount     int64        `json:"refcount"`
	OriginalSize int64        `json:"original_size"`
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

// PackedBackendOptions configures optional behavior for PackedBackend.
type PackedBackendOptions struct {
	Compress bool // enable zstd compression for packed (small) objects
}

// NewPackedBackend creates a packed backend wrapping inner.
// Objects smaller than threshold bytes are packed into blob files at blobDir.
// Compression is enabled by default; use NewPackedBackendWithOptions to opt out.
func NewPackedBackend(inner storage.Backend, blobDir string, threshold int64) (*PackedBackend, error) {
	return NewPackedBackendWithOptions(inner, blobDir, threshold, PackedBackendOptions{Compress: true})
}

// NewPackedBackendWithOptions creates a packed backend with optional configuration.
func NewPackedBackendWithOptions(inner storage.Backend, blobDir string, threshold int64, opts PackedBackendOptions) (*PackedBackend, error) {
	bs, err := NewBlobStore(blobDir, 256*1024*1024) // 256MB max blob
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

func (pb *PackedBackend) indexKey(bucket, key string) string {
	return bucket + "/" + key
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
			e := &indexEntry{Location: v.Location, OriginalSize: v.OriginalSize}
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
		e := &indexEntry{Location: loc}
		e.Refcount.Store(1)
		pb.index[k] = e
	}
	pb.mu.Unlock()
	return nil
}

// --- Bucket operations (pass through to inner) ---

func (pb *PackedBackend) CreateBucket(bucket string) error {
	return pb.inner.CreateBucket(bucket)
}

func (pb *PackedBackend) HeadBucket(bucket string) error {
	return pb.inner.HeadBucket(bucket)
}

func (pb *PackedBackend) DeleteBucket(bucket string) error {
	return pb.inner.DeleteBucket(bucket)
}

func (pb *PackedBackend) ListBuckets() ([]string, error) {
	return pb.inner.ListBuckets()
}

// --- Object operations ---

func (pb *PackedBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	if err := pb.inner.HeadBucket(bucket); err != nil {
		return nil, err
	}

	// Read all data to determine size
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	// Large objects pass through to inner backend
	if int64(len(data)) >= pb.threshold {
		return pb.inner.PutObject(bucket, key, bytes.NewReader(data), contentType)
	}

	// Small object → pack into blob
	ikey := pb.indexKey(bucket, key)
	loc, err := pb.blobStore.Append(ikey, data)
	if err != nil {
		return nil, fmt.Errorf("blob append: %w", err)
	}

	var etag string
	if !storage.IsInternalBucket(bucket) {
		h := md5.Sum(data)
		etag = hex.EncodeToString(h[:])
	}
	now := time.Now().Unix()

	pb.mu.Lock()
	// If replacing an existing packed entry, decrement old refcount
	if old, ok := pb.index[ikey]; ok {
		old.Refcount.Add(-1)
	}
	e := &indexEntry{Location: loc, OriginalSize: int64(len(data))}
	e.Refcount.Store(1)
	pb.index[ikey] = e
	pb.mu.Unlock()

	// Store metadata via inner PutObject (it will create a small marker file)
	// This keeps the metadata in BadgerDB consistent
	if _, err := pb.inner.PutObject(bucket, key, bytes.NewReader(nil), contentType); err != nil {
		return nil, fmt.Errorf("sync metadata: %w", err)
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}, nil
}

func (pb *PackedBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	ikey := pb.indexKey(bucket, key)

	pb.mu.RLock()
	entry, packed := pb.index[ikey]
	pb.mu.RUnlock()

	if packed && entry.Refcount.Load() > 0 {
		data, err := pb.blobStore.Read(entry.Location)
		if err != nil {
			return nil, nil, fmt.Errorf("read packed object: %w", err)
		}

		h := md5.Sum(data)
		obj := &storage.Object{
			Key:          key,
			Size:         int64(len(data)),
			ETag:         hex.EncodeToString(h[:]),
			LastModified: time.Now().Unix(),
		}
		return io.NopCloser(bytes.NewReader(data)), obj, nil
	}

	// Fall through to inner backend
	return pb.inner.GetObject(bucket, key)
}

func (pb *PackedBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	ikey := pb.indexKey(bucket, key)

	pb.mu.RLock()
	entry, packed := pb.index[ikey]
	pb.mu.RUnlock()

	if packed && entry.Refcount.Load() > 0 {
		data, err := pb.blobStore.Read(entry.Location)
		if err != nil {
			return nil, fmt.Errorf("read packed object: %w", err)
		}

		h := md5.Sum(data)
		return &storage.Object{
			Key:          key,
			Size:         int64(len(data)),
			ETag:         hex.EncodeToString(h[:]),
			LastModified: time.Now().Unix(),
		}, nil
	}

	return pb.inner.HeadObject(bucket, key)
}

func (pb *PackedBackend) DeleteObject(bucket, key string) error {
	ikey := pb.indexKey(bucket, key)

	pb.mu.Lock()
	if entry, ok := pb.index[ikey]; ok {
		if entry.Refcount.Add(-1) <= 0 {
			delete(pb.index, ikey)
		}
		pb.mu.Unlock()
		// Also remove from inner backend metadata
		pb.inner.DeleteObject(bucket, key)
		return nil
	}
	pb.mu.Unlock()

	return pb.inner.DeleteObject(bucket, key)
}

func (pb *PackedBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	// Get list from inner backend
	objects, err := pb.inner.ListObjects(bucket, prefix, maxKeys)
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

			// Update or add: packed objects have zero-byte markers in the inner backend,
			// so we must replace the inner-reported size with the actual original size.
			found := false
			for _, o := range objects {
				if o.Key == k {
					o.Size = entry.OriginalSize
					found = true
					break
				}
			}
			if !found {
				objects = append(objects, &storage.Object{
					Key:  k,
					Size: entry.OriginalSize,
				})
			}
		}
	}

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	return objects, nil
}

func (pb *PackedBackend) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	// Collect packed-index keys that match so we can fix sizes / emit extras.
	pb.mu.RLock()
	pfx := bucket + "/" + prefix
	packed := make(map[string]int64) // key → OriginalSize
	for ikey, entry := range pb.index {
		if entry.Refcount.Load() <= 0 {
			continue
		}
		if len(ikey) >= len(pfx) && ikey[:len(pfx)] == pfx {
			k := ikey[len(bucket)+1:]
			packed[k] = entry.OriginalSize
		}
	}
	pb.mu.RUnlock()

	seen := make(map[string]bool)
	if err := pb.inner.WalkObjects(bucket, prefix, func(obj *storage.Object) error {
		seen[obj.Key] = true
		if sz, ok := packed[obj.Key]; ok {
			obj.Size = sz
		}
		return fn(obj)
	}); err != nil {
		return err
	}

	// Emit packed-only entries not found in inner.
	for k, sz := range packed {
		if seen[k] {
			continue
		}
		if err := fn(&storage.Object{Key: k, Size: sz}); err != nil {
			return err
		}
	}
	return nil
}

// --- Copy operations (Copier interface) ---

// CopyObject performs a metadata-only copy for packed objects.
// For flat-file objects, it falls back to read+write.
func (pb *PackedBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*storage.Object, error) {
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
		if !stillPacked || srcEntry.Refcount.Load() <= 0 {
			pb.mu.Unlock()
			return nil, fmt.Errorf("source object deleted during copy")
		}
		srcEntry.Refcount.Add(1)
		dst := &indexEntry{Location: loc}
		dst.Refcount.Store(1)
		pb.index[dstIKey] = dst
		pb.mu.Unlock()

		h := md5.Sum(data)
		return &storage.Object{
			Key:          dstKey,
			Size:         int64(len(data)),
			ETag:         hex.EncodeToString(h[:]),
			LastModified: time.Now().Unix(),
		}, nil
	}
	pb.mu.Unlock()

	// Flat file fallback: read source, write to dest
	rc, obj, err := pb.inner.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return pb.PutObject(dstBucket, dstKey, rc, obj.ContentType)
}

// --- Multipart operations (pass through to inner) ---

func (pb *PackedBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return pb.inner.CreateMultipartUpload(bucket, key, contentType)
}

func (pb *PackedBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return pb.inner.UploadPart(bucket, key, uploadID, partNumber, r)
}

func (pb *PackedBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return pb.inner.CompleteMultipartUpload(bucket, key, uploadID, parts)
}

func (pb *PackedBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	return pb.inner.AbortMultipartUpload(bucket, key, uploadID)
}
