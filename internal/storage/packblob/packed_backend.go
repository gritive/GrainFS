package packblob

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// indexEntry tracks a small object stored in a blob file.
type indexEntry struct {
	Location BlobLocation
	Refcount int32
}

// PackedBackend wraps a storage.Backend and packs small objects into blob files.
// Objects below the threshold are stored in append-only blob files.
// Objects at or above the threshold pass through to the inner backend.
type PackedBackend struct {
	inner     storage.Backend
	blobStore *BlobStore
	threshold int64 // objects below this size go into blobs

	mu    sync.RWMutex
	index map[string]*indexEntry // "bucket/key" → blob location + refcount
}

// NewPackedBackend creates a packed backend wrapping inner.
// Objects smaller than threshold bytes are packed into blob files at blobDir.
func NewPackedBackend(inner storage.Backend, blobDir string, threshold int64) (*PackedBackend, error) {
	bs, err := NewBlobStore(blobDir, 256*1024*1024) // 256MB max blob
	if err != nil {
		return nil, fmt.Errorf("create blob store: %w", err)
	}

	return &PackedBackend{
		inner:     inner,
		blobStore: bs,
		threshold: threshold,
		index:     make(map[string]*indexEntry),
	}, nil
}

// Close closes the blob store.
func (pb *PackedBackend) Close() error {
	return pb.blobStore.Close()
}

func (pb *PackedBackend) indexKey(bucket, key string) string {
	return bucket + "/" + key
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

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	pb.mu.Lock()
	// If replacing an existing packed entry, decrement old refcount
	if old, ok := pb.index[ikey]; ok {
		old.Refcount--
	}
	pb.index[ikey] = &indexEntry{
		Location: loc,
		Refcount: 1,
	}
	pb.mu.Unlock()

	// Store metadata in inner backend's metadata store
	// We use a special marker object with zero-length body
	obj := &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}

	// Store metadata via inner PutObject (it will create a small marker file)
	// This keeps the metadata in BadgerDB consistent
	pb.inner.PutObject(bucket, key, bytes.NewReader(nil), contentType)

	return obj, nil
}

func (pb *PackedBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	ikey := pb.indexKey(bucket, key)

	pb.mu.RLock()
	entry, packed := pb.index[ikey]
	pb.mu.RUnlock()

	if packed && entry.Refcount > 0 {
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

	if packed && entry.Refcount > 0 {
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
		entry.Refcount--
		if entry.Refcount <= 0 {
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
		if entry.Refcount <= 0 {
			continue
		}
		if len(pfx) > 0 && len(ikey) >= len(pfx) && ikey[:len(pfx)] == pfx {
			// Extract the key part
			k := ikey[len(bucket)+1:]

			// Check if already in the list from inner backend
			found := false
			for _, o := range objects {
				if o.Key == k {
					found = true
					break
				}
			}
			if !found {
				objects = append(objects, &storage.Object{
					Key:  k,
					Size: int64(entry.Location.Length),
				})
			}
		}
	}

	if len(objects) > maxKeys {
		objects = objects[:maxKeys]
	}

	return objects, nil
}

// --- Copy operations (Copier interface) ---

// CopyObject performs a metadata-only copy for packed objects.
// For flat-file objects, it falls back to read+write.
func (pb *PackedBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*storage.Object, error) {
	srcIKey := pb.indexKey(srcBucket, srcKey)
	dstIKey := pb.indexKey(dstBucket, dstKey)

	pb.mu.Lock()
	srcEntry, packed := pb.index[srcIKey]
	if packed && srcEntry.Refcount > 0 {
		// Metadata-only copy: share the blob location, increment refcount
		srcEntry.Refcount++
		pb.index[dstIKey] = &indexEntry{
			Location: srcEntry.Location,
			Refcount: 1,
		}
		pb.mu.Unlock()

		// Read data to compute metadata
		data, err := pb.blobStore.Read(srcEntry.Location)
		if err != nil {
			return nil, err
		}

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
