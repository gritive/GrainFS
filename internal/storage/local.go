package storage

import (
	"crypto/md5"
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
)

// LocalBackend stores objects as flat files on disk with BadgerDB for metadata.
type LocalBackend struct {
	root string
	db   *badger.DB
}

// DB exposes the underlying BadgerDB for shared use (lifecycle, events).
func (b *LocalBackend) DB() *badger.DB { return b.db }

// NewLocalBackend creates a new local storage backend.
func NewLocalBackend(root string) (*LocalBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbDir := filepath.Join(root, "meta")
	opts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	return &LocalBackend{root: root, db: db}, nil
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

func (b *LocalBackend) CreateBucket(bucket string) error {
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
		return txn.Set(bk, []byte(`{}`))
	})
}

func (b *LocalBackend) HeadBucket(bucket string) error {
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		}
		return err
	})
}

func (b *LocalBackend) DeleteBucket(bucket string) error {
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
		return txn.Delete(bk)
	})
}

func (b *LocalBackend) ListBuckets() ([]string, error) {
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

func (b *LocalBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	f, err := os.Create(objPath)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	h := md5.New()
	w := io.MultiWriter(f, h)
	size, err := io.Copy(w, r)
	f.Close()
	if err != nil {
		os.Remove(objPath)
		return nil, fmt.Errorf("write object: %w", err)
	}

	etag := hex.EncodeToString(h.Sum(nil))
	now := time.Now().Unix()

	obj := &Object{
		Key:          key,
		Size:         size,
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}

	meta, err := marshalObject(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.objectMetaKey(bucket, key), meta)
	})
	if err != nil {
		return nil, err
	}

	return obj, nil
}

func (b *LocalBackend) GetObject(bucket, key string) (io.ReadCloser, *Object, error) {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Open(b.objectPath(bucket, key))
	if err != nil {
		return nil, nil, fmt.Errorf("open object: %w", err)
	}

	return f, obj, nil
}

func (b *LocalBackend) HeadObject(bucket, key string) (*Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var obj Object
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.objectMetaKey(bucket, key))
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			decoded, err := unmarshalObject(val)
			if err != nil {
				return err
			}
			obj = *decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// SetObjectACL satisfies storage.ACLSetter. Updates the ACL on the stored object metadata.
func (b *LocalBackend) SetObjectACL(bucket, key string, acl uint8) error {
	mk := b.objectMetaKey(bucket, key)
	return b.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(mk)
		if err == badger.ErrKeyNotFound {
			return ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			obj, merr := unmarshalObject(val)
			if merr != nil {
				return merr
			}
			obj.ACL = acl
			newVal, merr := marshalObject(obj)
			if merr != nil {
				return merr
			}
			return txn.Set(mk, newVal)
		})
	})
}

func (b *LocalBackend) DeleteObject(bucket, key string) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}

	os.Remove(b.objectPath(bucket, key))

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

func (b *LocalBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
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
			var obj Object
			err := it.Item().Value(func(val []byte) error {
				decoded, err := unmarshalObject(val)
				if err != nil {
					return err
				}
				obj = *decoded
				return nil
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

func (b *LocalBackend) WalkObjects(bucket, prefix string, fn func(*Object) error) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		pfx := []byte("obj:" + bucket + "/" + prefix)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			var obj Object
			if err := it.Item().Value(func(val []byte) error {
				decoded, err := unmarshalObject(val)
				if err != nil {
					return err
				}
				obj = *decoded
				return nil
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

// CopyObject copies an object by reading the source and writing to the destination.
func (b *LocalBackend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error) {
	rc, obj, err := b.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return b.PutObject(dstBucket, dstKey, rc, obj.ContentType)
}

func (b *LocalBackend) policyKey(bucket string) []byte {
	return []byte("policy:" + bucket)
}

// GetBucketPolicy returns the raw policy JSON for a bucket.
func (b *LocalBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	var data []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.policyKey(bucket))
		if err == badger.ErrKeyNotFound {
			return ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})
	return data, err
}

// SetBucketPolicy stores the raw policy JSON for a bucket.
func (b *LocalBackend) SetBucketPolicy(bucket string, policyJSON []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.policyKey(bucket), policyJSON)
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

			var obj Object
			if err := it.Item().Value(func(val []byte) error {
				decoded, err := unmarshalObject(val)
				if err != nil {
					return err
				}
				obj = *decoded
				return nil
			}); err != nil {
				return err
			}
			objs = append(objs, SnapshotObject{
				Bucket:      bucket,
				Key:         key,
				ETag:        obj.ETag,
				Size:        obj.Size,
				ContentType: obj.ContentType,
				Modified:    obj.LastModified,
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
	for _, snap := range objects {
		// Ensure bucket exists
		if err := b.CreateBucket(snap.Bucket); err != nil && !errors.Is(err, ErrBucketAlreadyExists) {
			return count, stale, fmt.Errorf("ensure bucket %s: %w", snap.Bucket, err)
		}
		// Check blob exists on disk
		if _, err := os.Stat(b.objectPath(snap.Bucket, snap.Key)); os.IsNotExist(err) {
			stale = append(stale, StaleBlob{Bucket: snap.Bucket, Key: snap.Key, ExpectedETag: snap.ETag})
			continue
		}
		// Restore metadata
		obj := &Object{Key: snap.Key, Size: snap.Size, ContentType: snap.ContentType, ETag: snap.ETag, LastModified: snap.Modified}
		meta, err := marshalObject(obj)
		if err != nil {
			return count, stale, fmt.Errorf("marshal %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		if err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(b.objectMetaKey(snap.Bucket, snap.Key), meta)
		}); err != nil {
			return count, stale, fmt.Errorf("restore %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++
	}
	return count, stale, nil
}
