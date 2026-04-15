package erasure

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/storage"
)

// ECBackend implements storage.Backend with erasure coding.
// Objects are split into k data + m parity shards stored on disk.
// On read, missing shards (up to m) are reconstructed automatically.
type ECBackend struct {
	root      string
	db        *badger.DB
	codec     *Codec
	encryptor Encryptor
}

// Encryptor is an optional interface for at-rest encryption.
type Encryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(data []byte) ([]byte, error)
}

// ecObjectMeta is the metadata stored in BadgerDB for an EC-encoded object.
type ecObjectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
	DataShards   int
	ParityShards int
	ShardSize    int
}

// ECOption configures the EC backend.
type ECOption func(*ECBackend)

// WithEncryption enables at-rest encryption.
func WithEncryption(enc Encryptor) ECOption {
	return func(b *ECBackend) {
		b.encryptor = enc
	}
}

// NewECBackend creates a new erasure coding storage backend.
func NewECBackend(root string, dataShards, parityShards int, opts ...ECOption) (*ECBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbDir := filepath.Join(root, "meta")
	dbOpts := badger.DefaultOptions(dbDir).WithLogger(nil)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	b := &ECBackend{
		root:  root,
		db:    db,
		codec: NewCodec(dataShards, parityShards),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

// Close closes the metadata database.
func (b *ECBackend) Close() error {
	return b.db.Close()
}

// ShardDir returns the directory where shards for a given bucket/key are stored.
// Exported for testing (shard deletion simulation).
func (b *ECBackend) ShardDir(bucket, key string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(b.root, "data", bucket, ".ec", hex.EncodeToString(h[:]))
}

func (b *ECBackend) shardPath(bucket, key string, idx int) string {
	return filepath.Join(b.ShardDir(bucket, key), fmt.Sprintf("%02d", idx))
}

// bucketMeta stores per-bucket configuration.
type bucketMeta struct {
	ECEnabled bool
}

func bucketKey(bucket string) []byte { return []byte("bucket:" + bucket) }
func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }
func multipartKey(uploadID string) []byte     { return []byte("mpu:" + uploadID) }

// --- Bucket operations ---

func (b *ECBackend) CreateBucket(bucket string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		bk := bucketKey(bucket)
		_, err := txn.Get(bk)
		if err == nil {
			return storage.ErrBucketAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		bucketDir := filepath.Join(b.root, "data", bucket)
		if err := os.MkdirAll(bucketDir, 0o755); err != nil {
			return fmt.Errorf("create bucket dir: %w", err)
		}

		meta := bucketMeta{ECEnabled: true}
		metaBytes, err := marshalBucketMeta(&meta)
		if err != nil {
			return fmt.Errorf("marshal bucket meta: %w", err)
		}
		return txn.Set(bk, metaBytes)
	})
}

// SetBucketECPolicy sets the EC enabled/disabled flag for a bucket.
func (b *ECBackend) SetBucketECPolicy(bucket string, ecEnabled bool) error {
	return b.db.Update(func(txn *badger.Txn) error {
		bk := bucketKey(bucket)
		_, err := txn.Get(bk)
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}

		meta := bucketMeta{ECEnabled: ecEnabled}
		metaBytes, err := marshalBucketMeta(&meta)
		if err != nil {
			return fmt.Errorf("marshal bucket meta: %w", err)
		}
		return txn.Set(bk, metaBytes)
	})
}

// GetBucketECPolicy returns whether EC is enabled for a bucket.
func (b *ECBackend) GetBucketECPolicy(bucket string) (bool, error) {
	var meta bucketMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalBucketMeta(val)
			if err != nil {
				// Legacy bucket with no EC metadata: default to EC enabled
				meta.ECEnabled = true
				return nil
			}
			meta = *m
			return nil
		})
	})
	if err != nil {
		return false, err
	}
	return meta.ECEnabled, nil
}

func (b *ECBackend) HeadBucket(bucket string) error {
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	})
}

func (b *ECBackend) DeleteBucket(bucket string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		bk := bucketKey(bucket)
		_, err := txn.Get(bk)
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}

		prefix := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return storage.ErrBucketNotEmpty
		}

		bucketDir := filepath.Join(b.root, "data", bucket)
		if err := os.RemoveAll(bucketDir); err != nil {
			return fmt.Errorf("remove bucket dir: %w", err)
		}
		return txn.Delete(bk)
	})
}

func (b *ECBackend) ListBuckets() ([]string, error) {
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

// --- Object operations ---

func (b *ECBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	ecEnabled, err := b.GetBucketECPolicy(bucket)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	if !ecEnabled {
		return b.putObjectPlain(bucket, key, data, contentType)
	}
	return b.putObjectData(bucket, key, data, contentType)
}

// putObjectPlain stores an object without erasure coding (flat file).
func (b *ECBackend) putObjectPlain(bucket, key string, data []byte, contentType string) (*storage.Object, error) {
	objDir := filepath.Join(b.root, "data", bucket, ".plain")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		return nil, fmt.Errorf("create plain dir: %w", err)
	}

	h := sha256.Sum256([]byte(key))
	filePath := filepath.Join(objDir, hex.EncodeToString(h[:]))
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write plain object: %w", err)
	}

	etag := fmt.Sprintf("%x", md5.Sum(data))
	now := time.Now().Unix()

	meta := ecObjectMeta{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		DataShards:   0, // indicates plain storage
		ParityShards: 0,
	}

	metaBytes, err := marshalECObjectMeta(&meta)
	if err != nil {
		return nil, fmt.Errorf("marshal object meta: %w", err)
	}
	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), metaBytes)
	})
	if err != nil {
		os.Remove(filePath)
		return nil, err
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}, nil
}

func (b *ECBackend) putObjectData(bucket, key string, data []byte, contentType string) (*storage.Object, error) {
	// Empty or very small data: store as plain to avoid EC split errors
	if len(data) < b.codec.DataShards {
		return b.putObjectPlain(bucket, key, data, contentType)
	}

	// Erasure encode
	shards, err := b.codec.Encode(data)
	if err != nil {
		return nil, fmt.Errorf("erasure encode: %w", err)
	}

	// Write shards to disk
	shardDir := b.ShardDir(bucket, key)
	if err := os.MkdirAll(shardDir, 0o755); err != nil {
		return nil, fmt.Errorf("create shard dir: %w", err)
	}

	for i, shard := range shards {
		data := shard
		if b.encryptor != nil {
			encrypted, err := b.encryptor.Encrypt(shard)
			if err != nil {
				os.RemoveAll(shardDir)
				return nil, fmt.Errorf("encrypt shard %d: %w", i, err)
			}
			data = encrypted
		}
		path := b.shardPath(bucket, key, i)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			os.RemoveAll(shardDir)
			return nil, fmt.Errorf("write shard %d: %w", i, err)
		}
	}

	// Compute ETag from original data
	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	meta := ecObjectMeta{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		DataShards:   b.codec.DataShards,
		ParityShards: b.codec.ParityShards,
		ShardSize:    len(shards[0]),
	}

	metaBytes, err := marshalECObjectMeta(&meta)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), metaBytes)
	})
	if err != nil {
		os.RemoveAll(shardDir)
		return nil, err
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
	}, nil
}

func (b *ECBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	meta, err := b.getObjectMeta(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	var data []byte
	if meta.DataShards == 0 {
		data, err = b.readPlain(bucket, key)
	} else {
		data, err = b.readAndDecode(bucket, key, meta)
	}
	if err != nil {
		return nil, nil, err
	}

	obj := &storage.Object{
		Key:          meta.Key,
		Size:         meta.Size,
		ContentType:  meta.ContentType,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
	}
	return io.NopCloser(bytes.NewReader(data)), obj, nil
}

func (b *ECBackend) readPlain(bucket, key string) ([]byte, error) {
	h := sha256.Sum256([]byte(key))
	filePath := filepath.Join(b.root, "data", bucket, ".plain", hex.EncodeToString(h[:]))
	return os.ReadFile(filePath)
}

func (b *ECBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	meta, err := b.getObjectMeta(bucket, key)
	if err != nil {
		return nil, err
	}

	return &storage.Object{
		Key:          meta.Key,
		Size:         meta.Size,
		ContentType:  meta.ContentType,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
	}, nil
}

func (b *ECBackend) DeleteObject(bucket, key string) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}

	// Remove both EC shards and plain file (one will be a no-op)
	os.RemoveAll(b.ShardDir(bucket, key))
	h := sha256.Sum256([]byte(key))
	os.Remove(filepath.Join(b.root, "data", bucket, ".plain", hex.EncodeToString(h[:])))

	return b.db.Update(func(txn *badger.Txn) error {
		mk := objectMetaKey(bucket, key)
		_, err := txn.Get(mk)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return txn.Delete(mk)
	})
}

func (b *ECBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var objects []*storage.Object
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
			var meta *ecObjectMeta
			err := it.Item().Value(func(val []byte) error {
				var unmarshalErr error
				meta, unmarshalErr = unmarshalECObjectMeta(val)
				return unmarshalErr
			})
			if err != nil {
				return err
			}
			objects = append(objects, &storage.Object{
				Key:          meta.Key,
				Size:         meta.Size,
				ContentType:  meta.ContentType,
				ETag:         meta.ETag,
				LastModified: meta.LastModified,
			})
			count++
		}
		return nil
	})
	return objects, err
}

// --- Multipart operations ---

func (b *ECBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	uploadID := uuid.New().String()
	partDir := filepath.Join(b.root, "parts", uploadID)
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()
	mpMeta := ecMultipartMeta{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	}
	meta, err := marshalECMultipartMeta(&mpMeta)
	if err != nil {
		os.RemoveAll(partDir)
		return nil, fmt.Errorf("marshal multipart meta: %w", err)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(multipartKey(uploadID), meta)
	})
	if err != nil {
		os.RemoveAll(partDir)
		return nil, err
	}

	return &storage.MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	}, nil
}

func (b *ECBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	partDir := filepath.Join(b.root, "parts", uploadID)
	partFile := filepath.Join(partDir, fmt.Sprintf("%05d", partNumber))

	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h := md5.New()
	w := io.MultiWriter(f, h)
	size, err := io.Copy(w, r)
	f.Close()
	if err != nil {
		os.Remove(partFile)
		return nil, fmt.Errorf("write part: %w", err)
	}

	return &storage.Part{
		PartNumber: partNumber,
		ETag:       hex.EncodeToString(h.Sum(nil)),
		Size:       size,
	}, nil
}

func (b *ECBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	var uploadMeta *ecMultipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var unmarshalErr error
			uploadMeta, unmarshalErr = unmarshalECMultipartMeta(val)
			return unmarshalErr
		})
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Assemble parts into one buffer
	var assembled bytes.Buffer
	partDir := filepath.Join(b.root, "parts", uploadID)
	for _, p := range parts {
		partFile := filepath.Join(partDir, fmt.Sprintf("%05d", p.PartNumber))
		data, err := os.ReadFile(partFile)
		if err != nil {
			return nil, fmt.Errorf("read part %d: %w", p.PartNumber, err)
		}
		assembled.Write(data)
	}

	// Check bucket EC policy
	ecEnabled, ecErr := b.GetBucketECPolicy(bucket)
	if ecErr != nil {
		ecEnabled = true // default to EC on error
	}

	var obj *storage.Object
	if ecEnabled {
		obj, err = b.putObjectData(bucket, key, assembled.Bytes(), uploadMeta.ContentType)
	} else {
		obj, err = b.putObjectPlain(bucket, key, assembled.Bytes(), uploadMeta.ContentType)
	}
	if err != nil {
		return nil, err
	}

	// Cleanup parts and multipart metadata
	os.RemoveAll(partDir)
	_ = b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(multipartKey(uploadID))
	})

	return obj, nil
}

func (b *ECBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return err
	}

	partDir := filepath.Join(b.root, "parts", uploadID)
	os.RemoveAll(partDir)

	return b.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

// --- Internal helpers ---

func (b *ECBackend) getObjectMeta(bucket, key string) (*ecObjectMeta, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var meta *ecObjectMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey(bucket, key))
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var unmarshalErr error
			meta, unmarshalErr = unmarshalECObjectMeta(val)
			return unmarshalErr
		})
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}

func (b *ECBackend) readAndDecode(bucket, key string, meta *ecObjectMeta) ([]byte, error) {
	codec := NewCodec(meta.DataShards, meta.ParityShards)
	total := codec.TotalShards()

	shards := make([][]byte, total)
	for i := range total {
		path := b.shardPath(bucket, key, i)
		data, err := os.ReadFile(path)
		if err != nil {
			// Missing shard — leave nil for reconstruction
			shards[i] = nil
			continue
		}
		if b.encryptor != nil {
			decrypted, err := b.encryptor.Decrypt(data)
			if err != nil {
				// Corrupted shard — treat as missing
				shards[i] = nil
				continue
			}
			data = decrypted
		}
		shards[i] = data
	}

	return codec.Decode(shards, meta.Size)
}
