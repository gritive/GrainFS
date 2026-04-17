package erasure

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// ECBackend implements storage.Backend with erasure coding.
// Objects are split into k data + m parity shards stored on disk.
// On read, missing shards (up to m) are reconstructed automatically.
type ECBackend struct {
	root       string
	db         *badger.DB
	codec      *Codec
	encryptor  Encryptor
	writeLocks sync.Map // map[string]*sync.RWMutex for per-key serialization
}

func (b *ECBackend) acquireWriteLock(bucket, key string) func() {
	lockKey := bucket + "\x00" + key
	mu, _ := b.writeLocks.LoadOrStore(lockKey, &sync.RWMutex{})
	mu.(*sync.RWMutex).Lock()
	return func() { mu.(*sync.RWMutex).Unlock() }
}

func (b *ECBackend) acquireReadLock(bucket, key string) func() {
	lockKey := bucket + "\x00" + key
	mu, _ := b.writeLocks.LoadOrStore(lockKey, &sync.RWMutex{})
	mu.(*sync.RWMutex).RLock()
	return func() { mu.(*sync.RWMutex).RUnlock() }
}

// shardWithCRC appends a 4-byte CRC32-IEEE footer to data.
func shardWithCRC(data []byte) []byte {
	out := make([]byte, len(data)+4)
	copy(out, data)
	binary.LittleEndian.PutUint32(out[len(data):], crc32.ChecksumIEEE(data))
	return out
}

// stripVerifyCRC verifies the 4-byte CRC32-IEEE footer and returns payload.
func stripVerifyCRC(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("shard too short for CRC footer (%d bytes)", len(data))
	}
	payload := data[:len(data)-4]
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != stored {
		return nil, fmt.Errorf("CRC mismatch")
	}
	return payload, nil
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

	unlock := b.acquireWriteLock(bucket, key)
	defer unlock()

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
		data := shardWithCRC(shard) // CRC32 footer (Eng Review #3)
		if b.encryptor != nil {
			encrypted, err := b.encryptor.Encrypt(data)
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

// --- Scrubbable interface (for scrubber package) ---

// ObjectExists reports whether an object's metadata still exists in BadgerDB.
// Used by the scrubber to detect DeleteObject races (Eng Review #9).
func (b *ECBackend) ObjectExists(bucket, key string) (bool, error) {
	_, err := b.HeadObject(bucket, key)
	if err == storage.ErrObjectNotFound || err == storage.ErrBucketNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ScanObjects streams EC object records for a bucket (DataShards > 0 only).
// Scrubber API contract — do not change without bumping scrubber version.
func (b *ECBackend) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}
	ch := make(chan scrubber.ObjectRecord, 64)
	go func() {
		defer close(ch)
		_ = b.db.View(func(txn *badger.Txn) error {
			prefix := []byte("obj:" + bucket + "/")
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				var meta *ecObjectMeta
				if err := it.Item().Value(func(val []byte) error {
					var err error
					meta, err = unmarshalECObjectMeta(val)
					return err
				}); err != nil {
					continue
				}
				if meta.DataShards <= 0 { // plain objects skipped (Eng Review #10)
					continue
				}
				rawKey := string(it.Item().Key())
				key := strings.TrimPrefix(rawKey, "obj:"+bucket+"/")
				ch <- scrubber.ObjectRecord{
					Bucket:       bucket,
					Key:          key,
					DataShards:   meta.DataShards,
					ParityShards: meta.ParityShards,
					ETag:         meta.ETag,
				}
			}
			return nil
		})
	}()
	return ch, nil
}

// ShardPaths returns all shard file paths for an object.
// Scrubber API contract — do not change without bumping scrubber version.
func (b *ECBackend) ShardPaths(bucket, key string, totalShards int) []string {
	paths := make([]string, totalShards)
	for i := range paths {
		paths[i] = b.shardPath(bucket, key, i)
	}
	return paths
}

// ReadShard reads a shard, decrypts it (if encryption is enabled), and verifies
// the CRC32 footer. Uses a read lock to allow concurrent reads alongside client GETs.
func (b *ECBackend) ReadShard(bucket, key, path string) ([]byte, error) {
	unlock := b.acquireReadLock(bucket, key)
	defer unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if b.encryptor != nil {
		data, err = b.encryptor.Decrypt(data)
		if err != nil {
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
	}
	return stripVerifyCRC(data)
}

// WriteShard atomically writes a shard with CRC32 footer using tmp+fsync+rename
// to protect against partial writes on power loss (Eng Review #4).
// Uses an exclusive lock to prevent concurrent writes.
func (b *ECBackend) WriteShard(bucket, key, path string, data []byte) error {
	unlock := b.acquireWriteLock(bucket, key)
	defer unlock()

	toWrite := shardWithCRC(data)
	if b.encryptor != nil {
		var err error
		toWrite, err = b.encryptor.Encrypt(toWrite)
		if err != nil {
			return fmt.Errorf("encrypt shard: %w", err)
		}
	}

	// Crash-safe: write to .tmp, fsync, rename, then fsync parent dir
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	if _, err := f.Write(toWrite); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write tmp shard: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync tmp shard: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename shard: %w", err)
	}

	// Fsync parent directory to persist the rename
	dir := filepath.Dir(path)
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		df.Close()
	}
	return nil
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

// ListAllObjects implements storage.Snapshotable: scans all object metadata across all buckets.
func (b *ECBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	var objs []storage.SnapshotObject
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

			var meta *ecObjectMeta
			if err := it.Item().Value(func(val []byte) error {
				var err error
				meta, err = unmarshalECObjectMeta(val)
				return err
			}); err != nil {
				return err
			}
			objs = append(objs, storage.SnapshotObject{
				Bucket:      bucket,
				Key:         key,
				ETag:        meta.ETag,
				Size:        meta.Size,
				ContentType: meta.ContentType,
				Modified:    meta.LastModified,
			})
		}
		return nil
	})
	return objs, err
}

// RestoreObjects implements storage.Snapshotable: replaces current object metadata with snapshot state.
func (b *ECBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
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

	var stale []storage.StaleBlob
	var count int
	for _, snap := range objects {
		// Ensure bucket exists
		if err := b.CreateBucket(snap.Bucket); err != nil && !strings.Contains(err.Error(), "already exists") {
			return count, stale, fmt.Errorf("ensure bucket %s: %w", snap.Bucket, err)
		}
		// Check shards exist
		shardDir := b.ShardDir(snap.Bucket, snap.Key)
		firstShard := filepath.Join(shardDir, "00")
		fi, err := os.Stat(firstShard)
		if err != nil {
			stale = append(stale, storage.StaleBlob{Bucket: snap.Bucket, Key: snap.Key, ExpectedETag: snap.ETag})
			continue
		}
		// Restore metadata with current codec settings and actual shard size
		meta := &ecObjectMeta{
			Key:          snap.Key,
			Size:         snap.Size,
			ContentType:  snap.ContentType,
			ETag:         snap.ETag,
			LastModified: snap.Modified,
			DataShards:   b.codec.DataShards,
			ParityShards: b.codec.ParityShards,
			ShardSize:    int(fi.Size()),
		}
		metaBytes, err := marshalECObjectMeta(meta)
		if err != nil {
			return count, stale, fmt.Errorf("marshal %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		if err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(objectMetaKey(snap.Bucket, snap.Key), metaBytes)
		}); err != nil {
			return count, stale, fmt.Errorf("restore %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++
	}
	return count, stale, nil
}

func (b *ECBackend) readAndDecode(bucket, key string, meta *ecObjectMeta) ([]byte, error) {
	codec := NewCodec(meta.DataShards, meta.ParityShards)
	total := codec.TotalShards()

	shards := make([][]byte, total)
	for i := range total {
		path := b.shardPath(bucket, key, i)
		data, err := os.ReadFile(path)
		if err != nil {
			shards[i] = nil // missing shard — leave nil for reconstruction
			continue
		}
		if b.encryptor != nil {
			decrypted, err := b.encryptor.Decrypt(data)
			if err != nil {
				shards[i] = nil // corrupted — treat as missing
				continue
			}
			data = decrypted
		}
		// Verify CRC32 footer and strip it
		payload, err := stripVerifyCRC(data)
		if err != nil {
			shards[i] = nil // bad CRC — treat as missing for reconstruction
			continue
		}
		shards[i] = payload
	}

	return codec.Decode(shards, meta.Size)
}
