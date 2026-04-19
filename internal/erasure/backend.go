package erasure

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// defaultScanPageSize is the number of DB keys read per BadgerDB transaction
// in ScanObjects. Smaller pages release MVCC read-lock earlier, reducing GC pressure.
const defaultScanPageSize = 256

// ECBackend implements storage.Backend with erasure coding.
// Objects are split into k data + m parity shards stored on disk.
// On read, missing shards (up to m) are reconstructed automatically.
type ECBackend struct {
	root         string
	db           *badger.DB
	codec        *Codec
	encryptor    Encryptor
	writeLocks   sync.Map // map[string]*sync.RWMutex for per-key serialization
	scanPageSize int      // keys per ScanObjects transaction page
	bloomFP      float64  // BadgerDB bloom filter false-positive rate (0 = use default)
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

// ErrCRCMismatch is returned when the stored CRC does not match the payload,
// or when a shard is too short to contain the 4-byte CRC footer.
// In both cases the scrubber treats the shard as corrupt and rewrites it via EC repair.
var ErrCRCMismatch = errors.New("CRC mismatch (bit-rot detected)")

// stripVerifyCRC verifies the 4-byte CRC32-IEEE footer and returns payload.
func stripVerifyCRC(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("%w: shard too short (%d bytes)", ErrCRCMismatch, len(data))
	}
	payload := data[:len(data)-4]
	stored := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(payload) != stored {
		return nil, ErrCRCMismatch
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
	Key            string
	Size           int64
	ContentType    string
	ETag           string
	LastModified   int64
	DataShards     int
	ParityShards   int
	ShardSize      int
	VersionID      string
	IsDeleteMarker bool
	CreatedNano    int64            // UnixNano; used for sub-second version ordering in DeleteObjectVersion
	ACL            s3auth.ACLGrant  // object-level ACL; 0 = ACLPrivate (backward compat)
}

// ECOption configures the EC backend.
type ECOption func(*ECBackend)

// WithEncryption enables at-rest encryption.
func WithEncryption(enc Encryptor) ECOption {
	return func(b *ECBackend) {
		b.encryptor = enc
	}
}

// WithScanPageSize sets the number of keys read per BadgerDB transaction in ScanObjects.
// Smaller values reduce MVCC read pressure at the cost of more transaction overhead.
// Intended for testing; production callers should rely on the default (256).
func WithScanPageSize(n int) ECOption {
	return func(b *ECBackend) {
		if n > 0 {
			b.scanPageSize = n
		}
	}
}

// WithBloomFalsePositive sets the bloom filter false-positive rate for BadgerDB SSTables.
// Lower values reduce read amplification but increase bloom filter memory usage.
// Default is 0.01 (1%). Applied before badger.Open via NewECBackend.
func WithBloomFalsePositive(p float64) ECOption {
	return func(b *ECBackend) {
		if p > 0 && p < 1 {
			b.bloomFP = p
		}
	}
}

// NewECBackend creates a new erasure coding storage backend.
func NewECBackend(root string, dataShards, parityShards int, opts ...ECOption) (*ECBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// First pass: collect db-level config from options before opening BadgerDB.
	b := &ECBackend{
		root:         root,
		codec:        NewCodec(dataShards, parityShards),
		scanPageSize: defaultScanPageSize,
	}
	for _, opt := range opts {
		opt(b)
	}

	dbDir := filepath.Join(root, "meta")
	dbOpts := badger.DefaultOptions(dbDir).WithLogger(nil)
	if b.bloomFP > 0 {
		dbOpts = dbOpts.WithBloomFalsePositive(b.bloomFP)
	}
	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	b.db = db
	return b, nil
}

// Close closes the metadata database.
func (b *ECBackend) Close() error {
	return b.db.Close()
}

// DB returns the underlying BadgerDB instance for shared metadata stores (e.g. lifecycle).
func (b *ECBackend) DB() *badger.DB { return b.db }

// ShardDir returns the directory where shards for a given bucket/key are stored.
// Exported for testing (shard deletion simulation).
func (b *ECBackend) ShardDir(bucket, key string) string {
	return b.shardDirFor(bucket, key, "")
}

func (b *ECBackend) shardDirFor(bucket, key, versionId string) string {
	input := key
	if versionId != "" {
		input = key + "/" + versionId
	}
	h := sha256.Sum256([]byte(input))
	return filepath.Join(b.root, "data", bucket, ".ec", hex.EncodeToString(h[:]))
}

func (b *ECBackend) shardPath(bucket, key string, idx int) string {
	return filepath.Join(b.ShardDir(bucket, key), fmt.Sprintf("%02d", idx))
}

func (b *ECBackend) shardPathFor(bucket, key, versionId string, idx int) string {
	return filepath.Join(b.shardDirFor(bucket, key, versionId), fmt.Sprintf("%02d", idx))
}

// bucketMeta stores per-bucket configuration.
type bucketMeta struct {
	ECEnabled        bool
	VersioningState  string // "Unversioned" (default), "Enabled", "Suspended"
}

func bucketKey(bucket string) []byte              { return []byte("bucket:" + bucket) }
func objectMetaKey(bucket, key string) []byte     { return []byte("obj:" + bucket + "/" + key) }
func objectMetaKeyV(bucket, key, vid string) []byte { return []byte("obj:" + bucket + "/" + key + "/" + vid) }
func latestKey(bucket, key string) []byte         { return []byte("lat:" + bucket + "/" + key) }
func multipartKey(uploadID string) []byte         { return []byte("mpu:" + uploadID) }

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
		meta, err := readBucketMeta(txn, bk)
		if err != nil {
			return err
		}
		meta.ECEnabled = ecEnabled
		metaBytes, err := marshalBucketMeta(meta)
		if err != nil {
			return fmt.Errorf("marshal bucket meta: %w", err)
		}
		return txn.Set(bk, metaBytes)
	})
}

// SetBucketVersioning sets the versioning state for a bucket ("Unversioned" or "Enabled").
func (b *ECBackend) SetBucketVersioning(bucket, state string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		bk := bucketKey(bucket)
		meta, err := readBucketMeta(txn, bk)
		if err != nil {
			return err
		}
		meta.VersioningState = state
		metaBytes, err := marshalBucketMeta(meta)
		if err != nil {
			return fmt.Errorf("marshal bucket meta: %w", err)
		}
		return txn.Set(bk, metaBytes)
	})
}

// GetBucketVersioning returns the versioning state for a bucket.
func (b *ECBackend) GetBucketVersioning(bucket string) (string, error) {
	var meta *bucketMeta
	err := b.db.View(func(txn *badger.Txn) error {
		bk := bucketKey(bucket)
		m, err := readBucketMeta(txn, bk)
		if err != nil {
			return err
		}
		meta = m
		return nil
	})
	if err != nil {
		return "", err
	}
	return meta.VersioningState, nil
}

// readBucketMeta reads and unmarshals bucket metadata from a BadgerDB transaction.
func readBucketMeta(txn *badger.Txn, bk []byte) (*bucketMeta, error) {
	item, err := txn.Get(bk)
	if err == badger.ErrKeyNotFound {
		return nil, storage.ErrBucketNotFound
	}
	if err != nil {
		return nil, err
	}
	var meta *bucketMeta
	if verr := item.Value(func(val []byte) error {
		m, err := unmarshalBucketMeta(val)
		if err != nil {
			meta = &bucketMeta{ECEnabled: true, VersioningState: "Unversioned"}
			return nil
		}
		meta = m
		return nil
	}); verr != nil {
		return nil, verr
	}
	return meta, nil
}

// GetBucketECPolicy returns whether EC is enabled for a bucket.
func (b *ECBackend) GetBucketECPolicy(bucket string) (bool, error) {
	var meta *bucketMeta
	err := b.db.View(func(txn *badger.Txn) error {
		m, err := readBucketMeta(txn, bucketKey(bucket))
		if err != nil {
			return err
		}
		meta = m
		return nil
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
	return b.putObjectCore(bucket, key, r, contentType, 0)
}

// PutObjectWithACL stores an object and atomically sets its ACL in the same
// BadgerDB transaction. Implements storage.AtomicACLPutter.
func (b *ECBackend) PutObjectWithACL(bucket, key string, r io.Reader, contentType string, acl uint8) (*storage.Object, error) {
	return b.putObjectCore(bucket, key, r, contentType, s3auth.ACLGrant(acl))
}

func (b *ECBackend) putObjectCore(bucket, key string, r io.Reader, contentType string, acl s3auth.ACLGrant) (*storage.Object, error) {
	bm, err := b.getBucketMeta(bucket)
	if err != nil {
		return nil, err
	}

	// Generate version ID for versioning-enabled buckets.
	var versionId string
	if bm.VersioningState == "Enabled" {
		versionId = uuid.New().String()
	}

	// Pass 1: stream body to temp file, computing size + ETag without buffering.
	f, size, etag, err := spoolToTemp(r)
	if err != nil {
		return nil, fmt.Errorf("spool object body: %w", err)
	}
	defer func() { f.Close(); os.Remove(f.Name()) }()

	unlock := b.acquireWriteLock(bucket, key)
	defer unlock()

	var obj *storage.Object
	if !bm.ECEnabled {
		// For plain storage, fall back to in-memory path (plain objects are
		// typically small; EC should be enabled for large objects).
		data, readErr := io.ReadAll(f)
		if readErr != nil {
			return nil, fmt.Errorf("read spooled plain data: %w", readErr)
		}
		obj, err = b.putObjectPlain(bucket, key, versionId, data, contentType, acl)
	} else {
		// Pass 2+: streaming Reed-Solomon encode from temp file.
		obj, err = b.putObjectDataStreaming(bucket, key, versionId, f, size, etag, contentType, acl)
	}
	if err != nil {
		return nil, err
	}
	if versionId != "" {
		obj.VersionID = versionId
	}
	return obj, nil
}

// getBucketMeta returns the bucket metadata or ErrBucketNotFound.
func (b *ECBackend) getBucketMeta(bucket string) (*bucketMeta, error) {
	var meta *bucketMeta
	err := b.db.View(func(txn *badger.Txn) error {
		m, err := readBucketMeta(txn, bucketKey(bucket))
		if err != nil {
			return err
		}
		meta = m
		return nil
	})
	return meta, err
}

// putObjectPlain stores an object without erasure coding (flat file).
func (b *ECBackend) putObjectPlain(bucket, key, versionId string, data []byte, contentType string, acl s3auth.ACLGrant) (*storage.Object, error) {
	objDir := filepath.Join(b.root, "data", bucket, ".plain")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		return nil, fmt.Errorf("create plain dir: %w", err)
	}

	// Version-aware file path: sha256(key) for Unversioned, sha256(key+"/"+versionId) for Enabled.
	var hashInput string
	if versionId != "" {
		hashInput = key + "/" + versionId
	} else {
		hashInput = key
	}
	h := sha256.Sum256([]byte(hashInput))
	filePath := filepath.Join(objDir, hex.EncodeToString(h[:]))
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write plain object: %w", err)
	}

	etag := fmt.Sprintf("%x", md5.Sum(data))
	nowNano := time.Now().UnixNano()

	meta := ecObjectMeta{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: nowNano / 1e9,
		CreatedNano:  nowNano,
		DataShards:   0, // indicates plain storage
		ParityShards: 0,
		VersionID:    versionId,
		ACL:          acl,
	}

	metaBytes, err := marshalECObjectMeta(&meta)
	if err != nil {
		return nil, fmt.Errorf("marshal object meta: %w", err)
	}

	var metaKey []byte
	if versionId != "" {
		metaKey = objectMetaKeyV(bucket, key, versionId)
	} else {
		metaKey = objectMetaKey(bucket, key)
	}

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(metaKey, metaBytes); err != nil {
			return err
		}
		if versionId != "" {
			return txn.Set(latestKey(bucket, key), []byte(versionId))
		}
		return nil
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
		LastModified: nowNano / 1e9,
	}, nil
}

func (b *ECBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	meta, err := b.getObjectMeta(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	var data []byte
	if meta.DataShards == 0 {
		data, err = b.readPlain(bucket, key, meta.VersionID)
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
		VersionID:    meta.VersionID,
		ACL:          uint8(meta.ACL),
	}
	return io.NopCloser(bytes.NewReader(data)), obj, nil
}

// GetObjectVersion retrieves a specific version of an object by versionId.
func (b *ECBackend) GetObjectVersion(bucket, key, versionId string) (io.ReadCloser, *storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, nil, err
	}

	var meta *ecObjectMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKeyV(bucket, key, versionId))
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
		return nil, nil, err
	}

	if meta.IsDeleteMarker {
		return nil, nil, storage.ErrMethodNotAllowed
	}

	var data []byte
	if meta.DataShards == 0 {
		data, err = b.readPlain(bucket, key, versionId)
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
		VersionID:    versionId,
		ACL:          uint8(meta.ACL),
	}
	return io.NopCloser(bytes.NewReader(data)), obj, nil
}

// HeadObjectVersion returns metadata for a specific version.
// Returns ErrMethodNotAllowed if the version is a delete marker.
func (b *ECBackend) HeadObjectVersion(bucket, key, versionId string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var meta *ecObjectMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKeyV(bucket, key, versionId))
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
	if meta.IsDeleteMarker {
		return nil, storage.ErrMethodNotAllowed
	}
	return &storage.Object{
		Key:          meta.Key,
		Size:         meta.Size,
		ContentType:  meta.ContentType,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
		VersionID:    versionId,
		ACL:          uint8(meta.ACL),
	}, nil
}

// SetObjectACL updates the ACL for the latest version of an object.
// Implements storage.ACLSetter.
func (b *ECBackend) SetObjectACL(bucket, key string, acl uint8) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}

	return b.db.Update(func(txn *badger.Txn) error {
		// Resolve latest version pointer if present.
		metaKey := objectMetaKey(bucket, key)
		if latItem, err := txn.Get(latestKey(bucket, key)); err == nil {
			var versionId string
			if err := latItem.Value(func(v []byte) error {
				versionId = string(v)
				return nil
			}); err != nil {
				return err
			}
			metaKey = objectMetaKeyV(bucket, key, versionId)
		}

		item, err := txn.Get(metaKey)
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}

		var meta *ecObjectMeta
		if err := item.Value(func(val []byte) error {
			var unmarshalErr error
			meta, unmarshalErr = unmarshalECObjectMeta(val)
			return unmarshalErr
		}); err != nil {
			return err
		}

		if meta.IsDeleteMarker {
			return storage.ErrObjectNotFound
		}

		meta.ACL = s3auth.ACLGrant(acl)
		encoded, err := marshalECObjectMeta(meta)
		if err != nil {
			return err
		}
		return txn.Set(metaKey, encoded)
	})
}

func (b *ECBackend) readPlain(bucket, key, versionId string) ([]byte, error) {
	var hashInput string
	if versionId != "" {
		hashInput = key + "/" + versionId
	} else {
		hashInput = key
	}
	h := sha256.Sum256([]byte(hashInput))
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
		VersionID:    meta.VersionID,
		ACL:          uint8(meta.ACL),
	}, nil
}

// DeleteObjectReturningMarker performs a soft-delete on a versioning-enabled bucket
// and returns the created delete marker's version ID.
func (b *ECBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	bm, err := b.getBucketMeta(bucket)
	if err != nil {
		return "", err
	}
	if bm.VersioningState != "Enabled" {
		return "", b.DeleteObject(bucket, key)
	}
	markerID := uuid.New().String()
	markerMeta := ecObjectMeta{
		Key:            key,
		IsDeleteMarker: true,
		VersionID:      markerID,
		LastModified:   time.Now().Unix(),
		CreatedNano:    time.Now().UnixNano(),
	}
	markerBytes, err := marshalECObjectMeta(&markerMeta)
	if err != nil {
		return "", fmt.Errorf("marshal delete marker: %w", err)
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKeyV(bucket, key, markerID), markerBytes); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(markerID))
	}); err != nil {
		return "", err
	}
	return markerID, nil
}

func (b *ECBackend) DeleteObject(bucket, key string) error {
	bm, err := b.getBucketMeta(bucket)
	if err != nil {
		return err
	}

	if bm.VersioningState == "Enabled" {
		// Versioning: create a delete marker (lat: → new marker UUID, no data deleted).
		markerID := uuid.New().String()
		markerMeta := ecObjectMeta{
			Key:            key,
			IsDeleteMarker: true,
			VersionID:      markerID,
			LastModified:   time.Now().Unix(),
		}
		markerBytes, err := marshalECObjectMeta(&markerMeta)
		if err != nil {
			return fmt.Errorf("marshal delete marker: %w", err)
		}
		return b.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(objectMetaKeyV(bucket, key, markerID), markerBytes); err != nil {
				return err
			}
			return txn.Set(latestKey(bucket, key), []byte(markerID))
		})
	}

	// Unversioned: hard delete — remove shards, plain file, and meta.
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

// DeleteObjectVersion hard-deletes a specific version of an object.
func (b *ECBackend) DeleteObjectVersion(bucket, key, versionId string) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}
	metaKey := objectMetaKeyV(bucket, key, versionId)

	return b.db.Update(func(txn *badger.Txn) error {
		// Verify version exists
		if _, err := txn.Get(metaKey); err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		} else if err != nil {
			return err
		}

		// Remove version metadata
		if err := txn.Delete(metaKey); err != nil {
			return err
		}

		// Update lat: pointer: find remaining latest version
		latKey := latestKey(bucket, key)
		item, err := txn.Get(latKey)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if err == nil {
			currentLatest := ""
			_ = item.Value(func(val []byte) error { currentLatest = string(val); return nil })
			if currentLatest == versionId {
				// Deleted version was latest — find the remaining version with the highest Modified.
				// UUIDs are random so lexicographic order does not reflect insertion order.
				var newLatest string
				var bestModified int64
				pfx := []byte("obj:" + bucket + "/" + key + "/")
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
					k := string(it.Item().Key())
					vid := strings.TrimPrefix(k, "obj:"+bucket+"/"+key+"/")
					if vid == versionId || vid == "" {
						continue
					}
					_ = it.Item().Value(func(val []byte) error {
						m, err := unmarshalECObjectMeta(val)
						if err == nil {
							// Use CreatedNano for sub-second precision; fall back to LastModified for old records.
							ts := m.CreatedNano
							if ts == 0 {
								ts = m.LastModified * 1e9
							}
							if newLatest == "" || ts > bestModified {
								bestModified = ts
								newLatest = vid
							}
						}
						return nil
					})
				}
				it.Close()
				if newLatest == "" {
					_ = txn.Delete(latKey)
				} else {
					_ = txn.Set(latKey, []byte(newLatest))
				}
			}
		}

		// Remove shard data; log on failure but don't roll back the committed metadata deletion.
		if err := os.RemoveAll(b.shardDirFor(bucket, key, versionId)); err != nil {
			slog.Warn("DeleteObjectVersion: shard removal failed", "err", err, "bucket", bucket, "key", key)
		}
		return nil
	})
}

func (b *ECBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var objects []*storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		// Pre-load lat: pointers so versioned entries can be deduplicated to latest only.
		latMap := map[string]string{} // base key → latest versionId
		latPfx := []byte("lat:" + bucket + "/")
		itLat := txn.NewIterator(badger.IteratorOptions{PrefetchValues: true, PrefetchSize: 100})
		for itLat.Seek(latPfx); itLat.ValidForPrefix(latPfx); itLat.Next() {
			baseKey := string(itLat.Item().Key()[len("lat:"+bucket+"/"):])
			_ = itLat.Item().Value(func(v []byte) error { latMap[baseKey] = string(v); return nil })
		}
		itLat.Close()

		seen := map[string]bool{} // deduplicate versioned base keys
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

			// Detect versioned key by UUID suffix: obj:bucket/baseKey/uuid
			rest := string(it.Item().Key()[len("obj:"+bucket+"/"):])
			lastSlash := strings.LastIndex(rest, "/")
			isVersioned := lastSlash >= 0 && len(rest[lastSlash+1:]) == 36 && strings.Count(rest[lastSlash+1:], "-") == 4

			var meta *ecObjectMeta
			if err := it.Item().Value(func(val []byte) error {
				var unmarshalErr error
				meta, unmarshalErr = unmarshalECObjectMeta(val)
				return unmarshalErr
			}); err != nil {
				return err
			}

			if isVersioned {
				baseKey := rest[:lastSlash]
				if seen[baseKey] {
					continue
				}
				// Only include the latest non-delete-marker version.
				if meta.VersionID != latMap[baseKey] || meta.IsDeleteMarker {
					continue
				}
				seen[baseKey] = true
			} else {
				// Non-versioned key: skip if a versioned entry for the same key was already included
				// (can happen during Suspended→Enabled transition).
				if seen[meta.Key] {
					continue
				}
				seen[meta.Key] = true
			}

			objects = append(objects, &storage.Object{
				Key:          meta.Key,
				Size:         meta.Size,
				ContentType:  meta.ContentType,
				ETag:         meta.ETag,
				LastModified: meta.LastModified,
				ACL:          uint8(meta.ACL),
			})
			count++
		}
		return nil
	})
	return objects, err
}

// ListObjectVersions returns all versions (including delete markers) for versioning-enabled bucket.
// Keys are scanned under "obj:<bucket>/<key>/<versionId>" prefix.
// For each unique key the lat: pointer identifies the latest version.
func (b *ECBackend) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	// collect latest versionId per key
	latestByKey := map[string]string{}
	err := b.db.View(func(txn *badger.Txn) error {
		latPfx := []byte("lat:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(latPfx); it.ValidForPrefix(latPfx); it.Next() {
			k := string(it.Item().Key())
			key := strings.TrimPrefix(k, "lat:"+bucket+"/")
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				continue
			}
			_ = it.Item().Value(func(val []byte) error {
				latestByKey[key] = string(val)
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var versions []*storage.ObjectVersion
	err = b.db.View(func(txn *badger.Txn) error {
		vPfx := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		count := 0
		for it.Seek(vPfx); it.ValidForPrefix(vPfx); it.Next() {
			if count >= maxKeys {
				break
			}
			k := string(it.Item().Key())
			rest := strings.TrimPrefix(k, "obj:"+bucket+"/")

			// versioned keys end with a UUID suffix: <key>/<36-char-uuid>
			lastSlash := strings.LastIndex(rest, "/")
			if lastSlash < 0 {
				continue
			}
			vid := rest[lastSlash+1:]
			if len(vid) != 36 || strings.Count(vid, "-") != 4 {
				continue // not a versionId suffix
			}
			objKey := rest[:lastSlash]
			if objKey == "" {
				continue
			}
			if prefix != "" && !strings.HasPrefix(objKey, prefix) {
				continue
			}

			var meta ecObjectMeta
			if err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalECObjectMeta(val)
				if err != nil {
					return err
				}
				meta = *m
				return nil
			}); err != nil {
				return err
			}

			v := &storage.ObjectVersion{
				Key:            objKey,
				VersionID:      vid,
				IsLatest:       latestByKey[objKey] == vid,
				IsDeleteMarker: meta.IsDeleteMarker,
				LastModified:   meta.LastModified,
				ETag:           meta.ETag,
				Size:           meta.Size,
			}
			versions = append(versions, v)
			count++
		}
		return nil
	})
	return versions, err
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

	// Stream-assemble parts via io.MultiReader → spool → EC encode.
	// Avoids bytes.Buffer OOM for large multipart uploads.
	partDir := filepath.Join(b.root, "parts", uploadID)
	partFiles := make([]*os.File, 0, len(parts))
	defer func() {
		for _, pf := range partFiles {
			pf.Close()
		}
	}()
	var partReaders []io.Reader
	for _, p := range parts {
		pf, openErr := os.Open(filepath.Join(partDir, fmt.Sprintf("%05d", p.PartNumber)))
		if openErr != nil {
			return nil, fmt.Errorf("open part %d: %w", p.PartNumber, openErr)
		}
		partFiles = append(partFiles, pf)
		partReaders = append(partReaders, pf)
	}

	// Spool assembled stream to a temp file to get size + ETag in one pass.
	spooled, totalSize, etag, spoolErr := spoolToTemp(io.MultiReader(partReaders...))
	if spoolErr != nil {
		return nil, fmt.Errorf("spool parts: %w", spoolErr)
	}
	defer func() { spooled.Close(); os.Remove(spooled.Name()) }()

	// Check bucket meta for EC and versioning
	bm, bmErr := b.getBucketMeta(bucket)
	if bmErr != nil {
		// Default to EC enabled on error
		bm = &bucketMeta{ECEnabled: true}
	}

	var versionId string
	if bm.VersioningState == "Enabled" {
		versionId = uuid.New().String()
	}

	var obj *storage.Object
	if bm.ECEnabled {
		obj, err = b.putObjectDataStreaming(bucket, key, versionId, spooled, totalSize, etag, uploadMeta.ContentType, 0)
	} else {
		data, readErr := io.ReadAll(spooled)
		if readErr != nil {
			return nil, fmt.Errorf("read spooled plain data: %w", readErr)
		}
		obj, err = b.putObjectPlain(bucket, key, versionId, data, uploadMeta.ContentType, 0)
	}
	if err != nil {
		return nil, err
	}
	if versionId != "" {
		obj.VersionID = versionId
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
//
// Internally iterates in pages of b.scanPageSize keys, opening a fresh BadgerDB
// read transaction per page. This releases the MVCC read-lock between pages,
// allowing BadgerDB GC to reclaim old value-log entries.
func (b *ECBackend) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}
	ch := make(chan scrubber.ObjectRecord, 64)
	go func() {
		defer close(ch)
		prefix := []byte("obj:" + bucket + "/")
		// cursor is the key to seek to at the start of each page.
		cursor := append([]byte{}, prefix...)
		pageSize := b.scanPageSize

		for {
			var nextCursor []byte
			_ = b.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				count := 0
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					if count == pageSize {
						// Record the start key of the next page.
						k := it.Item().Key()
						nextCursor = make([]byte, len(k))
						copy(nextCursor, k)
						return nil
					}
					count++

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
					if meta.IsDeleteMarker {
						continue // delete markers have no shards to scrub
					}
					rawKey := string(it.Item().Key())
					rest := strings.TrimPrefix(rawKey, "obj:"+bucket+"/")
					// Versioned keys: "key/versionId" — split on last slash
					objKey, versionID := rest, ""
					if idx := strings.LastIndex(rest, "/"); idx >= 0 {
						// heuristic: if suffix looks like a UUID (contains dashes), it's a versionId
						suffix := rest[idx+1:]
						if len(suffix) == 36 && strings.Count(suffix, "-") == 4 {
							objKey = rest[:idx]
							versionID = suffix
						}
					}
					ch <- scrubber.ObjectRecord{
						Bucket:         bucket,
						Key:            objKey,
						DataShards:     meta.DataShards,
						ParityShards:   meta.ParityShards,
						ETag:           meta.ETag,
						VersionID:      versionID,
						IsDeleteMarker: meta.IsDeleteMarker,
						LastModified:   meta.LastModified,
					}
				}
				return nil
			})
			if nextCursor == nil {
				return // last page exhausted
			}
			cursor = nextCursor
		}
	}()
	return ch, nil
}

// plainFilePath returns the filesystem path for a plain (non-EC) object.
func (b *ECBackend) plainFilePath(bucket, key, versionId string) string {
	var hashInput string
	if versionId != "" {
		hashInput = key + "/" + versionId
	} else {
		hashInput = key
	}
	h := sha256.Sum256([]byte(hashInput))
	return filepath.Join(b.root, "data", bucket, ".plain", hex.EncodeToString(h[:]))
}

// ScanPlainObjects streams plain objects in the bucket that are large enough to be
// re-encoded as EC. Only called when the backend is configured with EC (DataShards > 0).
// Implements scrubber.Migrator.
func (b *ECBackend) ScanPlainObjects(bucket string) (<-chan scrubber.PlainRecord, error) {
	ch := make(chan scrubber.PlainRecord, 64)
	if b.codec.DataShards <= 0 {
		close(ch)
		return ch, nil
	}
	if err := b.HeadBucket(bucket); err != nil {
		close(ch)
		return nil, err
	}
	go func() {
		defer close(ch)
		prefix := []byte("obj:" + bucket + "/")
		cursor := append([]byte{}, prefix...)
		pageSize := b.scanPageSize

		for {
			var nextCursor []byte
			_ = b.db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				count := 0
				for it.Seek(cursor); it.ValidForPrefix(prefix); it.Next() {
					if count == pageSize {
						k := it.Item().Key()
						nextCursor = make([]byte, len(k))
						copy(nextCursor, k)
						return nil
					}
					count++

					var meta *ecObjectMeta
					if err := it.Item().Value(func(val []byte) error {
						var err error
						meta, err = unmarshalECObjectMeta(val)
						return err
					}); err != nil {
						continue
					}
					if meta.DataShards > 0 || meta.IsDeleteMarker {
						continue
					}
					if meta.Size < int64(b.codec.DataShards) {
						continue // too small for EC; leave as plain
					}
					rawKey := string(it.Item().Key())
					rest := strings.TrimPrefix(rawKey, "obj:"+bucket+"/")
					objKey, versionID := rest, ""
					if idx := strings.LastIndex(rest, "/"); idx >= 0 {
						suffix := rest[idx+1:]
						if len(suffix) == 36 && strings.Count(suffix, "-") == 4 {
							objKey = rest[:idx]
							versionID = suffix
						}
					}
					ch <- scrubber.PlainRecord{
						Bucket:      bucket,
						Key:         objKey,
						VersionID:   versionID,
						Size:        meta.Size,
						ETag:        meta.ETag,
						ContentType: meta.ContentType,
					}
				}
				return nil
			})
			if nextCursor == nil {
				return
			}
			cursor = nextCursor
		}
	}()
	return ch, nil
}

// MigratePlainToEC re-encodes a plain object as EC shards and deletes the old plain file.
// Implements scrubber.Migrator.
func (b *ECBackend) MigratePlainToEC(rec scrubber.PlainRecord) error {
	unlock := b.acquireWriteLock(rec.Bucket, rec.Key)
	defer unlock()

	// Re-confirm under lock that the object is still plain (DataShards==0).
	// Guards against a concurrent PutObject that re-encoded this object while
	// the scrubber was scanning.
	stale := false
	_ = b.db.View(func(txn *badger.Txn) error {
		var metaKey []byte
		if rec.VersionID != "" {
			metaKey = objectMetaKeyV(rec.Bucket, rec.Key, rec.VersionID)
		} else {
			metaKey = objectMetaKey(rec.Bucket, rec.Key)
		}
		item, err := txn.Get(metaKey)
		if err != nil {
			stale = true
			return nil
		}
		return item.Value(func(val []byte) error {
			meta, err := unmarshalECObjectMeta(val)
			if err != nil || meta.DataShards > 0 {
				stale = true
			}
			return nil
		})
	})
	if stale {
		return nil
	}

	plainPath := b.plainFilePath(rec.Bucket, rec.Key, rec.VersionID)
	f, err := os.Open(plainPath)
	if err != nil {
		return fmt.Errorf("open plain file: %w", err)
	}
	defer f.Close()

	if _, err := b.putObjectDataStreaming(rec.Bucket, rec.Key, rec.VersionID, f, rec.Size, rec.ETag, rec.ContentType, 0); err != nil {
		return fmt.Errorf("re-encode as EC: %w", err)
	}

	_ = os.Remove(plainPath)
	return nil
}

// ShardPaths returns all shard file paths for an object.
// Scrubber API contract — do not change without bumping scrubber version.
func (b *ECBackend) ShardPaths(bucket, key, versionID string, totalShards int) []string {
	paths := make([]string, totalShards)
	for i := range paths {
		paths[i] = b.shardPathFor(bucket, key, versionID, i)
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
		// Check for a latest-version pointer (versioned objects).
		latItem, latErr := txn.Get(latestKey(bucket, key))
		if latErr == nil {
			var versionId string
			if err := latItem.Value(func(v []byte) error {
				versionId = string(v)
				return nil
			}); err != nil {
				return err
			}
			item, err := txn.Get(objectMetaKeyV(bucket, key, versionId))
			if err == badger.ErrKeyNotFound {
				return storage.ErrObjectNotFound
			}
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				var unmarshalErr error
				meta, unmarshalErr = unmarshalECObjectMeta(val)
				if unmarshalErr != nil {
					return unmarshalErr
				}
				if meta.IsDeleteMarker {
					meta = nil
					return storage.ErrObjectNotFound
				}
				return nil
			})
		}

		// Fall back to unversioned key.
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

	// Read all lat: pointers so we can mark IsLatest correctly.
	latMap := map[string]string{} // "bucket/key" → latestVersionID
	if err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("lat:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			bk := string(it.Item().Key()[len("lat:"):])
			if err := it.Item().Value(func(val []byte) error {
				latMap[bk] = string(val)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read lat: pointers: %w", err)
	}

	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("obj:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			rawKey := string(it.Item().Key())
			rest := rawKey[len("obj:"):]
			// rest = "<bucket>/<key>" or "<bucket>/<key>/<versionId>"
			firstSlash := strings.Index(rest, "/")
			if firstSlash < 0 {
				continue
			}
			bucket := rest[:firstSlash]
			keyAndVer := rest[firstSlash+1:]

			var meta *ecObjectMeta
			if err := it.Item().Value(func(val []byte) error {
				var err error
				meta, err = unmarshalECObjectMeta(val)
				return err
			}); err != nil {
				return err
			}
			if meta.IsDeleteMarker {
				continue // delete markers have no recoverable data
			}
			// Determine actual key (strip versionId suffix if present)
			objKey := keyAndVer
			versionID := meta.VersionID
			if versionID != "" {
				// versioned key: "key/versionId" — strip the suffix
				if idx := strings.LastIndex(keyAndVer, "/"); idx >= 0 {
					objKey = keyAndVer[:idx]
				}
			}
			isLatest := versionID == "" || latMap[bucket+"/"+objKey] == versionID
			objs = append(objs, storage.SnapshotObject{
				Bucket:      bucket,
				Key:         objKey,
				ETag:        meta.ETag,
				Size:        meta.Size,
				ContentType: meta.ContentType,
				Modified:    meta.LastModified,
				VersionID:   versionID,
				IsLatest:    isLatest,
				ACL:         uint8(meta.ACL),
			})
		}
		return nil
	})
	return objs, err
}

// RestoreObjects implements storage.Snapshotable: replaces current object metadata with snapshot state.
func (b *ECBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	inSnapshot := make(map[string]bool, len(objects))
	snapshotVersionedKeys := make(map[string]bool) // "bucket/key" pairs with versioned objects
	for _, o := range objects {
		if o.VersionID != "" {
			inSnapshot["obj:"+o.Bucket+"/"+o.Key+"/"+o.VersionID] = true
			snapshotVersionedKeys[o.Bucket+"/"+o.Key] = true
		} else {
			inSnapshot["obj:"+o.Bucket+"/"+o.Key] = true
		}
	}

	// Delete metadata for objects not in snapshot; also clean up orphaned lat: pointers.
	if err := b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		var toDelete [][]byte

		// Orphaned obj: entries
		objPrefix := []byte("obj:")
		it := txn.NewIterator(opts)
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			if !inSnapshot[string(it.Item().Key())] {
				cp := make([]byte, len(it.Item().Key()))
				copy(cp, it.Item().Key())
				toDelete = append(toDelete, cp)
			}
		}
		it.Close()

		// Orphaned lat: entries (versioned keys absent from snapshot)
		latPrefix := []byte("lat:")
		itLat := txn.NewIterator(opts)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			suffix := string(itLat.Item().Key()[len("lat:"):])
			if !snapshotVersionedKeys[suffix] {
				cp := make([]byte, len(itLat.Item().Key()))
				copy(cp, itLat.Item().Key())
				toDelete = append(toDelete, cp)
			}
		}
		itLat.Close()

		for _, k := range toDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return 0, nil, fmt.Errorf("remove obsolete objects: %w", err)
	}

	// latestVersions collects versioned objects marked IsLatest in the snapshot.
	// Used in a post-pass to set lat: pointers correctly after all objects are restored.
	type latEntry struct{ bucket, key, versionID string }
	var latestVersions []latEntry

	var stale []storage.StaleBlob
	var count int
	for _, snap := range objects {
		// Ensure bucket exists
		if err := b.CreateBucket(snap.Bucket); err != nil && !strings.Contains(err.Error(), "already exists") {
			return count, stale, fmt.Errorf("ensure bucket %s: %w", snap.Bucket, err)
		}

		// Check data blobs exist: EC objects have a shard "00"; plain objects (DataShards=0)
		// have a flat file under .plain/. Try EC first, then plain.
		shardDir := b.shardDirFor(snap.Bucket, snap.Key, snap.VersionID)
		firstShard := filepath.Join(shardDir, "00")
		shardFi, shardErr := os.Stat(firstShard)

		isPlain := false
		if shardErr != nil {
			// Not found in EC directory — check plain flat-file path.
			var hashInput string
			if snap.VersionID != "" {
				hashInput = snap.Key + "/" + snap.VersionID
			} else {
				hashInput = snap.Key
			}
			h := sha256.Sum256([]byte(hashInput))
			plainPath := filepath.Join(b.root, "data", snap.Bucket, ".plain", hex.EncodeToString(h[:]))
			if _, plainErr := os.Stat(plainPath); plainErr != nil {
				stale = append(stale, storage.StaleBlob{Bucket: snap.Bucket, Key: snap.Key, ExpectedETag: snap.ETag})
				continue
			}
			isPlain = true
		}

		// Restore metadata; preserve plain-vs-EC storage type.
		var dataShards, parityShards, shardSize int
		if !isPlain {
			dataShards = b.codec.DataShards
			parityShards = b.codec.ParityShards
			shardSize = int(shardFi.Size())
		}
		meta := &ecObjectMeta{
			Key:          snap.Key,
			Size:         snap.Size,
			ContentType:  snap.ContentType,
			ETag:         snap.ETag,
			LastModified: snap.Modified,
			DataShards:   dataShards,
			ParityShards: parityShards,
			ShardSize:    shardSize,
			VersionID:    snap.VersionID,
			ACL:          s3auth.ACLGrant(snap.ACL),
		}
		metaBytes, err := marshalECObjectMeta(meta)
		if err != nil {
			return count, stale, fmt.Errorf("marshal %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		if err := b.db.Update(func(txn *badger.Txn) error {
			if snap.VersionID != "" {
				return txn.Set(objectMetaKeyV(snap.Bucket, snap.Key, snap.VersionID), metaBytes)
			}
			return txn.Set(objectMetaKey(snap.Bucket, snap.Key), metaBytes)
		}); err != nil {
			return count, stale, fmt.Errorf("restore %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++

		// Collect IsLatest versions for the lat: post-pass.
		if snap.VersionID != "" && snap.IsLatest {
			latestVersions = append(latestVersions, latEntry{snap.Bucket, snap.Key, snap.VersionID})
		}
	}

	// Post-pass: set lat: pointers using the IsLatest marker captured by ListAllObjects.
	// This is correct even when multiple versions share the same Modified timestamp.
	latSet := make(map[string]bool)
	for _, le := range latestVersions {
		if err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(latestKey(le.bucket, le.key), []byte(le.versionID))
		}); err != nil {
			return count, stale, fmt.Errorf("restore lat: %s/%s: %w", le.bucket, le.key, err)
		}
		latSet[le.bucket+"/"+le.key] = true
	}

	// Backward compat: old snapshots without IsLatest markers fall back to max-Modified.
	type fbCandidate struct {
		modified  int64
		versionID string
		bucket    string
		key       string
	}
	fallback := make(map[string]fbCandidate)
	for _, snap := range objects {
		if snap.VersionID == "" {
			continue
		}
		mapKey := snap.Bucket + "/" + snap.Key
		if latSet[mapKey] {
			continue
		}
		if prev, ok := fallback[mapKey]; !ok || snap.Modified > prev.modified {
			fallback[mapKey] = fbCandidate{snap.Modified, snap.VersionID, snap.Bucket, snap.Key}
		}
	}
	for _, cand := range fallback {
		if err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(latestKey(cand.bucket, cand.key), []byte(cand.versionID))
		}); err != nil {
			return count, stale, fmt.Errorf("restore lat: fallback %s/%s: %w", cand.bucket, cand.key, err)
		}
	}

	return count, stale, nil
}

// ListAllBuckets implements storage.BucketSnapshotable: scans `bucket:` prefix
// and returns per-bucket state (versioning, EC flag) so PITR snapshots can
// replay the exact configuration.
func (b *ECBackend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	var out []storage.SnapshotBucket
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("bucket:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			name := string(it.Item().Key()[len("bucket:"):])
			var meta *bucketMeta
			if err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalBucketMeta(val)
				if err != nil {
					meta = &bucketMeta{ECEnabled: true, VersioningState: "Unversioned"}
					return nil
				}
				meta = m
				return nil
			}); err != nil {
				return err
			}
			out = append(out, storage.SnapshotBucket{
				Name:            name,
				VersioningState: meta.VersioningState,
				ECEnabled:       meta.ECEnabled,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list bucket meta: %w", err)
	}
	return out, nil
}

// RestoreBuckets implements storage.BucketSnapshotable: writes each snapshot
// bucket's metadata, creating entries for buckets that no longer exist and
// overwriting state for any that do. Does NOT delete buckets absent from the
// snapshot — RestoreObjects handles object-level cleanup; bucket deletion is
// a separate lifecycle concern.
func (b *ECBackend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	return b.db.Update(func(txn *badger.Txn) error {
		for _, sb := range buckets {
			meta := &bucketMeta{
				ECEnabled:       sb.ECEnabled,
				VersioningState: sb.VersioningState,
			}
			data, err := marshalBucketMeta(meta)
			if err != nil {
				return fmt.Errorf("marshal bucket meta %s: %w", sb.Name, err)
			}
			if err := txn.Set(bucketKey(sb.Name), data); err != nil {
				return fmt.Errorf("set bucket meta %s: %w", sb.Name, err)
			}
		}
		return nil
	})
}

func (b *ECBackend) readAndDecode(bucket, key string, meta *ecObjectMeta) ([]byte, error) {
	codec := NewCodec(meta.DataShards, meta.ParityShards)
	total := codec.TotalShards()

	shards := make([][]byte, total)
	for i := range total {
		path := b.shardPathFor(bucket, key, meta.VersionID, i)
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
