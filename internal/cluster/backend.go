package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// OnApplyFunc is called after FSM.Apply() with the command type, bucket, and key.
// Used for cache invalidation and metrics updates.
type OnApplyFunc func(cmdType CommandType, bucket, key string)

// DistributedBackend implements storage.Backend with Raft-replicated metadata
// and local file storage for data. Metadata mutations go through Raft;
// reads are served from the local BadgerDB (kept in sync by the FSM).
type DistributedBackend struct {
	root        string
	db          *badger.DB
	node        *raft.Node
	fsm         *FSM
	logger      *slog.Logger
	lastApplied atomic.Uint64
	onApply     OnApplyFunc
	snapMgr     *raft.SnapshotManager
	snapNode    *raft.Node // node for CompactLog after snapshot
	shardSvc    *ShardService
	allNodes    []string // all node addresses (including self) for shard placement
}

// NewDistributedBackend creates a new distributed storage backend.
// The FSM apply loop must be started separately via RunApplyLoop.
func NewDistributedBackend(root string, db *badger.DB, node *raft.Node) (*DistributedBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fsm := NewFSM(db)
	return &DistributedBackend{
		root:   root,
		db:     db,
		node:   node,
		fsm:    fsm,
		logger: slog.With("component", "distributed-backend"),
	}, nil
}

// SetShardService configures the distributed shard service for fan-out.
// allNodes includes all cluster node addresses for placement.
func (b *DistributedBackend) SetShardService(svc *ShardService, allNodes []string) {
	b.shardSvc = svc
	b.allNodes = allNodes
}

// SetSnapshotManager configures automatic snapshot creation after N applied entries.
// Must be called before RunApplyLoop.
func (b *DistributedBackend) SetSnapshotManager(mgr *raft.SnapshotManager, node *raft.Node) {
	b.snapMgr = mgr
	b.snapNode = node
}

// SetOnApply sets the callback invoked after each FSM apply.
// Must be called before RunApplyLoop.
func (b *DistributedBackend) SetOnApply(fn OnApplyFunc) {
	b.onApply = fn
}

// RunApplyLoop consumes committed entries from the Raft node and applies them to the FSM.
// This must run in a goroutine.
func (b *DistributedBackend) RunApplyLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case entry := <-b.node.ApplyCh():
			if err := b.fsm.Apply(entry.Command); err != nil {
				b.logger.Error("fsm apply error", "index", entry.Index, "error", err)
			}
			b.lastApplied.Store(entry.Index)

			// Notify cache/metrics callback
			if b.onApply != nil {
				b.notifyOnApply(entry.Command)
			}

			// Check if snapshot should be taken
			if b.snapMgr != nil {
				if b.snapMgr.MaybeTrigger(entry.Index, entry.Term) {
					b.logger.Info("snapshot taken", "index", entry.Index, "term", entry.Term)
					if b.snapNode != nil {
						b.snapNode.CompactLog(entry.Index)
					}
				}
			}
		}
	}
}

// notifyOnApply extracts bucket/key from a committed command and calls the callback.
func (b *DistributedBackend) notifyOnApply(raw []byte) {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return
	}

	var bucket, key string
	switch cmd.Type {
	case CmdPutObjectMeta:
		c, err := decodePutObjectMetaCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	case CmdDeleteObject:
		c, err := decodeDeleteObjectCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	case CmdCompleteMultipart:
		c, err := decodeCompleteMultipartCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	default:
		// Other commands don't affect object cache
		bucket = ""
	}

	if bucket != "" {
		b.onApply(cmd.Type, bucket, key)
	}
}

func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	data, err := EncodeCommand(cmdType, payload)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	idx, err := b.node.ProposeWait(proposeCtx, data)
	if err != nil {
		return err
	}

	// Wait until the FSM has applied this entry
	for b.lastApplied.Load() < idx {
		select {
		case <-proposeCtx.Done():
			return proposeCtx.Err()
		default:
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}

// Close closes the metadata database.
func (b *DistributedBackend) Close() error {
	return b.db.Close()
}

// --- Bucket operations ---

func (b *DistributedBackend) CreateBucket(bucket string) error {
	// Check if already exists (read local)
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		return err
	})
	if err == nil {
		return storage.ErrBucketAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
		return fmt.Errorf("create bucket dir: %w", err)
	}

	return b.propose(context.Background(), CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
}

func (b *DistributedBackend) HeadBucket(bucket string) error {
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	})
}

func (b *DistributedBackend) DeleteBucket(bucket string) error {
	// Check existence and emptiness
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
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
		return nil
	})
	if err != nil {
		return err
	}

	if err := os.RemoveAll(b.bucketDir(bucket)); err != nil {
		return fmt.Errorf("remove bucket dir: %w", err)
	}

	return b.propose(context.Background(), CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})
}

func (b *DistributedBackend) ListBuckets() ([]string, error) {
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

func (b *DistributedBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	// Read all data into memory for replication
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	// Write data locally
	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	if err := os.WriteFile(objPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write object: %w", err)
	}

	// Replicate data to all peer nodes via ShardService
	if b.shardSvc != nil {
		ctx := context.Background()
		for _, peer := range b.allNodes {
			if peer == b.node.ID() {
				continue // skip self
			}
			if err := b.shardSvc.WriteShard(ctx, peer, bucket, key, 0, data); err != nil {
				b.logger.Warn("data replication failed", "peer", peer, "bucket", bucket, "key", key, "error", err)
			}
		}
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	// Replicate metadata through Raft
	err = b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        etag,
		ModTime:     now,
	})
	if err != nil {
		os.Remove(objPath)
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

func (b *DistributedBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	// Try local first
	f, err := os.Open(b.objectPath(bucket, key))
	if err == nil {
		return f, obj, nil
	}

	// Local file not found — try fetching from peer nodes
	if b.shardSvc != nil && os.IsNotExist(err) {
		ctx := context.Background()
		for _, peer := range b.allNodes {
			if peer == b.node.ID() {
				continue
			}
			data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, key, 0)
			if fetchErr == nil && data != nil {
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
		}
	}

	return nil, nil, fmt.Errorf("open object: %w", err)
}

func (b *DistributedBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var obj storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey(bucket, key))
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			obj = storage.Object{
				Key:          m.Key,
				Size:         m.Size,
				ContentType:  m.ContentType,
				ETag:         m.ETag,
				LastModified: m.LastModified,
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (b *DistributedBackend) DeleteObject(bucket, key string) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}

	// Delete local data
	os.Remove(b.objectPath(bucket, key))

	// Delete data from peer nodes (distributed GC)
	if b.shardSvc != nil {
		ctx := context.Background()
		for _, peer := range b.allNodes {
			if peer == b.node.ID() {
				continue
			}
			if err := b.shardSvc.DeleteShards(ctx, peer, bucket, key); err != nil {
				b.logger.Warn("remote shard delete failed", "peer", peer, "bucket", bucket, "key", key, "error", err)
			}
		}
	}

	return b.propose(context.Background(), CmdDeleteObject, DeleteObjectCmd{
		Bucket: bucket,
		Key:    key,
	})
}

func (b *DistributedBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
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
			var obj storage.Object
			err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
				}
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

// --- Multipart operations ---

func (b *DistributedBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	uploadID := uuid.New().String()
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()

	err := b.propose(context.Background(), CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	})
	if err != nil {
		os.RemoveAll(b.partDir(uploadID))
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

func (b *DistributedBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	// Verify upload exists (read local metadata)
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

	// Data write is local — no Raft needed for part data
	partFile := b.partPath(uploadID, partNumber)
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

func (b *DistributedBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	// Read upload metadata
	var meta clusterMultipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalClusterMultipartMeta(val)
			if err != nil {
				return err
			}
			meta = m
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Sort parts and assemble locally
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	objPath := b.objectPath(bucket, key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	out, err := os.Create(objPath)
	if err != nil {
		return nil, fmt.Errorf("create final object: %w", err)
	}

	h := md5.New()
	mw := io.MultiWriter(out, h)
	var totalSize int64

	for _, p := range parts {
		partFile := b.partPath(uploadID, p.PartNumber)
		f, err := os.Open(partFile)
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("open part %d: %w", p.PartNumber, err)
		}
		n, err := io.Copy(mw, f)
		f.Close()
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("copy part %d: %w", p.PartNumber, err)
		}
		totalSize += n
	}
	out.Close()

	etag := hex.EncodeToString(h.Sum(nil))
	now := time.Now().Unix()

	err = b.propose(context.Background(), CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:      bucket,
		Key:         key,
		UploadID:    uploadID,
		Size:        totalSize,
		ContentType: meta.ContentType,
		ETag:        etag,
		ModTime:     now,
	})
	if err != nil {
		return nil, err
	}

	os.RemoveAll(b.partDir(uploadID))

	return &storage.Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  meta.ContentType,
		ETag:         etag,
		LastModified: now,
	}, nil
}

func (b *DistributedBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
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

	os.RemoveAll(b.partDir(uploadID))

	return b.propose(context.Background(), CmdAbortMultipart, AbortMultipartCmd{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	})
}

// --- Path helpers ---

func (b *DistributedBackend) bucketDir(bucket string) string {
	return filepath.Join(b.root, "data", bucket)
}

func (b *DistributedBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.root, "data", bucket, key)
}

func (b *DistributedBackend) partDir(uploadID string) string {
	return filepath.Join(b.root, "parts", uploadID)
}

func (b *DistributedBackend) partPath(uploadID string, partNumber int) string {
	return filepath.Join(b.partDir(uploadID), fmt.Sprintf("%05d", partNumber))
}
