package cluster

// Slice 2 of refactor/unify-storage-paths: Scrubbable interface on
// DistributedBackend. The scrubber lives in internal/scrubber and, until
// Slice 3 wires it in, has no cluster-mode caller. Slice 2 is the plumbing.
//
// Open issues carried to later slices:
//   - CRC32 footer. The interface comment promises "verifying its CRC32 footer"
//     on ReadShard, but cluster shards (written by ShardService.WriteLocalShard
//     and friends) are raw bytes with no footer. Retrofitting requires changing
//     every caller of Write/ReadLocalShard (putObjectEC, RepairShard, getObjectEC,
//     the QUIC handlers), which is explicitly outside Slice 2's scope. ReadShard
//     and WriteShard below are plain atomic I/O until Slice 3 (or a dedicated
//     slice) retrofits the shard_service.go path through eccodec.
//   - IterObjectMetas parses versioned `obj:{bucket}/{key}/{versionID}` keys
//     with a first-slash split, folding the versionID into the key. See the
//     comment on that function for the migration note. ScanObjects below
//     sidesteps the issue entirely by iterating `lat:` pointers instead.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// acquireShardWriteLock serializes ReadShard/WriteShard on (bucket, key) for
// this backend instance only. Lock state lives on the backend so multiple
// backends in the same process (tests, future multi-tenant) do not alias.
func (b *DistributedBackend) acquireShardWriteLock(bucket, key string) func() {
	lockKey := bucket + "\x00" + key
	mu, _ := b.shardLocks.LoadOrStore(lockKey, &sync.RWMutex{})
	mu.(*sync.RWMutex).Lock()
	return func() { mu.(*sync.RWMutex).Unlock() }
}

func (b *DistributedBackend) acquireShardReadLock(bucket, key string) func() {
	lockKey := bucket + "\x00" + key
	mu, _ := b.shardLocks.LoadOrStore(lockKey, &sync.RWMutex{})
	mu.(*sync.RWMutex).RLock()
	return func() { mu.(*sync.RWMutex).RUnlock() }
}

// ScanObjects streams one scrubber.ObjectRecord per live EC object in the
// bucket. It iterates `lat:{bucket}/{key}` pointers — each entry gives the
// exact object key (unambiguous even when S3 keys contain '/') and the
// latest VersionID. Legacy unversioned data (no `lat:` pointer) is skipped;
// EC is a cluster-mode feature and every EC object carries versioning.
//
// Delete markers (tombstones) and non-EC clusters return no records — EC has
// no shards to scrub in those cases.
func (b *DistributedBackend) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	ch := make(chan scrubber.ObjectRecord, 64)

	// If EC isn't active for this cluster, there are no shards to scrub.
	if !b.ECActive() {
		close(ch)
		return ch, nil
	}

	dataShards := b.ecConfig.DataShards
	parityShards := b.ecConfig.ParityShards

	go func() {
		defer close(ch)
		latPrefix := []byte("lat:" + bucket + "/")

		_ = b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(latPrefix); it.ValidForPrefix(latPrefix); it.Next() {
				item := it.Item()
				key := string(item.Key()[len(latPrefix):])

				var versionID string
				if err := item.Value(func(v []byte) error {
					versionID = string(v)
					return nil
				}); err != nil || versionID == "" {
					continue
				}

				metaItem, err := txn.Get(objectMetaKeyV(bucket, key, versionID))
				if err != nil {
					continue
				}
				var meta objectMeta
				if err := metaItem.Value(func(v []byte) error {
					var derr error
					meta, derr = unmarshalObjectMeta(v)
					return derr
				}); err != nil {
					continue
				}
				if meta.ETag == deleteMarkerETag {
					continue // tombstone — no shards to scrub
				}

				ch <- scrubber.ObjectRecord{
					Bucket:       bucket,
					Key:          key,
					VersionID:    versionID,
					DataShards:   dataShards,
					ParityShards: parityShards,
					ETag:         meta.ETag,
					LastModified: meta.LastModified,
				}
			}
			return nil
		})
	}()

	return ch, nil
}

// ObjectExists returns true if key resolves to a live (non-tombstone)
// version. Used by the scrubber to avoid repairing shards for an object
// deleted between scan and verify (Eng Review #9).
func (b *DistributedBackend) ObjectExists(bucket, key string) (bool, error) {
	_, err := b.HeadObject(bucket, key)
	if err == nil {
		return true, nil
	}
	if err == storage.ErrObjectNotFound || err == storage.ErrBucketNotFound {
		return false, nil
	}
	return false, err
}

// ShardPaths returns the on-disk path for each shard of the versioned
// object, matching the layout putObjectEC writes to via ShardService:
//
//	{dataDir}/shards/{bucket}/{key}/{versionID}/shard_{N}
//
// dataDir is the cluster backend's root. The shardSvc's dataDir is
// root/shards/; putObjectEC composes shardKey as `key/versionID` before
// handing it to ShardService, so the physical path includes the version.
func (b *DistributedBackend) ShardPaths(bucket, key, versionID string, totalShards int) []string {
	base := filepath.Join(b.root, "shards", bucket, key, versionID)
	paths := make([]string, totalShards)
	for i := 0; i < totalShards; i++ {
		paths[i] = filepath.Join(base, fmt.Sprintf("shard_%d", i))
	}
	return paths
}

// ReadShard reads a shard at path under a per-object read lock. Cluster
// shards are raw bytes today (see file-header note on CRC retrofit). When
// the retrofit lands, this becomes eccodec.ReadShardVerified.
func (b *DistributedBackend) ReadShard(bucket, key, path string) ([]byte, error) {
	unlock := b.acquireShardReadLock(bucket, key)
	defer unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read shard: %w", err)
	}
	if b.shardSvc != nil {
		aad := shardAAD(bucket, key, path)
		data, err = b.shardSvc.DecryptPayload(data, aad)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// WriteShard writes data atomically at path under a per-object write lock.
// The write uses tmp → fsync → rename so a crash mid-repair cannot leave a
// torn shard. Matches the raw-bytes format produced by WriteLocalShard so
// the shard written here is readable by the non-scrubber code path.
func (b *DistributedBackend) WriteShard(bucket, key, path string, data []byte) error {
	unlock := b.acquireShardWriteLock(bucket, key)
	defer unlock()
	payload := data
	if b.shardSvc != nil {
		var err error
		aad := shardAAD(bucket, key, path)
		payload, err = b.shardSvc.EncryptPayload(data, aad)
		if err != nil {
			return fmt.Errorf("encrypt shard: %w", err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir shard dir: %w", err)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp shard: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync tmp shard: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return nil
}

// shardAAD builds the AAD string for the given shard path.
// path must end with /shard_N; if parsing fails an empty slice is returned.
// Must match the AAD used by WriteLocalShard/ReadLocalShard.
func shardAAD(bucket, key, path string) []byte {
	// filepath.Base(path) = "shard_N"
	base := filepath.Base(path)
	idxStr := strings.TrimPrefix(base, "shard_")
	return []byte(bucket + "/" + key + "/" + idxStr)
}

// NodeID implements scrubber.ShardOwner. Returns the raft address (selfAddr)
// stored by SetShardService — the same identifier written into shard placement
// vectors by putObjectEC — so OwnedShards comparisons work correctly.
// Returns "" before SetShardService is called (scrubber skips all objects in
// that state, which is safe because production always calls SetShardService
// before starting the scrubber).
func (b *DistributedBackend) NodeID() string {
	return b.selfAddr
}

// RaftNodeID returns this node's Raft node name (b.node.ID()). This is the
// human-readable name configured with --node-id, distinct from the raft
// address stored in shard placement vectors. Use NodeID() for scrubber
// filtering; use RaftNodeID() for admin tooling and log messages.
func (b *DistributedBackend) RaftNodeID() string {
	if b.node == nil {
		return ""
	}
	return b.node.ID()
}

// OwnedShards implements scrubber.ShardOwner. Returns the shard indices of
// placement that match the supplied nodeID. Returns nil when the object has no
// placement record (non-EC / N× path) or when nodeID does not appear in the
// placement vector.
// versionID is used to construct the shardKey (key+"/"+versionID) matching
// the placement record written by putObjectEC. Empty versionID falls back to
// bare key lookup for legacy pre-versioned EC objects.
func (b *DistributedBackend) OwnedShards(bucket, key, versionID, nodeID string) []int {
	lookupKey := key
	if versionID != "" {
		lookupKey = key + "/" + versionID
	}
	placement, ok := b.fsm.LookupShardPlacement(bucket, lookupKey)
	if !ok {
		return nil
	}
	var owned []int
	for i, holder := range placement {
		if holder == nodeID {
			owned = append(owned, i)
		}
	}
	return owned
}

// RepairShardLocal is a context-less wrapper around RepairShard for the
// scrubber, which has no request ctx of its own. Implements
// scrubber.ShardRepairer. versionID is threaded through so the repair reads
// and writes the correct on-disk path (shardKey = key + "/" + versionID).
func (b *DistributedBackend) RepairShardLocal(bucket, key, versionID string, shardIdx int) error {
	return b.RepairShard(context.Background(), bucket, key, versionID, shardIdx)
}
