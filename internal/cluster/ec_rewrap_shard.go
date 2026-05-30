package cluster

import (
	"fmt"
	"os"

	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// rewrapTestHook, when non-nil, runs between the gen-stale shard's plaintext
// read and its re-seal. Tests use it to widen the read-modify-write window and
// prove the per-shard write lock makes the rewrap atomic versus a concurrent
// config-upgrade re-split. nil in production.
var rewrapTestHook func()

// RewrapShardIfStale migrates a single owned EC shard onto activeGen if its
// on-disk DEK generation differs. It is idempotent (a shard already at activeGen
// is a no-op) and multi-gen-safe (it inspects only the shard's own header, never
// the rotation's oldGen). Returns true iff the shard was re-encrypted.
//
// The whole operation runs under the per-(bucket, canonicalKey) shard write lock
// so it serialises with WriteShard and the config-upgrade re-split writer that
// target the same path. Every primitive it calls below the lock takes NO
// DistributedBackend shard lock — the lock here is non-reentrant, so re-locking
// (e.g. via ReadShard/readShardIntegrity/WriteShard) would self-deadlock.
func (b *DistributedBackend) RewrapShardIfStale(bucket, key, versionID string, shardIdx int, activeGen uint32) (bool, error) {
	canonicalKey := ecObjectShardKey(key, versionID)
	unlock := b.acquireShardWriteLock(bucket, canonicalKey)
	defer unlock()

	// Gen probe only: locate + read the raw shard bytes to inspect the header.
	raw, path, err := b.readOwnedShardRaw(bucket, canonicalKey, shardIdx)
	if err != nil {
		return false, err
	}
	gen, ok := eccodec.EncryptedShardGen(raw)
	if !ok {
		// Plaintext shard or not a shard header — nothing to rewrap.
		return false, nil
	}
	if gen == activeGen {
		// Already migrated.
		return false, nil
	}

	// Decode the plaintext at the shard's own gen (header-driven, no lock).
	plain, err := b.shardSvc.ReadLocalShard(bucket, canonicalKey, shardIdx)
	if err != nil {
		return false, fmt.Errorf("rewrap read shard %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
	}
	if rewrapTestHook != nil {
		rewrapTestHook()
	}
	// Re-seal at the active gen.
	encoded, err := b.shardSvc.EncodeEncryptedShardBuffer(bucket, canonicalKey, shardIdx, plain)
	if err != nil {
		return false, fmt.Errorf("rewrap encode shard %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
	}
	if err := b.writeEncodedShard(bucket, canonicalKey, shardIdx, path, encoded); err != nil {
		return false, fmt.Errorf("rewrap write shard %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
	}
	return true, nil
}

// readOwnedShardRaw locates and returns the raw on-disk shard bytes (pack-first,
// else the standalone file) together with the canonical standalone path used for
// a standalone re-write. It exists ONLY to feed eccodec.EncryptedShardGen and so
// takes NO shard lock and does NOT call ReadShard/readShardIntegrity (those take
// acquireShardReadLock on the same non-reentrant mutex the caller write-holds,
// which would deadlock). The returned path is always the canonical standalone
// location; the pack-resident case never reads it.
func (b *DistributedBackend) readOwnedShardRaw(bucket, canonicalKey string, shardIdx int) ([]byte, string, error) {
	path, err := b.shardSvc.getShardPath(bucket, canonicalKey, shardIdx)
	if err != nil {
		return nil, "", fmt.Errorf("rewrap locate shard %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
	}
	if b.shardSvc.shardPack != nil {
		if raw, ok, err := b.shardSvc.shardPack.get(bucket, canonicalKey, shardIdx); err != nil {
			return nil, "", fmt.Errorf("rewrap read pack shard %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
		} else if ok {
			return raw, path, nil
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("rewrap read shard file %s/%s/%d: %w", bucket, canonicalKey, shardIdx, err)
	}
	return raw, path, nil
}
