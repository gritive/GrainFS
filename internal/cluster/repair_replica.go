package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
)

// RepairReplica fetches the named replicated object from a healthy peer and
// atomically rewrites the local copy. Used by scrubber.BlockVerifier when
// local verification detects missing or corrupt replicated objects (volume
// blocks).
//
// Strategy:
//  1. headObjectMeta → expected ETag, versionID
//  2. iterate liveNodes() with peerHealth priority
//  3. shardSvc.ReadShard(peer, bucket, shardKey, 0) — shardIdx 0, replicated objects are single-shard
//  4. verify ETag (MD5 or xxhash3 by length); require match against meta.ETag
//  5. atomic write (tmp + fsync + rename) to objectPathV(bucket, key, versionID)
//  6. error if no peer returned matching bytes
//
// Single-node clusters (no shardSvc) are unsupported — there is no peer to
// pull from. Coverage: integration test in scrubber_volume_integration_test.go
// (Task 13 of the Volume Scrub plan).
// peerReader is the minimal interface RepairReplica needs to fetch a candidate
// replica from another node. Satisfied by *ShardService. Extracted so that
// repairReplicaWith can be unit-tested without standing up a real QUIC stream
// service or raft cluster.
type peerReader interface {
	ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error)
}

func (b *DistributedBackend) RepairReplica(ctx context.Context, bucket, key string) error {
	obj, _, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("repair replica head: %w", err)
	}
	return b.repairReplicaWith(ctx, b.shardSvc, bucket, key, obj.VersionID, obj.ETag)
}

// repairReplicaWith runs the orchestration that RepairReplica wraps after the
// HEAD lookup. Split out so unit tests can exercise the full guard +
// peer-iteration + ETag verify (MD5/xxhash3) + write path without hitting BadgerDB or QUIC.
//
// Strategy:
//  1. iterate b.liveNodes() with peerHealth priority (healthy first)
//  2. reader.ReadShard(peer, bucket, shardKey, 0) — shardKey = key+"/"+versionID
//  3. verify ETag (MD5 or xxhash3 by length); require match against expectedETag
//  4. atomic write (tmp + fsync + rename) to objectPathV(bucket, key, versionID)
//  5. error if no peer returned matching bytes
//
// Single-node clusters (reader == nil) are unsupported — there is no peer to
// pull from. Coverage: unit tests in repair_replica_test.go + integration via
// tests/e2e/volume_scrub_multinode_test.go.
func (b *DistributedBackend) repairReplicaWith(ctx context.Context, reader peerReader, bucket, key, versionID, expectedETag string) error {
	if reader == nil {
		return fmt.Errorf("repair replica: no shard service (single-node mode)")
	}
	if expectedETag == "" {
		return fmt.Errorf("repair replica: missing expected ETag for %s/%s", bucket, key)
	}
	if versionID == "" {
		versionID = "current"
	}
	shardKey := key + "/" + versionID

	peers := b.liveNodes()
	tried := 0
	for _, peer := range peers {
		if peer == b.currentSelfAddr() {
			continue
		}
		if b.currentPeerHealth() != nil && !b.currentPeerHealth().IsHealthy(peer) {
			continue
		}
		if data, ok := b.tryRepairFromPeer(ctx, reader, peer, bucket, shardKey, expectedETag); ok {
			return b.writeRepairedReplica(bucket, key, versionID, data)
		}
		tried++
	}
	for _, peer := range peers {
		if peer == b.currentSelfAddr() {
			continue
		}
		if b.currentPeerHealth() != nil && b.currentPeerHealth().IsHealthy(peer) {
			continue
		}
		if data, ok := b.tryRepairFromPeer(ctx, reader, peer, bucket, shardKey, expectedETag); ok {
			return b.writeRepairedReplica(bucket, key, versionID, data)
		}
		tried++
	}
	return fmt.Errorf("repair replica %s/%s: no peer returned matching bytes (tried %d)", bucket, key, tried)
}

func (b *DistributedBackend) tryRepairFromPeer(ctx context.Context, reader peerReader, peer, bucket, shardKey, expectedETag string) ([]byte, bool) {
	data, err := reader.ReadShard(ctx, peer, bucket, shardKey, 0)
	if err != nil || data == nil {
		if b.currentPeerHealth() != nil && err != nil {
			b.currentPeerHealth().MarkUnhealthy(peer)
		}
		return nil, false
	}
	var computed string
	switch len(expectedETag) {
	case 32: // MD5
		h := md5.Sum(data)
		computed = hex.EncodeToString(h[:])
	case 16: // xxhash3
		computed = storage.InternalETag(data)
	default:
		return nil, false
	}
	if computed != expectedETag {
		return nil, false
	}
	if b.currentPeerHealth() != nil {
		b.currentPeerHealth().MarkHealthy(peer)
	}
	return data, true
}

func (b *DistributedBackend) writeRepairedReplica(bucket, key, versionID string, data []byte) error {
	objPath := b.objectPathV(bucket, key, versionID)
	if err := writeFileAtomicFromReader(objPath, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("repair replica write %s: %w", objPath, err)
	}
	return nil
}
