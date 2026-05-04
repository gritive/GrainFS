package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

// RepairReplica fetches the named non-EC object from a healthy peer and
// atomically rewrites the local copy. Used by scrubber.BlockVerifier when
// local verification detects missing or corrupt replicated objects (volume
// blocks).
//
// Strategy:
//  1. headObjectMeta → expected ETag, versionID
//  2. iterate liveNodes() with peerHealth priority
//  3. shardSvc.ReadShard(peer, bucket, shardKey, 0) — shardKey = key+"/"+versionID
//     (matches the shape used by replicating writes in putObjectNxSpooled)
//  4. compute MD5; require match against meta.ETag
//  5. atomic write (tmp + fsync + rename) to objectPathV(bucket, key, versionID)
//  6. error if no peer returned matching bytes
//
// Single-node clusters (no shardSvc) are unsupported — there is no peer to
// pull from. Coverage: integration test in scrubber_volume_integration_test.go
// (Task 13 of the Volume Scrub plan).
func (b *DistributedBackend) RepairReplica(ctx context.Context, bucket, key string) error {
	obj, _, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("repair replica head: %w", err)
	}
	versionID := obj.VersionID
	if versionID == "" {
		versionID = "current"
	}
	expectedETag := obj.ETag
	if expectedETag == "" {
		return fmt.Errorf("repair replica: missing expected ETag for %s/%s", bucket, key)
	}
	if b.shardSvc == nil {
		return fmt.Errorf("repair replica: no shard service (single-node mode)")
	}
	shardKey := key + "/" + versionID

	peers := b.liveNodes()
	tried := 0
	// Healthy peers first.
	for _, peer := range peers {
		if peer == b.selfAddr {
			continue
		}
		if b.peerHealth != nil && !b.peerHealth.IsHealthy(peer) {
			continue
		}
		if data, ok := b.tryRepairFromPeer(ctx, peer, bucket, shardKey, expectedETag); ok {
			return b.writeRepairedReplica(bucket, key, versionID, data)
		}
		tried++
	}
	// Fallback: unhealthy peers (they may have recovered).
	for _, peer := range peers {
		if peer == b.selfAddr {
			continue
		}
		if b.peerHealth != nil && b.peerHealth.IsHealthy(peer) {
			continue
		}
		if data, ok := b.tryRepairFromPeer(ctx, peer, bucket, shardKey, expectedETag); ok {
			return b.writeRepairedReplica(bucket, key, versionID, data)
		}
		tried++
	}
	return fmt.Errorf("repair replica %s/%s: no peer returned matching bytes (tried %d)", bucket, key, tried)
}

func (b *DistributedBackend) tryRepairFromPeer(ctx context.Context, peer, bucket, shardKey, expectedETag string) ([]byte, bool) {
	data, err := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
	if err != nil || data == nil {
		if b.peerHealth != nil && err != nil {
			b.peerHealth.MarkUnhealthy(peer)
		}
		return nil, false
	}
	h := md5.Sum(data)
	if hex.EncodeToString(h[:]) != expectedETag {
		return nil, false
	}
	if b.peerHealth != nil {
		b.peerHealth.MarkHealthy(peer)
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
