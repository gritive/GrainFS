package cluster

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotEC            = errors.New("not EC-managed")
	ErrPlacementCorrupt = errors.New("placement metadata corrupt")
)

type PlacementSource string

const (
	PlacementSourceRing     PlacementSource = "ring"
	PlacementSourceMetadata PlacementSource = "metadata"
	PlacementSourceLegacy   PlacementSource = "legacy"
)

type PlacementMeta struct {
	VersionID   string
	RingVersion RingVersion
	ECData      uint8
	ECParity    uint8
	NodeIDs     []string
}

type ResolvedPlacement struct {
	Bucket    string
	Key       string
	VersionID string
	ShardKey  string
	Record    PlacementRecord
	Source    PlacementSource
}

type PlacementResolver interface {
	ResolvePlacement(ctx context.Context, bucket, key string, meta PlacementMeta) (ResolvedPlacement, error)
}

func (b *DistributedBackend) ResolvePlacement(ctx context.Context, bucket, key string, meta PlacementMeta) (ResolvedPlacement, error) {
	if err := ctx.Err(); err != nil {
		return ResolvedPlacement{}, err
	}

	shardKey := key
	if meta.VersionID != "" {
		shardKey = key + "/" + meta.VersionID
	}
	base := ResolvedPlacement{
		Bucket:    bucket,
		Key:       key,
		VersionID: meta.VersionID,
		ShardKey:  shardKey,
	}

	if meta.RingVersion > 0 {
		cfg, err := placementConfig(meta)
		if err != nil {
			return ResolvedPlacement{}, fmt.Errorf("%w: %s/%s ring metadata: %v", ErrPlacementCorrupt, bucket, key, err)
		}
		ring, err := b.fsm.GetRingStore().GetRing(meta.RingVersion)
		if err != nil {
			return ResolvedPlacement{}, fmt.Errorf("%w: ring %d for %s/%s: %v", ErrPlacementCorrupt, meta.RingVersion, bucket, key, err)
		}
		nodes := ring.PlacementForKey(cfg, shardKey)
		if len(nodes) != cfg.NumShards() {
			return ResolvedPlacement{}, fmt.Errorf("%w: ring placement length %d != k+m %d for %s/%s", ErrPlacementCorrupt, len(nodes), cfg.NumShards(), bucket, key)
		}
		base.Record = PlacementRecord{Nodes: nodes, K: cfg.DataShards, M: cfg.ParityShards}
		base.Source = PlacementSourceRing
		return base, nil
	}

	if len(meta.NodeIDs) > 0 {
		cfg, err := placementConfig(meta)
		if err != nil {
			return ResolvedPlacement{}, fmt.Errorf("%w: %s/%s metadata: %v", ErrPlacementCorrupt, bucket, key, err)
		}
		if len(meta.NodeIDs) != cfg.NumShards() {
			return ResolvedPlacement{}, fmt.Errorf("%w: metadata placement length %d != k+m %d for %s/%s", ErrPlacementCorrupt, len(meta.NodeIDs), cfg.NumShards(), bucket, key)
		}
		base.Record = PlacementRecord{Nodes: meta.NodeIDs, K: cfg.DataShards, M: cfg.ParityShards}
		base.Source = PlacementSourceMetadata
		return base, nil
	}

	if b.fsm != nil {
		if rec, err := b.fsm.LookupShardPlacement(bucket, shardKey); err == nil && len(rec.Nodes) > 0 {
			base.Record = rec
			base.Source = PlacementSourceLegacy
			return base, nil
		} else if err != nil {
			return ResolvedPlacement{}, fmt.Errorf("lookup shard placement: %w", err)
		}
		if meta.VersionID != "" {
			if rec, err := b.fsm.LookupShardPlacement(bucket, key); err == nil && len(rec.Nodes) > 0 {
				base.ShardKey = key
				base.Record = rec
				base.Source = PlacementSourceLegacy
				return base, nil
			} else if err != nil {
				return ResolvedPlacement{}, fmt.Errorf("lookup bare shard placement: %w", err)
			}
		}
	}

	return ResolvedPlacement{}, fmt.Errorf("%w: no placement for %s/%s", ErrNotEC, bucket, shardKey)
}

func placementConfig(meta PlacementMeta) (ECConfig, error) {
	if meta.ECData == 0 {
		return ECConfig{}, errors.New("ECData is zero")
	}
	if meta.ECParity == 0 {
		return ECConfig{}, errors.New("ECParity is zero")
	}
	cfg := ECConfig{DataShards: int(meta.ECData), ParityShards: int(meta.ECParity)}
	if cfg.ParityShards < 0 || cfg.NumShards() <= 0 {
		return ECConfig{}, fmt.Errorf("invalid EC config k=%d m=%d", cfg.DataShards, cfg.ParityShards)
	}
	return cfg, nil
}
