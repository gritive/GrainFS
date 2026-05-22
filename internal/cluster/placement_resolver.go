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

type PlacementMeta struct {
	VersionID        string
	ECData           uint8
	ECParity         uint8
	NodeIDs          []string
	PlacementGroupID string
}

type ResolvedPlacement struct {
	Bucket    string
	Key       string
	VersionID string
	ShardKey  string
	Record    PlacementRecord
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

	if len(meta.NodeIDs) == 0 {
		return ResolvedPlacement{}, fmt.Errorf("%w: no placement for %s/%s", ErrNotEC, bucket, shardKey)
	}

	cfg, err := placementConfig(meta)
	if err != nil {
		return ResolvedPlacement{}, fmt.Errorf("%w: %s/%s metadata: %v", ErrPlacementCorrupt, bucket, key, err)
	}
	if len(meta.NodeIDs) != cfg.NumShards() {
		return ResolvedPlacement{}, fmt.Errorf("%w: metadata placement length %d != k+m %d for %s/%s",
			ErrPlacementCorrupt, len(meta.NodeIDs), cfg.NumShards(), bucket, key)
	}
	base.Record = PlacementRecord{Nodes: meta.NodeIDs, K: cfg.DataShards, M: cfg.ParityShards}
	return base, nil
}

func placementConfig(meta PlacementMeta) (ECConfig, error) {
	if meta.ECData == 0 {
		return ECConfig{}, errors.New("ECData is zero")
	}
	cfg := ECConfig{DataShards: int(meta.ECData), ParityShards: int(meta.ECParity)}
	if cfg.ParityShards < 0 || cfg.NumShards() <= 0 {
		return ECConfig{}, fmt.Errorf("invalid EC config k=%d m=%d", cfg.DataShards, cfg.ParityShards)
	}
	return cfg, nil
}
