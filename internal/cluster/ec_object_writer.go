package cluster

import (
	"context"
	"fmt"
	"io"
	"time"

	"golang.org/x/sync/errgroup"
)

type ecObjectShardStore interface {
	WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error
	WriteShardStream(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader) error
	DeleteLocalShards(bucket, key string) error
	DeleteShards(ctx context.Context, peer, bucket, key string) error
}

type ecObjectPeerHealth interface {
	MarkHealthy(peer string) bool
	MarkUnhealthy(peer string) bool
}

type ecObjectWriter struct {
	selfID        string
	shards        ecObjectShardStore
	peerHealth    ecObjectPeerHealth
	writeAttempts int
	writeBackoff  time.Duration
}

type ecObjectWritePlan struct {
	Bucket           string
	Key              string
	VersionID        string
	PlacementGroupID string
	Config           ECConfig
	Placement        []string
	RingVersion      RingVersion
	ContentType      string
}

type ecObjectWriteResult struct {
	Size        int64
	ETag        string
	ModTime     int64
	ShardKey    string
	Placement   []string
	RingVersion RingVersion
	ECData      uint8
	ECParity    uint8
}

func newECObjectWriter(selfID string, shards ecObjectShardStore, peerHealth ecObjectPeerHealth) ecObjectWriter {
	return ecObjectWriter{
		selfID:        selfID,
		shards:        shards,
		peerHealth:    peerHealth,
		writeAttempts: ecShardWriteAttempts,
		writeBackoff:  ecShardWriteBackoff,
	}
}

func (w ecObjectWriter) writeShardReaders(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	metricPath string,
) (ecObjectWriteResult, error) {
	shardKey := plan.Key + "/" + plan.VersionID
	written := make(chan string, len(plan.Placement))

	cleanup := func() {
		close(written)
		for node := range written {
			if node == w.selfID {
				_ = w.shards.DeleteLocalShards(plan.Bucket, shardKey)
			} else {
				_ = w.shards.DeleteShards(ctx, node, plan.Bucket, shardKey)
			}
		}
	}

	stageStart := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	for i, node := range plan.Placement {
		i, node := i, node
		g.Go(func() error {
			shardStageStart := time.Now()
			var werr error
			if node == w.selfID {
				body, err := openShard(i)
				if err != nil {
					return fmt.Errorf("open ec shard %d: %w", i, err)
				}
				werr = w.shards.WriteLocalShardStream(plan.Bucket, shardKey, i, body)
				observePutStage("ec_write_shard", "local", shardStageStart)
			} else {
				werr = w.writeRemoteShard(gctx, openShard, i, node, plan.Bucket, shardKey)
				observePutStage("ec_write_shard", "remote", shardStageStart)
				if w.peerHealth != nil {
					if werr != nil {
						w.peerHealth.MarkUnhealthy(node)
					} else {
						w.peerHealth.MarkHealthy(node)
					}
				}
			}
			if werr != nil {
				return fmt.Errorf("ec write shard %d to %s: %w", i, node, werr)
			}
			written <- node
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return ecObjectWriteResult{}, err
	}
	observePutStage(metricPath, "write_shards", stageStart)
	close(written)

	return ecObjectWriteResult{
		Size:        sp.Size,
		ETag:        sp.ETag,
		ModTime:     time.Now().Unix(),
		ShardKey:    shardKey,
		Placement:   cloneStringSlice(plan.Placement),
		RingVersion: plan.RingVersion,
		ECData:      uint8(plan.Config.DataShards),
		ECParity:    uint8(plan.Config.ParityShards),
	}, nil
}

func (w ecObjectWriter) writeRemoteShard(ctx context.Context, openShard func(int) (io.Reader, error), shardIdx int, node, bucket, shardKey string) error {
	attempts := w.writeAttempts
	if attempts <= 0 {
		attempts = ecShardWriteAttempts
	}
	backoff := w.writeBackoff
	if backoff <= 0 {
		backoff = ecShardWriteBackoff
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		body, err := openShard(shardIdx)
		if err != nil {
			return fmt.Errorf("open ec shard %d: %w", shardIdx, err)
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, shardRPCTimeout)
		err = w.shards.WriteShardStream(writeCtx, node, bucket, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
		if closer, ok := body.(io.Closer); ok {
			if closeErr := closer.Close(); err == nil && closeErr != nil {
				err = fmt.Errorf("close ec shard %d: %w", shardIdx, closeErr)
			}
		}
		writeCancel()
		if err == nil {
			return nil
		}

		lastErr = err
		if ctx.Err() != nil || attempt == attempts {
			return lastErr
		}

		timer := time.NewTimer(time.Duration(attempt) * backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
	}
	return lastErr
}
