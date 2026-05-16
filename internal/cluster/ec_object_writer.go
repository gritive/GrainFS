package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"time"

	"golang.org/x/sync/errgroup"
)

type ecObjectShardStore interface {
	WriteLocalShard(bucket, key string, shardIdx int, data []byte) error
	WriteLocalShardContext(ctx context.Context, bucket, key string, shardIdx int, data []byte) error
	WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error
	WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error
	WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error
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
	UserMetadata     map[string]string
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

type ecObjectShardWriteError struct {
	shardIdx int
	node     string
	err      error
}

func (e *ecObjectShardWriteError) Error() string {
	return fmt.Sprintf("ec write shard %d to %s: %v", e.shardIdx, e.node, e.err)
}

func (e *ecObjectShardWriteError) Unwrap() error {
	return e.err
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

//nolint:unused // referenced by ec_object_writer_test.go.
func (w ecObjectWriter) writeShardReaders(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	metricPath string,
) (ecObjectWriteResult, error) {
	return w.writeShardReadersWithSize(ctx, plan, sp, openShard, nil, metricPath)
}

func (w ecObjectWriter) writeMemoryShards(ctx context.Context, plan ecObjectWritePlan, sp *spooledObject) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	src, err := sp.Open()
	if err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("open spooled object: %w", err)
	}
	data, readErr := io.ReadAll(src)
	closeErr := src.Close()
	if readErr != nil {
		return ecObjectWriteResult{}, fmt.Errorf("read spooled object: %w", readErr)
	}
	if closeErr != nil {
		return ecObjectWriteResult{}, fmt.Errorf("close spooled object: %w", closeErr)
	}
	if int64(len(data)) != sp.Size {
		return ecObjectWriteResult{}, fmt.Errorf("read spooled object: got %d bytes, expected %d", len(data), sp.Size)
	}
	observePutStage("ec_memory", "read_object", stageStart)

	stageStart = time.Now()
	shards, err := ECSplit(plan.Config, data)
	clear(data)
	if err != nil {
		return ecObjectWriteResult{}, err
	}
	observePutStage("ec_memory", "split_encode", stageStart)
	defer func() {
		for _, shard := range shards {
			clear(shard)
		}
	}()

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		if idx < 0 || idx >= len(shards) {
			return nil, fmt.Errorf("ec memory shard %d out of range", idx)
		}
		return bytes.NewReader(shards[idx]), nil
	}, func(idx int) (int64, error) {
		if idx < 0 || idx >= len(shards) {
			return 0, fmt.Errorf("ec memory shard %d out of range", idx)
		}
		return int64(len(shards[idx])), nil
	}, "ec_memory")
}

func (w ecObjectWriter) writeDataShards(ctx context.Context, plan ecObjectWritePlan, data []byte) (ecObjectWriteResult, error) {
	shards, err := ECSplit(plan.Config, data)
	if err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("ec split: %w", err)
	}
	h := md5.Sum(data)
	sp := &spooledObject{
		Size: int64(len(data)),
		ETag: hex.EncodeToString(h[:]),
	}

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		if idx < 0 || idx >= len(shards) {
			return nil, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return bytes.NewReader(shards[idx]), nil
	}, func(idx int) (int64, error) {
		if idx < 0 || idx >= len(shards) {
			return 0, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return int64(len(shards[idx])), nil
	}, "ec")
}

func (w ecObjectWriter) writeSpooledShards(ctx context.Context, plan ecObjectWritePlan, spoolDir string, sp *spooledObject) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	shards, err := spoolECShards(ctx, plan.Config, spoolDir, sp)
	if err != nil {
		return ecObjectWriteResult{}, err
	}
	observePutStage("ec", "spool_shards", stageStart)
	defer shards.Cleanup()

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		return shards.OpenShard(idx)
	}, shards.ShardSize, "ec")
}

func (w ecObjectWriter) writeShardReadersWithSize(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	shardSize func(idx int) (int64, error),
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
				if shardSize != nil {
					if size, sizeErr := shardSize(i); sizeErr == nil && size <= ecShardBufferedLimit {
						var data []byte
						data, werr = io.ReadAll(body)
						if closer, ok := body.(io.Closer); ok {
							if closeErr := closer.Close(); werr == nil && closeErr != nil {
								werr = fmt.Errorf("close ec shard %d: %w", i, closeErr)
							}
						}
						if werr == nil {
							werr = w.shards.WriteLocalShardContext(gctx, plan.Bucket, shardKey, i, data)
						}
					} else {
						werr = w.shards.WriteLocalShardStreamContext(gctx, plan.Bucket, shardKey, i, body)
						if closer, ok := body.(io.Closer); ok {
							if closeErr := closer.Close(); werr == nil && closeErr != nil {
								werr = fmt.Errorf("close ec shard %d: %w", i, closeErr)
							}
						}
					}
				} else {
					werr = w.shards.WriteLocalShardStreamContext(gctx, plan.Bucket, shardKey, i, body)
					if closer, ok := body.(io.Closer); ok {
						if closeErr := closer.Close(); werr == nil && closeErr != nil {
							werr = fmt.Errorf("close ec shard %d: %w", i, closeErr)
						}
					}
				}
				observePutStage("ec_write_shard", "local", shardStageStart)
				ObservePutTraceStage(ctx, PutTraceStageShardWriteLocal, shardStageStart, PutTraceStageFields{
					ShardIndex:       i,
					ShardTarget:      node,
					ShardTargetClass: "local",
					Error:            putTraceErrorString(werr),
				})
			} else {
				werr = w.writeRemoteShard(gctx, openShard, shardSize, i, node, plan.Bucket, shardKey)
				observePutStage("ec_write_shard", "remote", shardStageStart)
				ObservePutTraceStage(ctx, PutTraceStageShardWriteRemote, shardStageStart, PutTraceStageFields{
					ShardIndex:       i,
					ShardTarget:      node,
					ShardTargetClass: "remote",
					Error:            putTraceErrorString(werr),
				})
				if w.peerHealth != nil {
					if werr != nil {
						w.peerHealth.MarkUnhealthy(node)
					} else {
						w.peerHealth.MarkHealthy(node)
					}
				}
			}
			if werr != nil {
				return &ecObjectShardWriteError{shardIdx: i, node: node, err: werr}
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

func (w ecObjectWriter) writeSingleLocalReader(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	body io.Reader,
	metricPath string,
	bodyHash hash.Hash,
) (ecObjectWriteResult, error) {
	shardKey := plan.Key + "/" + plan.VersionID
	stageStart := time.Now()

	header := encodeShardHeader(sp.Size)
	if bodyHash != nil {
		body = io.TeeReader(body, bodyHash)
	}
	shardBody := io.MultiReader(bytes.NewReader(header[:]), body)
	if err := w.shards.WriteLocalShardStreamContext(ctx, plan.Bucket, shardKey, 0, shardBody); err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("write single local shard: %w", err)
	}
	observePutStage(metricPath, "write_local_shard", stageStart)

	if bodyHash != nil {
		sp.ETag = hex.EncodeToString(bodyHash.Sum(nil))
	}

	return ecObjectWriteResult{
		Size:        sp.Size,
		ETag:        sp.ETag,
		ModTime:     time.Now().Unix(),
		ShardKey:    shardKey,
		Placement:   cloneStringSlice(plan.Placement),
		RingVersion: plan.RingVersion,
		ECData:      1,
		ECParity:    0,
	}, nil
}

func (w ecObjectWriter) writeRemoteShard(
	ctx context.Context,
	openShard func(int) (io.Reader, error),
	shardSize func(int) (int64, error),
	shardIdx int,
	node, bucket, shardKey string,
) error {
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
		openStart := time.Now()
		body, err := openShard(shardIdx)
		if err != nil {
			ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteOpen, openStart, PutTraceStageFields{
				ShardIndex:       shardIdx,
				ShardTarget:      node,
				ShardTargetClass: "remote",
				Error:            err.Error(),
			})
			return fmt.Errorf("open ec shard %d: %w", shardIdx, err)
		}
		ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteOpen, openStart, PutTraceStageFields{
			ShardIndex:       shardIdx,
			ShardTarget:      node,
			ShardTargetClass: "remote",
		})

		writeCtx, writeCancel := context.WithTimeout(ctx, shardRPCTimeout)
		if shardSize != nil {
			if size, sizeErr := shardSize(shardIdx); sizeErr == nil && size <= ecShardBufferedLimit {
				bufferStart := time.Now()
				var data []byte
				data, err = io.ReadAll(body)
				if err == nil {
					ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteBuffer, bufferStart, PutTraceStageFields{
						Bytes:            int64(len(data)),
						ShardIndex:       shardIdx,
						ShardTarget:      node,
						ShardTargetClass: "remote",
					})
					rpcStart := time.Now()
					err = w.shards.WriteShard(writeCtx, node, bucket, shardKey, shardIdx, data)
					ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteRPC, rpcStart, PutTraceStageFields{
						Bytes:            int64(len(data)),
						ShardIndex:       shardIdx,
						ShardTarget:      node,
						ShardTargetClass: "remote",
						Error:            putTraceErrorString(err),
					})
				} else {
					ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteBuffer, bufferStart, PutTraceStageFields{
						ShardIndex:       shardIdx,
						ShardTarget:      node,
						ShardTargetClass: "remote",
						Error:            err.Error(),
					})
				}
			} else {
				rpcStart := time.Now()
				err = w.shards.WriteShardStream(writeCtx, node, bucket, shardKey, shardIdx, body)
				ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteRPC, rpcStart, PutTraceStageFields{
					ShardIndex:       shardIdx,
					ShardTarget:      node,
					ShardTargetClass: "remote",
					Error:            putTraceErrorString(err),
				})
			}
		} else {
			rpcStart := time.Now()
			err = w.shards.WriteShardStream(writeCtx, node, bucket, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
			ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteRPC, rpcStart, PutTraceStageFields{
				ShardIndex:       shardIdx,
				ShardTarget:      node,
				ShardTargetClass: "remote",
				Error:            putTraceErrorString(err),
			})
		}
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
