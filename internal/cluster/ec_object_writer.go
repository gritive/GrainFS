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

	"github.com/gritive/GrainFS/internal/storage"
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
	// localDataDirs returns the shard service's local drive roots so the C
	// optimisation in writeSpooledShards can pick a per-drive spool path.
	// Returns an empty slice for shard services that don't expose drives.
	localDataDirs() []string
	// importLocalShardFromPath atomically promotes a fully-written shard
	// payload (final on-disk format) at srcPath into the canonical shard
	// slot for (bucket, key, shardIdx). Used by the C rename-instead-of-copy
	// path in writeShardReadersWithSize. requireFsync controls per-shard
	// durability: pass true for shards whose loss the cluster cannot tolerate
	// (data shards in EC-aware mode), false when an earlier WAL fsync already
	// covers the shard (the C-path WAL metadata batch).
	importLocalShardFromPath(ctx context.Context, bucket, key string, shardIdx int, srcPath string, requireFsync bool) error
	// appendShardMetadataWALBatch records one OpShardPut metadata-only entry
	// per shard and fsyncs the WAL exactly once. Used immediately before the
	// C-path renames so a single WAL fsync covers durability for the entire
	// object — recovery verifies each shard's presence and defers
	// missing-shard reconstruction to read time / scrubber.
	appendShardMetadataWALBatch(ctx context.Context, bucket, key string, shards []shardMetaWALEntry) error
	// hasDataWAL reports whether a data WAL is wired. When false the C-path
	// must still fsync each data shard for crash safety (legacy fallback).
	hasDataWAL() bool
}

type ecObjectSizedShardStore interface {
	WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error
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
	ModTime          int64
	PreserveModTime  bool
	ExpectedETag     string
	Config           ECConfig
	Placement        []string
	ContentType      string
	UserMetadata     map[string]string
	SSEAlgorithm     string
	// SegmentBlobID is empty for legacy single-blob writes; when non-empty
	// the plan refers to a chunked-PUT segment and shardKey is scoped by
	// SegmentBlobID via ecObjectSegmentShardKey.
	SegmentBlobID string
	// SegmentIdx is the 0-based chunk index. Meaningful only when
	// SegmentBlobID != "".
	SegmentIdx int
}

type ecObjectWriteResult struct {
	Size      int64
	ETag      string
	ModTime   int64
	ShardKey  string
	Placement []string
	ECData    uint8
	ECParity  uint8
	Parts     []storage.MultipartPartEntry // populated by CompleteMultipartUpload
	Tags      []storage.Tag                // populated by CompleteMultipartUpload (carried from multipartMeta.Tags)
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

func ecObjectShardKey(key, versionID string) string {
	if versionID == "" {
		return key
	}
	return key + "/" + versionID
}

// ecObjectSegmentShardKey is the post-Phase-2 unified shardKey helper. It
// preserves legacy behaviour when plan.SegmentBlobID == "" (returns
// ecObjectShardKey(Key, VersionID) verbatim) and switches to a
// segment-scoped key when SegmentBlobID != "". Because the existing
// ShardService inline AAD is bucket+"/"+shardKey+"/"+shardIdx, swapping the
// shardKey automatically scopes AAD to the segment.
func ecObjectSegmentShardKey(plan ecObjectWritePlan) string {
	if plan.SegmentBlobID == "" {
		return ecObjectShardKey(plan.Key, plan.VersionID)
	}
	return plan.Key + "/segments/" + plan.SegmentBlobID
}

// writeSegmentInput bundles the per-segment write parameters consumed by
// writeOneSegment. Caller (clusterSegmentBackend.WriteSegment) buffers the
// SegmentWriter chunk into Data; segments are ≤ DefaultChunkSize.
//
//nolint:unused // wired up by clusterSegmentBackend.WriteSegment in Task 2.3.
type writeSegmentInput struct {
	Bucket, Key, VersionID string
	SegmentBlobID          string
	SegmentIdx             int
	Group                  ShardGroupEntry
	Cfg                    ECConfig
	Data                   []byte
}

// writeOneSegment is a thin wrapper around writeDataShards that fills in the
// segment-scoped fields on ecObjectWritePlan. Returns the PlacementRecord
// (synthesized from the chosen K+M peers), the data-shard etag, and the same
// blobID echoed back for caller convenience.
//
//nolint:unused // wired up by clusterSegmentBackend.WriteSegment in Task 2.3.
func (w ecObjectWriter) writeOneSegment(ctx context.Context, in writeSegmentInput) (PlacementRecord, string, string, error) {
	nShards := in.Cfg.DataShards + in.Cfg.ParityShards
	if len(in.Group.PeerIDs) < nShards {
		return PlacementRecord{}, "", "", fmt.Errorf("group %s has %d peers, need %d", in.Group.ID, len(in.Group.PeerIDs), nShards)
	}
	placementKey := ecObjectSegmentShardKey(ecObjectWritePlan{
		Key:           in.Key,
		VersionID:     in.VersionID,
		SegmentBlobID: in.SegmentBlobID,
	})
	placement := PlacementForNodes(in.Cfg, in.Group.PeerIDs, placementKey)
	plan := ecObjectWritePlan{
		Bucket:           in.Bucket,
		Key:              in.Key,
		VersionID:        in.VersionID,
		PlacementGroupID: in.Group.ID,
		Config:           in.Cfg,
		Placement:        placement,
		SegmentBlobID:    in.SegmentBlobID,
		SegmentIdx:       in.SegmentIdx,
	}
	res, err := w.writeDataShards(ctx, plan, in.Data)
	if err != nil {
		return PlacementRecord{}, "", "", fmt.Errorf("write segment %d (blob %s): %w", in.SegmentIdx, in.SegmentBlobID, err)
	}
	rec := PlacementRecord{Nodes: placement, K: in.Cfg.DataShards, M: in.Cfg.ParityShards}
	return rec, res.ETag, in.SegmentBlobID, nil
}

//nolint:unused // referenced by ec_object_writer_test.go.
func (w ecObjectWriter) writeShardReaders(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	metricPath string,
) (ecObjectWriteResult, error) {
	return w.writeShardReadersWithSize(ctx, plan, sp, openShard, nil, metricPath, nil)
}

func (w ecObjectWriter) writeMemoryShards(ctx context.Context, plan ecObjectWritePlan, sp *spooledObject) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	src, err := sp.Open()
	if err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("open spooled object: %w", err)
	}
	data := make([]byte, sp.Size)
	_, readErr := io.ReadFull(src, data)
	closeErr := src.Close()
	if readErr != nil {
		return ecObjectWriteResult{}, fmt.Errorf("read spooled object: %w", readErr)
	}
	if closeErr != nil {
		return ecObjectWriteResult{}, fmt.Errorf("close spooled object: %w", closeErr)
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
	}, "ec_memory", nil)
}

func (w ecObjectWriter) writeDataShards(ctx context.Context, plan ecObjectWritePlan, data []byte) (ecObjectWriteResult, error) {
	shards, err := ecSplitBodies(plan.Config, data)
	if err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("ec split: %w", err)
	}
	header := encodeShardHeader(int64(len(data)))
	h := md5.Sum(data)
	sp := &spooledObject{
		Size: int64(len(data)),
		ETag: hex.EncodeToString(h[:]),
	}

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		if idx < 0 || idx >= len(shards) {
			return nil, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader(shards[idx])), nil
	}, func(idx int) (int64, error) {
		if idx < 0 || idx >= len(shards) {
			return 0, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return int64(shardHeaderSize + len(shards[idx])), nil
	}, "ec", nil)
}

func (w ecObjectWriter) writeSpooledShards(ctx context.Context, plan ecObjectWritePlan, spoolDir string, sp *spooledObject) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	// Prefer the C-optimized per-drive spool that bakes in the final shard
	// AAD so writeShardReadersWithSize can rename-instead-of-copy on local
	// shards. Requires an encryptor (the at-rest-encryption default) and a
	// local ShardService that exposes its data drives. Cluster fan-out and
	// plaintext deployments still take the legacy single-dir spool path.
	var (
		shards *spooledECShards
		err    error
	)
	if sp != nil && sp.encrypted && sp.encryptor != nil && w.shards != nil {
		if drives := w.shards.localDataDirs(); len(drives) > 0 {
			shardKey := ecObjectSegmentShardKey(plan)
			shards, err = spoolECShardsToFinal(ctx, plan.Config, drives, plan.Bucket, shardKey, sp)
		}
	}
	if shards == nil {
		shards, err = spoolECShards(ctx, plan.Config, spoolDir, sp)
	}
	if err != nil {
		return ecObjectWriteResult{}, err
	}
	observePutStage("ec", "spool_shards", stageStart)
	defer shards.Cleanup()

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		return shards.OpenShard(idx)
	}, shards.ShardSize, "ec", shards)
}

func (w ecObjectWriter) writeShardReadersWithSize(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	shardSize func(idx int) (int64, error),
	metricPath string,
	spooled *spooledECShards,
) (ecObjectWriteResult, error) {
	shardKey := ecObjectSegmentShardKey(plan)
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

	// C-path WAL batch: when the spool prepared every local shard in final
	// format AND a data WAL is wired, record all shards' metadata in ONE WAL
	// flush. After this single fsync, the per-shard rename loop can skip
	// fsync entirely — recovery uses the WAL records to verify the on-disk
	// state and defers missing-shard reconstruction to read time. Matches
	// the original "fsync only in data WAL" design intent that
	// size-aware WAL bypass had unintentionally regressed.
	walCoversCPath := false
	if spooled != nil && spooled.finalFormat && w.shards.hasDataWAL() {
		var meta []shardMetaWALEntry
		for i, node := range plan.Placement {
			if node != w.selfID || i >= len(spooled.paths) || spooled.paths[i] == "" {
				continue
			}
			if shardSize == nil {
				continue
			}
			size, err := shardSize(i)
			if err != nil {
				continue
			}
			meta = append(meta, shardMetaWALEntry{shardIdx: i, size: size})
		}
		if len(meta) > 0 {
			if err := w.shards.appendShardMetadataWALBatch(ctx, plan.Bucket, shardKey, meta); err != nil {
				return ecObjectWriteResult{}, fmt.Errorf("data wal metadata batch: %w", err)
			}
			walCoversCPath = true
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
				// C optimisation: when the spool wrote shard i to its target
				// drive in the final shard format under the final AAD, the
				// shard service can just rename the spool file into the
				// canonical slot. Skips an entire decrypt + re-encrypt + 2nd
				// disk write per shard.
				if spooled != nil && spooled.finalFormat && i < len(spooled.paths) && spooled.paths[i] != "" {
					srcPath := spooled.paths[i]
					// Durability layering:
					//   - walCoversCPath=true: the batched WAL flush above
					//     already made the metadata durable. We can skip the
					//     per-shard fsync entirely; on crash, the WAL replay
					//     points at missing shards and EC reconstruction
					//     rebuilds them from surviving peers at read time.
					//   - walCoversCPath=false (no WAL wired): no fsync
					//     coverage exists, so each data shard must fsync
					//     itself. Parity still skips fsync — EC reconstruct
					//     from data shards still works.
					shardRequireFsync := false
					if !walCoversCPath {
						shardRequireFsync = i < plan.Config.DataShards
					}
					werr = w.shards.importLocalShardFromPath(gctx, plan.Bucket, shardKey, i, srcPath, shardRequireFsync)
					if werr == nil {
						// Tell the spooled set we transferred ownership of
						// this path so Cleanup() doesn't try to delete the
						// (now-renamed-away) file.
						spooled.paths[i] = ""
					}
					observePutStage("ec_write_shard", "local_rename", shardStageStart)
					ObservePutTraceStage(ctx, PutTraceStageShardWriteLocal, shardStageStart, PutTraceStageFields{
						ShardIndex:       i,
						ShardTarget:      node,
						ShardTargetClass: "local",
						Error:            putTraceErrorString(werr),
					})
					if werr != nil {
						return &ecObjectShardWriteError{shardIdx: i, node: node, err: werr}
					}
					written <- node
					return nil
				}
				body, err := openShard(i)
				if err != nil {
					return fmt.Errorf("open ec shard %d: %w", i, err)
				}
				if shardSize != nil {
					if size, sizeErr := shardSize(i); sizeErr == nil && size <= ecShardBufferedLimit {
						data := make([]byte, size)
						_, werr = io.ReadFull(body, data)
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
		Size:      sp.Size,
		ETag:      sp.ETag,
		ModTime:   time.Now().Unix(),
		ShardKey:  shardKey,
		Placement: cloneStringSlice(plan.Placement),
		ECData:    uint8(plan.Config.DataShards),
		ECParity:  uint8(plan.Config.ParityShards),
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
	shardKey := ecObjectSegmentShardKey(plan)
	stageStart := time.Now()

	header := encodeShardHeader(sp.Size)
	if bodyHash != nil {
		body = io.TeeReader(body, bodyHash)
	}
	shardBody := io.MultiReader(bytes.NewReader(header[:]), body)
	if sized, ok := w.shards.(ecObjectSizedShardStore); ok {
		if err := sized.WriteLocalShardStreamSizedContext(ctx, plan.Bucket, shardKey, 0, shardBody, int64(shardHeaderSize)+sp.Size); err != nil {
			return ecObjectWriteResult{}, fmt.Errorf("write single local shard: %w", err)
		}
	} else if err := w.shards.WriteLocalShardStreamContext(ctx, plan.Bucket, shardKey, 0, shardBody); err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("write single local shard: %w", err)
	}
	observePutStage(metricPath, "write_local_shard", stageStart)

	if bodyHash != nil {
		sp.ETag = hex.EncodeToString(bodyHash.Sum(nil))
	}

	return ecObjectWriteResult{
		Size:      sp.Size,
		ETag:      sp.ETag,
		ModTime:   time.Now().Unix(),
		ShardKey:  shardKey,
		Placement: cloneStringSlice(plan.Placement),
		ECData:    1,
		ECParity:  0,
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
				data := make([]byte, size)
				_, err = io.ReadFull(body, data)
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
