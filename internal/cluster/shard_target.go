package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// shardEndpoint abstracts a single placement slot's shard I/O. It is the one
// seam that decides "is this slot served locally or via an RPC to a peer".
// Before this abstraction the writer and reader scattered `node == selfID`
// branches across six dispatch sites; endpointFor now makes that decision once
// and hands back a concrete endpoint whose methods carry the local-vs-remote
// specifics (retry/backoff, peerHealth marking, trace stages).
//
// Defined at the consumer (the EC writer/reader) per the repo convention; only
// the operations the writer and reader actually invoke are exposed — no
// speculative methods.
type shardEndpoint interface {
	// Node is the placement slot's node id.
	Node() string
	// IsLocal reports whether the slot resolves to selfID. Used to choose the
	// local vs remote shard read/write path.
	IsLocal() bool

	// WriteShardReader writes shard shardIdx. openShard yields a fresh reader for
	// the shard payload; shardSize (nil ⇒ unknown) yields its size so the impl can
	// select the sized streaming variant for body-length validation. The local and
	// remote impls emit their own trace stages and (remote) peerHealth marks so the
	// observable call set is identical to the pre-refactor inline dispatch. All
	// paths stream — there is no buffered branch.
	//
	// stagingShardKey (PR1 segment staging): when non-empty the shard BYTES are
	// written to this staging physical path while shardKey stays the FINAL path
	// used as encryption AAD, so a post-promote read of shardKey decrypts
	// correctly. Empty ⇒ direct-to-final write (shardKey is both path and AAD).
	//
	// logicalShardSize is the shard's PRE-compression (logical) size, threaded for
	// the fsync-class decision only (per-segment zstd shrinks the on-disk bytes, so
	// the local writer must classify on logical size to keep a large redundant
	// shard off the commit-tail fsync). -1 ⇒ unknown (use on-disk size). The remote
	// impl ignores it: the logical size is not carried on the shard-write wire, so
	// remote shards keep the on-disk-size classification (cluster follow-up).
	WriteShardReader(ctx context.Context, bucket, shardKey, stagingShardKey string, shardIdx int, logicalShardSize int64, openShard func(int) (io.Reader, error), shardSize func(int) (int64, error)) error
	// DeleteShards removes all shards for shardKey on this slot (write cleanup).
	DeleteShards(ctx context.Context, bucket, shardKey string) error

	// OpenShardStream opens a streaming reader for the shard.
	OpenShardStream(ctx context.Context, bucket, shardKey string, shardIdx int) (io.ReadCloser, error)
	// ReadShardAt reads len(buf) bytes at offset within the shard. offset is the
	// on-disk offset (the caller already adds shardHeaderSize). The remote impl
	// always streams — there is no buffered one-shot branch.
	ReadShardAt(ctx context.Context, bucket, shardKey string, shardIdx int, offset int64, buf []byte) (int, error)
}

// endpointFor is the SOLE place the local-vs-remote decision is made. The EC
// writer and reader call it once per placement slot. Both share the same
// ShardService and (optional) peerHealth they already hold.

// ecShardStore is the unified local+remote ShardService surface the endpoints
// drive. Both the EC writer and reader hold one (always a *ShardService); the
// split write-only / read-only interfaces of the past added no value because the
// concrete value is identical, and the dispatcher needs both halves to express a
// single seam.
type ecShardStore interface {
	localShardStore
	remoteShardStore
}

func (w ecObjectWriter) endpointFor(node string) shardEndpoint {
	if node == w.selfID {
		return localShardEndpoint{node: node, shards: w.shards}
	}
	return remoteShardEndpoint{
		node:          node,
		shards:        w.shards,
		peerHealth:    w.peerHealth,
		writeAttempts: w.writeAttempts,
		writeBackoff:  w.writeBackoff,
	}
}

func (r ecObjectReader) endpointFor(node string) shardEndpoint {
	if node == r.selfID {
		return localShardEndpoint{node: node, shards: r.shards}
	}
	return remoteShardEndpoint{
		node:       node,
		shards:     r.shards,
		peerHealth: r.peerHealth,
	}
}

// localShardEndpoint serves a slot from the local ShardService. It never touches
// peerHealth (a node never marks itself).
type localShardEndpoint struct {
	node   string
	shards localShardStore
}

// localShardStore is the subset of ShardService the local endpoint uses. It
// covers both the writer's local-write and the reader's local-read needs;
// *ShardService satisfies it.
type localShardStore interface {
	WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error
	// WriteLocalShardStreamStagedContext writes a shard to the staging physical
	// path stagingKey while sealing with finalKey as AAD (PR1 segment staging).
	WriteLocalShardStreamStagedContext(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader) error
	DeleteLocalShards(bucket, key string) error
	ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error)
	OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error)
	ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error)
}

func (e localShardEndpoint) Node() string  { return e.node }
func (e localShardEndpoint) IsLocal() bool { return true }

func (e localShardEndpoint) WriteShardReader(ctx context.Context, bucket, shardKey, stagingShardKey string, shardIdx int, logicalShardSize int64, openShard func(int) (io.Reader, error), shardSize func(int) (int64, error)) error {
	shardStageStart := time.Now()

	// An openShard failure returns before the local trace stage is emitted,
	// matching the pre-refactor inline dispatch (which returned early on open
	// error, never reaching observePutStage / ObservePutTraceStage).
	body, err := openShard(shardIdx)
	if err != nil {
		return fmt.Errorf("open ec shard %d: %w", shardIdx, err)
	}

	if stagingShardKey != "" {
		size, knownSize := int64(-1), false
		if shardSize != nil {
			if sz, sizeErr := shardSize(shardIdx); sizeErr == nil {
				size, knownSize = sz, true
			}
		}
		var werr error
		if knownSize {
			if sized, ok := e.shards.(ecObjectStagedSizedShardStore); ok {
				werr = sized.WriteLocalShardStreamStagedSizedContext(ctx, bucket, stagingShardKey, shardKey, shardIdx, body, size, logicalShardSize)
			} else {
				werr = e.shards.WriteLocalShardStreamStagedContext(ctx, bucket, stagingShardKey, shardKey, shardIdx, body)
			}
		} else {
			werr = e.shards.WriteLocalShardStreamStagedContext(ctx, bucket, stagingShardKey, shardKey, shardIdx, body)
		}
		if closer, ok := body.(io.Closer); ok {
			if closeErr := closer.Close(); werr == nil && closeErr != nil {
				werr = fmt.Errorf("close ec shard %d: %w", shardIdx, closeErr)
			}
		}
		observePutStage("ec_write_shard", "local", shardStageStart)
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocal, shardStageStart, PutTraceStageFields{
			ShardIndex:       shardIdx,
			ShardTarget:      e.node,
			ShardTargetClass: "local",
			Error:            putTraceErrorString(werr),
		})
		return werr
	}

	var werr error
	size, knownSize := int64(-1), false
	if shardSize != nil {
		if sz, sizeErr := shardSize(shardIdx); sizeErr == nil {
			size, knownSize = sz, true
		}
	}
	if knownSize {
		if sized, ok := e.shards.(ecObjectSizedShardStore); ok {
			werr = sized.WriteLocalShardStreamSizedContext(ctx, bucket, shardKey, shardIdx, body, size)
		} else {
			werr = e.shards.WriteLocalShardStreamContext(ctx, bucket, shardKey, shardIdx, body)
		}
	} else {
		werr = e.shards.WriteLocalShardStreamContext(ctx, bucket, shardKey, shardIdx, body)
	}
	if closer, ok := body.(io.Closer); ok {
		if closeErr := closer.Close(); werr == nil && closeErr != nil {
			werr = fmt.Errorf("close ec shard %d: %w", shardIdx, closeErr)
		}
	}

	observePutStage("ec_write_shard", "local", shardStageStart)
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocal, shardStageStart, PutTraceStageFields{
		ShardIndex:       shardIdx,
		ShardTarget:      e.node,
		ShardTargetClass: "local",
		Error:            putTraceErrorString(werr),
	})
	return werr
}

func (e localShardEndpoint) DeleteShards(_ context.Context, bucket, shardKey string) error {
	return e.shards.DeleteLocalShards(bucket, shardKey)
}

func (e localShardEndpoint) OpenShardStream(_ context.Context, bucket, shardKey string, shardIdx int) (io.ReadCloser, error) {
	return e.shards.OpenLocalShard(bucket, shardKey, shardIdx)
}

func (e localShardEndpoint) ReadShardAt(_ context.Context, bucket, shardKey string, shardIdx int, offset int64, buf []byte) (int, error) {
	return e.shards.ReadLocalShardAt(bucket, shardKey, shardIdx, offset, buf)
}

// remoteShardEndpoint serves a slot over the ShardService RPC surface and wraps
// every result in peerHealth marking (success → MarkHealthy, failure →
// MarkUnhealthy) exactly as the inline dispatch did.
type remoteShardEndpoint struct {
	node          string
	shards        remoteShardStore
	peerHealth    ecObjectPeerHealth // nil = disabled
	writeAttempts int
	writeBackoff  time.Duration
}

// remoteShardStore is the subset of ShardService the remote endpoint uses.
type remoteShardStore interface {
	WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error
	WriteShardStream(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader) error
	// WriteShardStreamStaged streams a shard to peer, written to the staging
	// physical path stagingKey but sealed with finalKey as AAD (PR1 segment
	// staging). The wire carries finalKey in the request Key (so AAD stays final)
	// and stagingKey as a separate redirect field.
	WriteShardStreamStaged(ctx context.Context, peer, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader) error
	DeleteShards(ctx context.Context, peer, bucket, key string) error
	ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error)
	ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error)
	ReadShardRangeStream(ctx context.Context, peer, bucket, key string, shardIdx int, offset, length int64) (io.ReadCloser, error)
}

func (e remoteShardEndpoint) Node() string  { return e.node }
func (e remoteShardEndpoint) IsLocal() bool { return false }

func (e remoteShardEndpoint) markHealth(ok bool) {
	if e.peerHealth == nil {
		return
	}
	if ok {
		e.peerHealth.MarkHealthy(e.node)
	} else {
		e.peerHealth.MarkUnhealthy(e.node)
	}
}

func (e remoteShardEndpoint) WriteShardReader(ctx context.Context, bucket, shardKey, stagingShardKey string, shardIdx int, _ int64, openShard func(int) (io.Reader, error), shardSize func(int) (int64, error)) error {
	// logicalShardSize is intentionally ignored: the shard-write wire carries no
	// logical size, so the remote receiver classifies the fsync on the on-disk
	// (compressed) size. Threading it over the wire is a cluster follow-up.
	shardStageStart := time.Now()
	werr := e.writeRemoteShard(ctx, openShard, shardSize, shardIdx, bucket, shardKey, stagingShardKey)
	observePutStage("ec_write_shard", "remote", shardStageStart)
	ObservePutTraceStage(ctx, PutTraceStageShardWriteRemote, shardStageStart, PutTraceStageFields{
		ShardIndex:       shardIdx,
		ShardTarget:      e.node,
		ShardTargetClass: "remote",
		Error:            putTraceErrorString(werr),
	})
	e.markHealth(werr == nil)
	return werr
}

func (e remoteShardEndpoint) writeRemoteShard(
	ctx context.Context,
	openShard func(int) (io.Reader, error),
	shardSize func(int) (int64, error),
	shardIdx int,
	bucket, shardKey, stagingShardKey string,
) error {
	attempts := e.writeAttempts
	if attempts <= 0 {
		attempts = ecShardWriteAttempts
	}
	backoff := e.writeBackoff
	if backoff <= 0 {
		backoff = ecShardWriteBackoff
	}
	node := e.node

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
		if stagingShardKey != "" {
			size, knownSize := int64(-1), false
			if shardSize != nil {
				if sz, sizeErr := shardSize(shardIdx); sizeErr == nil {
					size, knownSize = sz, true
				}
			}
			rpcStart := time.Now()
			if knownSize {
				if sized, ok := e.shards.(ecObjectRemoteStagedSizedShardStore); ok {
					err = sized.WriteShardStreamStagedSized(writeCtx, node, bucket, stagingShardKey, shardKey, shardIdx, readerWithoutWriterTo{Reader: body}, size)
				} else {
					err = e.shards.WriteShardStreamStaged(writeCtx, node, bucket, stagingShardKey, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
				}
			} else {
				err = e.shards.WriteShardStreamStaged(writeCtx, node, bucket, stagingShardKey, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
			}
			ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteRPC, rpcStart, PutTraceStageFields{
				ShardIndex:       shardIdx,
				ShardTarget:      node,
				ShardTargetClass: "remote",
				Error:            putTraceErrorString(err),
			})
		} else {
			size, knownSize := int64(-1), false
			if shardSize != nil {
				if sz, sizeErr := shardSize(shardIdx); sizeErr == nil {
					size, knownSize = sz, true
				}
			}
			rpcStart := time.Now()
			if knownSize {
				if sized, ok := e.shards.(ecObjectRemoteSizedShardStore); ok {
					err = sized.WriteShardStreamSized(writeCtx, node, bucket, shardKey, shardIdx, readerWithoutWriterTo{Reader: body}, size)
				} else {
					err = e.shards.WriteShardStream(writeCtx, node, bucket, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
				}
			} else {
				err = e.shards.WriteShardStream(writeCtx, node, bucket, shardKey, shardIdx, readerWithoutWriterTo{Reader: body})
			}
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

func (e remoteShardEndpoint) DeleteShards(ctx context.Context, bucket, shardKey string) error {
	return e.shards.DeleteShards(ctx, e.node, bucket, shardKey)
}

// OpenShardStream marks peerHealth at the RPC boundary (success → healthy,
// failure → unhealthy). The returned reader additionally flips the peer
// unhealthy on the first mid-body read failure — a peer that serves 200 then
// truncates/resets must not keep being selected. Normal EOF and caller-driven
// cancellation (context.Canceled / DeadlineExceeded — shardRPCTimeout bounds
// the whole body read, so large legitimate reads can hit it; a stalled peer
// surfaces as the transport idle-read i/o timeout instead, which IS marked)
// are not peer faults.
func (e remoteShardEndpoint) OpenShardStream(ctx context.Context, bucket, shardKey string, shardIdx int) (io.ReadCloser, error) {
	rc, err := e.shards.ReadShardStream(ctx, e.node, bucket, shardKey, shardIdx)
	e.markHealth(err == nil)
	if err != nil {
		return nil, err
	}
	return &healthTrackingReadCloser{rc: rc, markUnhealthy: func() { e.markHealth(false) }}, nil
}

// ReadShardAt marks peerHealth at the RPC boundary, and additionally flips the
// peer unhealthy on a mid-body transport failure from the post-RPC read. The
// short-read case keeps the existing semantic: io.ReadFull's ErrUnexpectedEOF
// does NOT flip the peer — a short range read is ambiguous at this layer.
// All reads use the streaming RPC — there is no buffered one-shot branch.
func (e remoteShardEndpoint) ReadShardAt(ctx context.Context, bucket, shardKey string, shardIdx int, offset int64, buf []byte) (int, error) {
	rc, err := e.shards.ReadShardRangeStream(ctx, e.node, bucket, shardKey, shardIdx, offset, int64(len(buf)))
	e.markHealth(err == nil)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && isPeerFaultReadErr(err) {
		e.markHealth(false)
	}
	return n, err
}

// isPeerFaultReadErr reports whether a mid-body read error is evidence of a
// misbehaving peer (as opposed to normal EOF or caller-driven cancellation).
func isPeerFaultReadErr(err error) bool {
	return !errors.Is(err, io.EOF) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// healthTrackingReadCloser wraps a remote shard body and reports the first
// mid-body read failure to peerHealth. marked is atomic so a future concurrent
// consumer of this general endpoint method cannot race the flip; current EC
// read paths are single-consumer.
type healthTrackingReadCloser struct {
	rc            io.ReadCloser
	markUnhealthy func()
	marked        atomic.Bool
}

func (h *healthTrackingReadCloser) Read(p []byte) (int, error) {
	n, err := h.rc.Read(p)
	if err != nil && isPeerFaultReadErr(err) && h.marked.CompareAndSwap(false, true) {
		h.markUnhealthy()
	}
	return n, err
}

func (h *healthTrackingReadCloser) Close() error { return h.rc.Close() }
