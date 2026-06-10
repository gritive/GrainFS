package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// quorumMetaShadowTimeout bounds the synchronous shadow so a hung placement
// node cannot stall the real PUT it rides on. A timed-out shadow records its
// stage with a non-empty Error and is excluded from the p99 ratio analysis.
const quorumMetaShadowTimeout = 30 * time.Second //nolint:unused

// Phase 0 — quorum-meta shadow write (kill-only perf spike).
//
// A SHADOW per-node quorum metadata write run alongside the existing data_raft
// propose, gated OFF by default. It measures whether a leaderless quorum meta
// write tail is *egregiously* slower than the raft-commit tail at conc32 — the
// only Phase 0 question. It is NOT load-bearing: GET/LIST/DELETE never read it,
// failures never fail the PUT, and the data is discarded. See
// docs/superpowers/specs/phase0-quorum-meta-shadow-spec.md.
//
// Fidelity (honest): this writes a separate file + one fsync per placement node,
// while the real Phase 3 design co-locates meta with the shard (rides the shard
// WAL flush ~free). The measured tail is therefore a CONSERVATIVE UPPER BOUND.

// quorumMetaShadowEnabled is read once at process start. Default OFF →
// byte-identical inert (the shadow code is never entered). Tests set it directly.
var quorumMetaShadowEnabled = os.Getenv("GRAINFS_QUORUM_META_SHADOW") != "" //nolint:unused

// quorumMetaShadow performs the shadow quorum meta write for one object PUT,
// SYNCHRONOUSLY (the PUT waits for the quorum). Synchronous — not a detached
// goroutine — so conc32 load is preserved symmetrically on both the shadow and
// the raft stages and no unbounded shadow pile-up self-injects a contention
// axis. Errors are swallowed: this is measurement only.
func (b *DistributedBackend) quorumMetaShadow(ctx context.Context, cmd PutObjectMetaCmd) { //nolint:unused
	if !quorumMetaShadowEnabled {
		return
	}
	if b.shardSvc == nil || len(cmd.NodeIDs) == 0 {
		return
	}
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	if err != nil {
		return
	}
	self := b.currentSelfAddr()
	k := int(cmd.ECData)

	sctx, cancel := context.WithTimeout(ctx, quorumMetaShadowTimeout)
	defer cancel()
	done := StartPutTraceStage(ctx, PutTraceStageQuorumMetaWrite)
	ferr := fanOutQuorumMetaShadow(sctx, cmd.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			// self = local write (no loopback) — the real Phase 3 design also
			// writes self locally, so loopback would inject a false-STOP axis.
			return b.shardSvc.writeShadowMetaLocal(cmd.Bucket, cmd.Key, blob)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteShadowMeta(fctx, addr, cmd.Bucket, cmd.Key, blob)
	})
	done(PutTraceStageFields{
		Bytes:            int64(len(blob)),
		MetaProposeCount: k,
		Error:            putTraceErrorString(ferr),
	})
}

// fanOutQuorumMetaShadow dispatches to every placement node concurrently and
// returns as soon as K acks arrive (quorum = K data shards, NOT all-N — a
// straggler must not dominate the measured tail). It returns an error only when
// the quorum becomes unreachable (more than N-K failures).
func fanOutQuorumMetaShadow(ctx context.Context, nodes []string, k int, dispatch func(context.Context, string) error) error { //nolint:unused
	if k <= 0 {
		k = 1
	}
	n := len(nodes)
	if n < k {
		return fmt.Errorf("quorum meta shadow: placement %d < quorum %d", n, k)
	}
	results := make(chan error, n)
	for _, node := range nodes {
		node := node
		go func() { results <- dispatch(ctx, node) }()
	}
	var ok, failed int
	for i := 0; i < n; i++ {
		var err error
		select {
		case err = <-results:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err == nil {
			ok++
			if ok >= k {
				return nil
			}
			continue
		}
		failed++
		if failed > n-k {
			return fmt.Errorf("quorum meta shadow: %d/%d nodes failed, quorum %d unreachable", failed, n, k)
		}
	}
	return nil
}

// WriteShadowMeta sends the shadow object-meta blob to a remote placement node.
// Thin mirror of WriteShard: build the envelope, CallFlatBuffer, check for a
// remote Error reply.
func (s *ShardService) WriteShadowMeta(ctx context.Context, addr, bucket, key string, data []byte) error {
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("WriteShadowMeta", bucket, key, 0, data)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, addr, fw)
	if err != nil {
		return fmt.Errorf("write shadow meta to %s: %w", addr, err)
	}
	rpcType, _, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal shadow meta response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote shadow meta error from %s", addr)
	}
	return nil
}

// writeShadowMetaLocal durably writes the shadow meta blob under
// {dataDirs[0]}/.shadow_meta/{bucket}/{key} with a single fsync — the dominant,
// honest durability cost being measured. Same device as the shard files.
func (s *ShardService) writeShadowMetaLocal(bucket, key string, data []byte) error {
	if len(s.dataDirs) == 0 {
		return fmt.Errorf("shadow meta: no data dir")
	}
	root := filepath.Join(s.dataDirs[0], ".shadow_meta")
	target := filepath.Join(root, bucket, key)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("shadow meta: key %q escapes root", key)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("shadow meta mkdir: %w", err)
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("shadow meta create: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("shadow meta write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("shadow meta fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("shadow meta close: %w", err)
	}
	return nil
}
