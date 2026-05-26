package scrubber

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/metrics"
)

// ECScrubSource adapts the EC scrub Scrubbable interface to the BlockSource
// contract so Director.Trigger can route CLI-initiated scrubs to EC buckets.
// Background EC scrub (BackgroundScrubber.runOnce) is unchanged — the two
// paths coexist (D3=α). keyPrefix is filtered adapter-side because
// Scrubbable.ScanObjects has no prefix parameter today (D5=α defer).
//
// The source caches each yielded ObjectRecord by (bucket, key, versionID) so
// the paired ECScrubVerifier can recover EC metadata (DataShards/ParityShards)
// without re-querying the backend. Cache entries are removed by the verifier
// after Verify (healthy/skipped path) or Repair (non-healthy path).
type ECScrubSource struct {
	resolve GroupResolver
	nodeID  string
	cache   sync.Map // recordKey → ObjectRecord
}

// GroupResolver returns the Scrubbable for a bucket if the bucket lives on a
// locally-owned group, or (nil, false) when it lives on a peer-owned group.
// Multi-raft clusters route per-bucket to the right group's BadgerDB; a peer-
// owned bucket means this node's scrub closes an empty channel (the peer's
// Director.ApplyFromFSM picks up the same SessionID and walks its own DB).
type GroupResolver func(bucket string) (Scrubbable, bool)

// SingleBackendResolver returns a GroupResolver that always yields backend.
// Used by tests and any single-backend deployment that has no per-bucket
// routing.
func SingleBackendResolver(backend Scrubbable) GroupResolver {
	return func(string) (Scrubbable, bool) { return backend, true }
}

// NewECScrubSource constructs an EC adapter. Director routes to it via
// routeSource("<bucket>") → "ec" (anything not internal-replicated). nodeID
// enables ShardOwner filtering when the resolved backend implements ShardOwner.
func NewECScrubSource(resolve GroupResolver, nodeID string) *ECScrubSource {
	return &ECScrubSource{resolve: resolve, nodeID: nodeID}
}

// Name returns the registry name "ec" — matches Director.routeSource.
func (s *ECScrubSource) Name() string { return "ec" }

func recordKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func (s *ECScrubSource) loadRecord(bucket, key, versionID string) (ObjectRecord, bool) {
	v, ok := s.cache.Load(recordKey(bucket, key, versionID))
	if !ok {
		return ObjectRecord{}, false
	}
	return v.(ObjectRecord), true
}

func (s *ECScrubSource) deleteRecord(bucket, key, versionID string) {
	s.cache.Delete(recordKey(bucket, key, versionID))
}

// PrimeForTest installs an ObjectRecord in the source's cache without going
// through Iter. Test helper only.
func (s *ECScrubSource) PrimeForTest(bucket, key, versionID string, rec ObjectRecord) {
	s.cache.Store(recordKey(bucket, key, versionID), rec)
}

// Iter walks bucket via Scrubbable.ScanObjects and yields one Block per EC
// ObjectRecord — the `lat:` index. keyPrefix narrows by adapter-side string
// prefix. ObjectExists race check + ShardOwner filter mirror runOnce policy
// (D4=A).
func (s *ECScrubSource) Iter(ctx context.Context, bucket, keyPrefix string) (<-chan Block, error) {
	out := make(chan Block, 64)
	if bucket == "" {
		close(out)
		return out, nil
	}
	backend, owned := s.resolve(bucket)
	if !owned || backend == nil {
		close(out)
		return out, nil
	}
	objCh, err := backend.ScanObjects(bucket)
	if err != nil {
		close(out)
		return out, err
	}
	go func() {
		defer close(out)
		for rec := range objCh {
			if keyPrefix != "" && !strings.HasPrefix(rec.Key, keyPrefix) {
				continue
			}
			if exists, eerr := backend.ObjectExists(rec.Bucket, rec.Key); eerr != nil || !exists {
				continue
			}
			if owner, ok := backend.(ShardOwner); ok {
				idx := owner.OwnedShards(rec.Bucket, rec.Key, rec.VersionID, s.nodeID)
				if len(idx) == 0 {
					continue
				}
			}
			s.cache.Store(recordKey(rec.Bucket, rec.Key, rec.VersionID), rec)
			blk := Block{
				Bucket:       rec.Bucket,
				Key:          rec.Key,
				VersionID:    rec.VersionID,
				ExpectedETag: rec.ETag,
			}
			select {
			case <-ctx.Done():
				return
			case out <- blk:
			}
		}
	}()
	return out, nil
}

// ECScrubVerifier wraps ShardVerifier + ShardRepairer as a BlockVerifier for
// the Director path. Verify checks shard integrity, emits PhaseDetect events,
// and uses BlockStatus.Skipped when signing is unavailable to preserve the
// "no unsigned receipts" invariant. Repair runs the per-shard reconstruct
// pipeline and emits Reconstruct/Write/Verify events + FinalizeSession.
type ECScrubVerifier struct {
	backend  Scrubbable
	verifier *ShardVerifier
	limiter  *rate.Limiter
	emitter  Emitter
	nodeID   string
	src      *ECScrubSource

	correlationByBlock sync.Map // recordKey → correlationID (Verify→Repair link)
}

// NewECScrubVerifier wires the verifier with shared primitives from
// BackgroundScrubber (Verifier/Limiter/Emitter getters) and the paired source
// (for ObjectRecord cache access).
func NewECScrubVerifier(backend Scrubbable, ver *ShardVerifier, lim *rate.Limiter, em Emitter, nodeID string, src *ECScrubSource) *ECScrubVerifier {
	return &ECScrubVerifier{
		backend:  backend,
		verifier: ver,
		limiter:  lim,
		emitter:  em,
		nodeID:   nodeID,
		src:      src,
	}
}

func (v *ECScrubVerifier) backendFor(bucket string) Scrubbable {
	if v.src != nil && v.src.resolve != nil {
		if backend, ok := v.src.resolve(bucket); ok && backend != nil {
			return backend
		}
	}
	return v.backend
}

func (v *ECScrubVerifier) shardVerifierFor(backend Scrubbable) *ShardVerifier {
	if backend == v.backend {
		return v.verifier
	}
	return NewShardVerifier(backend, WithVerifyRetryDelay(v.verifier.retryDelay))
}

func (v *ECScrubVerifier) Verify(ctx context.Context, blk Block) (BlockStatus, error) {
	if err := v.limiter.Wait(ctx); err != nil {
		return BlockStatus{}, err
	}
	if checker, ok := v.emitter.(SigningHealthChecker); ok && !checker.SigningHealthy() {
		v.src.deleteRecord(blk.Bucket, blk.Key, blk.VersionID)
		return BlockStatus{Skipped: true, Detail: "signing unavailable"}, nil
	}
	rec, ok := v.src.loadRecord(blk.Bucket, blk.Key, blk.VersionID)
	if !ok {
		return BlockStatus{Skipped: true, Detail: "ObjectRecord cache miss"}, nil
	}
	backend := v.backendFor(blk.Bucket)
	verifier := v.shardVerifierFor(backend)
	var indices []int
	if owner, ok := backend.(ShardOwner); ok {
		indices = owner.OwnedShards(rec.Bucket, rec.Key, rec.VersionID, v.nodeID)
	}
	status := verifier.VerifyIndices(rec, indices)
	if status.IsHealthy() {
		v.src.deleteRecord(blk.Bucket, blk.Key, blk.VersionID)
		return BlockStatus{Healthy: true}, nil
	}
	correlationID := newCorrelationID()
	v.correlationByBlock.Store(recordKey(blk.Bucket, blk.Key, blk.VersionID), correlationID)
	emitDetectEvents(v.emitter, rec, status, correlationID)
	if len(status.Unverified) > 0 {
		metrics.ECScrubUnverifiedShardsTotal.WithLabelValues("legacy_no_crc").Add(float64(len(status.Unverified)))
		v.src.deleteRecord(blk.Bucket, blk.Key, blk.VersionID)
		return BlockStatus{
			Missing: len(status.Missing) > 0,
			Corrupt: len(status.Corrupt) > 0,
			Skipped: true,
			Detail:  legacyNoCRCRepairSkippedDetail(status),
		}, nil
	}
	out := BlockStatus{Detail: fmt.Sprintf("missing=%d corrupt=%d", len(status.Missing), len(status.Corrupt))}
	if len(status.Missing) > 0 {
		out.Missing = true
	}
	if len(status.Corrupt) > 0 {
		out.Corrupt = true
	}
	return out, nil
}

func (v *ECScrubVerifier) Repair(ctx context.Context, blk Block) error {
	defer v.src.deleteRecord(blk.Bucket, blk.Key, blk.VersionID)
	rec, ok := v.src.loadRecord(blk.Bucket, blk.Key, blk.VersionID)
	if !ok {
		return fmt.Errorf("ECScrubVerifier.Repair: cache miss for %s/%s/%s", blk.Bucket, blk.Key, blk.VersionID)
	}
	corrIDVal, _ := v.correlationByBlock.LoadAndDelete(recordKey(blk.Bucket, blk.Key, blk.VersionID))
	correlationID, _ := corrIDVal.(string)
	if correlationID == "" {
		correlationID = newCorrelationID()
	}
	var indices []int
	backend := v.backendFor(blk.Bucket)
	verifier := v.shardVerifierFor(backend)
	if owner, ok := backend.(ShardOwner); ok {
		indices = owner.OwnedShards(rec.Bucket, rec.Key, rec.VersionID, v.nodeID)
	}
	status := verifier.VerifyIndices(rec, indices)
	if status.IsHealthy() {
		return nil
	}
	repairer, _ := backend.(ShardRepairer)
	if repairer == nil {
		eng := NewRepairEngine(backend, WithRepairEmitter(v.emitter))
		return eng.RepairWithCorrelation(rec, status, correlationID)
	}
	if quarantiner, ok := repairer.(CorruptShardQuarantiner); ok && len(status.Corrupt) > 0 {
		for _, idx := range status.Corrupt {
			_ = quarantiner.QuarantineCorruptShardLocal(rec.Bucket, rec.Key, rec.VersionID, idx, "CRC mismatch (CLI scrub)")
		}
	}
	damaged := append(append([]int{}, status.Missing...), status.Corrupt...)
	var firstErr error
	for _, idx := range damaged {
		start := time.Now()
		if err := repairer.RepairShardLocal(rec.Bucket, rec.Key, rec.VersionID, idx); err != nil {
			ev := newRepairEvent(PhaseReconstruct, OutcomeFailed, rec, correlationID)
			ev.ShardID = int32(idx)
			ev.DurationMs = uint32(time.Since(start).Milliseconds())
			ev.ErrCode = "reconstruct_failed"
			v.emitter.Emit(ev)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		recEv := newRepairEvent(PhaseReconstruct, OutcomeSuccess, rec, correlationID)
		recEv.ShardID = int32(idx)
		recEv.DurationMs = uint32(time.Since(start).Milliseconds())
		v.emitter.Emit(recEv)
		wEv := newRepairEvent(PhaseWrite, OutcomeSuccess, rec, correlationID)
		wEv.ShardID = int32(idx)
		v.emitter.Emit(wEv)
	}
	v.emitter.Emit(newRepairEvent(PhaseVerify, OutcomeSuccess, rec, correlationID))
	if finalizer, ok := v.emitter.(SessionFinalizer); ok {
		finalizer.FinalizeSession(correlationID)
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}
