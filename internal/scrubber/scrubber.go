package scrubber

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/metrics"
)

const otelTracerName = "grainfs/scrubber"

// Scrubbable is the interface ECBackend must implement for scrubbing.
// Defined here (not in erasure) to invert the dependency.
type Scrubbable interface {
	ListBuckets(ctx context.Context) ([]string, error)
	// ScanObjects streams ObjectRecords for EC objects in bucket (DataShards > 0 only).
	ScanObjects(bucket string) (<-chan ObjectRecord, error)
	// ObjectExists checks whether the object's metadata still exists.
	// Returns false if the object was deleted between scan and verify (Eng Review #9).
	ObjectExists(bucket, key string) (bool, error)
	// ShardPaths returns all expected shard file paths for an object.
	ShardPaths(bucket, key, versionID string, totalShards int) []string
	// ReadShard reads and decrypts a shard, verifying its CRC32 footer.
	// bucket+key are used for locking (RLock).
	ReadShard(bucket, key, path string) ([]byte, error)
	// WriteShard encrypts and atomically writes a shard with a CRC32 footer.
	// bucket+key are used for locking (Lock).
	WriteShard(bucket, key, path string, data []byte) error
}

// ObjectRecord carries metadata needed for scrubbing.
type ObjectRecord struct {
	Bucket         string
	Key            string
	DataShards     int
	ParityShards   int
	ETag           string
	VersionID      string
	IsDeleteMarker bool
	LastModified   int64 // Unix seconds; used by lifecycle worker for expiration checks
}

// PlainRecord carries metadata for a plain object that can be migrated to EC.
type PlainRecord struct {
	Bucket      string
	Key         string
	VersionID   string
	Size        int64
	ETag        string
	ContentType string
}

// Migrator is an optional interface ECBackend can implement to enable plain→EC migration.
// If the backend implements this, the scrubber will re-encode plain objects each cycle.
type Migrator interface {
	ScanPlainObjects(bucket string) (<-chan PlainRecord, error)
	MigratePlainToEC(rec PlainRecord) error
}

// ShardOwner is an optional interface backends can implement to scope
// verification to the shards this node is responsible for. Cluster mode
// fans shards out across peers; each node's scrubber should only verify
// and repair the shards it holds locally — peer-owned shards are the peer's
// responsibility. When the backend does not implement this interface, the
// scrubber verifies every shard (the legacy single-node behaviour).
//
// Slice 3 of refactor/unify-storage-paths.
type ShardOwner interface {
	// NodeID identifies this node inside the placement vector.
	NodeID() string
	// OwnedShards returns the shard indices this node owns for the given
	// object. Returns nil when the object has no placement (non-EC) or this
	// node is not in the placement vector.
	OwnedShards(bucket, key, versionID, nodeID string) []int
}

// ShardRepairer is an optional interface backends can implement to repair
// a single owned shard by pulling surviving shards from peers and writing
// the reconstructed bytes back to local disk. When a backend implements
// both ShardOwner and ShardRepairer, the scrubber delegates repair of
// missing/corrupt owned shards to RepairShardLocal instead of running the
// in-process Reed-Solomon reconstruct path (which would fail in cluster
// mode since this node only holds its own shards, not peers').
//
// Slice 3 of refactor/unify-storage-paths.
type ShardRepairer interface {
	RepairShardLocal(bucket, key, versionID string, shardIdx int) error
}

type CorruptShardQuarantiner interface {
	QuarantineCorruptShardLocal(bucket, key, versionID string, shardIdx int, reason string) error
}

// maxRepairsPerCycle limits repairs per scrub cycle to avoid I/O storms.
const maxRepairsPerCycle = 100

// maxMigrationsPerCycle limits plain→EC migrations per cycle to avoid I/O storms.
const maxMigrationsPerCycle = 50

// BackgroundScrubber periodically verifies and repairs EC shard integrity.
type BackgroundScrubber struct {
	backend  Scrubbable
	verifier *ShardVerifier
	repairer *RepairEngine
	emitter  Emitter
	interval time.Duration
	resetCh  chan time.Duration // hot-reload interval signal
	limiter  *rate.Limiter      // 100 objects/sec scan throttle (Eng Review #8)

	// stats and orphanTombstone are owned by the background goroutine (single writer).
	// No synchronisation needed for writes; external reads use statsSnap.
	stats           ScrubStats
	statsSnap       atomic.Value // published at end of each cycle; stores ScrubStats
	orphanTombstone map[string]struct{}

	// mu guards lastStatuses for concurrent reads. The background goroutine is the
	// sole writer; RWMutex lets multiple concurrent callers of LastStatus() proceed
	// without blocking each other.
	mu           sync.RWMutex
	lastStatuses map[string]ShardStatus // "bucket/key" → last observed status
}

// ScrubStats is a snapshot of scrubbing statistics.
type ScrubStats struct {
	LastRun        time.Time
	ObjectsChecked int64
	ShardErrors    int64
	Repaired       int64
	Unrepairable   int64
	PlainMigrated  int64
}

// ScrubberOption configures a BackgroundScrubber.
type ScrubberOption func(*BackgroundScrubber)

// WithNoRetry disables the transient-error retry delay in the verifier (for tests).
func WithNoRetry() ScrubberOption {
	return func(s *BackgroundScrubber) {
		s.verifier = NewShardVerifier(s.backend, WithVerifyRetryDelay(0))
	}
}

// WithEmitter wires the scrubber (and its repair engine) to an Emitter so that
// HealEvents flow to the SSE hub and the eventstore. Defaults to NoopEmitter.
func WithEmitter(e Emitter) ScrubberOption {
	return func(s *BackgroundScrubber) {
		if e == nil {
			return
		}
		s.emitter = e
		s.repairer = NewRepairEngine(s.backend, WithRepairEmitter(e))
	}
}

// New creates a BackgroundScrubber with a rate limit of 100 scans/sec.
func New(backend Scrubbable, interval time.Duration, opts ...ScrubberOption) *BackgroundScrubber {
	s := &BackgroundScrubber{
		backend:         backend,
		verifier:        NewShardVerifier(backend),
		repairer:        NewRepairEngine(backend),
		emitter:         NoopEmitter{},
		interval:        interval,
		resetCh:         make(chan time.Duration, 1),
		limiter:         rate.NewLimiter(100, 10),
		lastStatuses:    make(map[string]ShardStatus),
		orphanTombstone: make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// LastStatus returns the last observed ShardStatus for bucket/key (for tests).
func (s *BackgroundScrubber) LastStatus(bucket, key string) ShardStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastStatuses[bucket+"/"+key]
}

// SetEmitter swaps the HealEvent emitter at runtime. Used by serve.go to wire
// the server-owned heal emitter into a scrubber that was constructed before
// the server existed. Must be called before Start; concurrent emit during a
// swap is unsafe.
func (s *BackgroundScrubber) SetEmitter(e Emitter) {
	if e == nil {
		return
	}
	s.emitter = e
	s.repairer = NewRepairEngine(s.backend, WithRepairEmitter(e))
}

// SetInterval changes the scrub interval at runtime without restarting.
func (s *BackgroundScrubber) SetInterval(d time.Duration) {
	select {
	case s.resetCh <- d:
	default:
		// Drain stale pending reset and replace it.
		select {
		case <-s.resetCh:
		default:
		}
		s.resetCh <- d
	}
}

// Start launches the background scrub loop; returns immediately.
func (s *BackgroundScrubber) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-s.resetCh:
				ticker.Reset(d)
			case <-ticker.C:
				s.runOnce(ctx)
			}
		}
	}()
}

// RunOnce is exported for testing.
func (s *BackgroundScrubber) RunOnce(ctx context.Context) {
	s.runOnce(ctx)
}

func (s *BackgroundScrubber) runOnce(ctx context.Context) {
	tr := otel.GetTracerProvider().Tracer(otelTracerName)
	ctx, cycleSpan := tr.Start(ctx, "scrub.cycle")
	defer cycleSpan.End()

	buckets, err := s.backend.ListBuckets(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("scrub: list buckets failed")
		cycleSpan.RecordError(err)
		cycleSpan.SetStatus(codes.Error, "list buckets failed")
		return
	}

	// If signing is unavailable, skip repairs this cycle to avoid producing
	// unaudited sessions with no receipt. Verification (detect phase) still
	// runs so the dashboard shows degraded state while signing is down.
	signingOK := true
	if checker, ok := s.emitter.(SigningHealthChecker); ok {
		if !checker.SigningHealthy() {
			signingOK = false
			log.Warn().Msg("scrub: signing unavailable, skipping repairs this cycle")
		}
	}
	// sessionFinalizer is non-nil only when signing is available.
	var sessionFinalizer SessionFinalizer
	if signingOK {
		sessionFinalizer, _ = s.emitter.(SessionFinalizer)
	}

	// Cluster-mode opt-in: when the backend implements ShardOwner, we only
	// verify the shards this node actually holds; peer-owned shards are each
	// peer's responsibility. When ShardRepairer is also implemented, missing
	// shards are repaired by pulling survivors from peers rather than by the
	// in-process Reed-Solomon reconstruct path (which has no peer shards to
	// feed it in cluster mode).
	owner, _ := s.backend.(ShardOwner)
	repairer, _ := s.backend.(ShardRepairer)

	knownDirs := make(map[string]bool)
	repairCount := 0
	for _, bucket := range buckets {
		objCh, err := s.backend.ScanObjects(bucket)
		if err != nil {
			log.Warn().Str("bucket", bucket).Err(err).Msg("scrub: scan objects failed")
			continue
		}
		for rec := range objCh {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Throttle scan to protect foreground I/O (Eng Review #8)
			if err := s.limiter.Wait(ctx); err != nil {
				return
			}

			// Race check: object may have been deleted since scan (Eng Review #9)
			exists, err := s.backend.ObjectExists(rec.Bucket, rec.Key)
			if err != nil || !exists {
				continue
			}

			// Resolve the indices to verify. In ShardOwner mode an empty
			// result means "nothing on this node" — skip the object wholesale
			// so the orphan sweep doesn't mistake an unverified object for
			// a healthy one.
			var indices []int
			if owner != nil {
				indices = owner.OwnedShards(rec.Bucket, rec.Key, rec.VersionID, owner.NodeID())
				if len(indices) == 0 {
					continue
				}
			}

			// Track shard dir as known (for orphan sweep).
			total := rec.DataShards + rec.ParityShards
			if total > 0 {
				paths := s.backend.ShardPaths(rec.Bucket, rec.Key, rec.VersionID, total)
				if len(paths) > 0 {
					knownDirs[shardDirFromPath(paths[0])] = true
				}
			}

			metrics.ScrubObjectsCheckedTotal.Inc()
			s.stats.ObjectsChecked++

			status := s.verifier.VerifyIndices(rec, indices)

			s.mu.Lock()
			s.lastStatuses[rec.Bucket+"/"+rec.Key] = status
			s.mu.Unlock()

			if status.IsHealthy() {
				continue
			}

			errCount := int64(len(status.Missing) + len(status.Corrupt))
			metrics.ScrubShardErrorsTotal.Add(float64(errCount))
			s.stats.ShardErrors += errCount

			// Group every event for this object's repair under one correlation ID.
			correlationID := newCorrelationID()
			s.emitDetect(rec, status, correlationID)

			// Per-cycle repair cap (Eng Review #5)
			if repairCount >= maxRepairsPerCycle {
				metrics.ScrubSkippedOverCapTotal.Inc()
				ev := newRepairEvent(PhaseReconstruct, OutcomeSkipped, rec, correlationID)
				ev.ErrCode = "cycle_cap"
				s.emitter.Emit(ev)
				continue
			}

			// Signing unavailable this cycle: skip repair to preserve the
			// "no unsigned receipts" audit invariant.
			if !signingOK {
				continue
			}

			repaired, rerr := s.repairOne(ctx, rec, status, correlationID, repairer)
			if rerr != nil {
				metrics.ECDegradedTotal.Inc()
				s.stats.Unrepairable++
				log.Error().Str("bucket", rec.Bucket).Str("key", rec.Key).Err(rerr).Msg("scrub: unrepairable")
				continue
			}
			metrics.ScrubRepairedTotal.Inc()
			metrics.HealShardsRepairedTotal.Add(float64(repaired))
			s.stats.Repaired++
			repairCount++

			// Notify the emitter that this repair session is complete so it
			// can aggregate the session's HealEvents into a signed HealReceipt.
			// Re-check signing health: the key store may have rotated out after
			// the cycle-start check; calling FinalizeSession when signing is
			// gone would silently drop the receipt.
			if sessionFinalizer != nil {
				if checker, ok := s.emitter.(SigningHealthChecker); ok && !checker.SigningHealthy() {
					log.Warn().Str("correlation_id", correlationID).Msg("scrub: signing unavailable mid-cycle, receipt dropped")
				} else {
					sessionFinalizer.FinalizeSession(correlationID)
				}
			}
		}
	}

	// Optional: migrate plain objects to EC if backend supports it.
	if migrator, ok := s.backend.(Migrator); ok {
		s.runMigration(ctx, migrator, buckets)
	}

	// Optional: sweep orphan shard dirs left by migration crashes.
	if walker, ok := s.backend.(OrphanWalkable); ok {
		s.orphanSweep(walker, knownDirs)
	}

	s.stats.LastRun = time.Now()
	s.statsSnap.Store(s.stats)
}

// repairOne drives repair for a single object. When the backend implements
// ShardRepairer, each missing/corrupt shard is repaired by RepairShardLocal
// (peer-sourced reconstruct). Otherwise the legacy in-process engine runs.
// Returns the count of shards repaired.
func (s *BackgroundScrubber) repairOne(ctx context.Context, rec ObjectRecord, status ShardStatus, correlationID string, repairer ShardRepairer) (int64, error) {
	tr := otel.GetTracerProvider().Tracer(otelTracerName)
	_, span := tr.Start(ctx, "scrub.repair")
	defer span.End()
	span.SetAttributes(
		attribute.String("grainfs.bucket", rec.Bucket),
		attribute.String("grainfs.key", rec.Key),
		attribute.String("grainfs.correlation_id", correlationID),
		attribute.Int("grainfs.shards_missing", len(status.Missing)),
		attribute.Int("grainfs.shards_corrupt", len(status.Corrupt)),
	)

	if repairer == nil {
		if err := s.repairer.RepairWithCorrelation(rec, status, correlationID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "repair failed")
			return 0, err
		}
		repaired := int64(len(status.Missing) + len(status.Corrupt))
		span.SetAttributes(attribute.Int64("grainfs.shards_repaired", repaired))
		return repaired, nil
	}

	var repaired int64
	var firstErr error
	if quarantiner, ok := repairer.(CorruptShardQuarantiner); ok && len(status.Corrupt) > 0 {
		for _, idx := range status.Corrupt {
			if err := quarantiner.QuarantineCorruptShardLocal(rec.Bucket, rec.Key, rec.VersionID, idx, "CRC mismatch during scrub verification"); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			repaired++
		}
		status.Corrupt = nil
	}

	damaged := append(append([]int{}, status.Missing...), status.Corrupt...)
	for _, idx := range damaged {
		reconstructStart := time.Now()
		if err := repairer.RepairShardLocal(rec.Bucket, rec.Key, rec.VersionID, idx); err != nil {
			ev := newRepairEvent(PhaseReconstruct, OutcomeFailed, rec, correlationID)
			ev.ShardID = int32(idx)
			ev.DurationMs = uint32(time.Since(reconstructStart).Milliseconds())
			ev.ErrCode = "reconstruct_failed"
			s.emitter.Emit(ev)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reconstructEv := newRepairEvent(PhaseReconstruct, OutcomeSuccess, rec, correlationID)
		reconstructEv.ShardID = int32(idx)
		reconstructEv.DurationMs = uint32(time.Since(reconstructStart).Milliseconds())
		s.emitter.Emit(reconstructEv)
		writeEv := newRepairEvent(PhaseWrite, OutcomeSuccess, rec, correlationID)
		writeEv.ShardID = int32(idx)
		s.emitter.Emit(writeEv)
		repaired++
	}
	if firstErr != nil && repaired == 0 {
		return 0, firstErr
	}
	s.emitter.Emit(newRepairEvent(PhaseVerify, OutcomeSuccess, rec, correlationID))
	return repaired, nil
}

func (s *BackgroundScrubber) runMigration(ctx context.Context, migrator Migrator, buckets []string) {
	migrateCount := 0
	for _, bucket := range buckets {
		plainCh, err := migrator.ScanPlainObjects(bucket)
		if err != nil {
			log.Warn().Str("bucket", bucket).Err(err).Msg("scrub: scan plain objects failed")
			continue
		}
		for rec := range plainCh {
			select {
			case <-ctx.Done():
				for range plainCh { //nolint:revive // drain to unblock producer
				}
				return
			default:
			}
			if migrateCount >= maxMigrationsPerCycle {
				metrics.ScrubMigrationSkippedOverCapTotal.Inc()
				for range plainCh { //nolint:revive // drain to unblock producer
				}
				return
			}
			if err := s.limiter.Wait(ctx); err != nil {
				for range plainCh { //nolint:revive // drain to unblock producer
				}
				return
			}
			if err := migrator.MigratePlainToEC(rec); err != nil {
				metrics.ScrubPlainMigrateErrorTotal.Inc()
				log.Error().Str("bucket", rec.Bucket).Str("key", rec.Key).Err(err).Msg("scrub: plain→EC migration failed")
				continue
			}
			metrics.ScrubPlainMigratedTotal.Inc()
			s.stats.PlainMigrated++
			migrateCount++
			log.Info().Str("bucket", rec.Bucket).Str("key", rec.Key).Msg("scrub: plain→EC migrated")
		}
	}
}

// Stats returns a snapshot of the current scrub statistics.
func (s *BackgroundScrubber) Stats() ScrubStats {
	if snap, ok := s.statsSnap.Load().(ScrubStats); ok {
		return snap
	}
	return ScrubStats{}
}

// emitDetect publishes one detect HealEvent per missing/corrupt shard so the
// dashboard can show what triggered a repair before reconstruct/write events
// arrive.
func (s *BackgroundScrubber) emitDetect(rec ObjectRecord, status ShardStatus, correlationID string) {
	for _, idx := range status.Missing {
		ev := newRepairEvent(PhaseDetect, OutcomeFailed, rec, correlationID)
		ev.ShardID = int32(idx)
		ev.ErrCode = "missing"
		s.emitter.Emit(ev)
	}
	for _, idx := range status.Corrupt {
		ev := newRepairEvent(PhaseDetect, OutcomeFailed, rec, correlationID)
		ev.ShardID = int32(idx)
		ev.ErrCode = "corrupt"
		s.emitter.Emit(ev)
	}
}
