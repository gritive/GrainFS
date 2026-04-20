package scrubber

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/metrics"
)

// Scrubbable is the interface ECBackend must implement for scrubbing.
// Defined here (not in erasure) to invert the dependency.
type Scrubbable interface {
	ListBuckets() ([]string, error)
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

// maxRepairsPerCycle limits repairs per scrub cycle to avoid I/O storms.
const maxRepairsPerCycle = 100

// maxMigrationsPerCycle limits plain→EC migrations per cycle to avoid I/O storms.
const maxMigrationsPerCycle = 50

// BackgroundScrubber periodically verifies and repairs EC shard integrity.
type BackgroundScrubber struct {
	backend         Scrubbable
	verifier        *ShardVerifier
	repairer        *RepairEngine
	emitter         Emitter
	interval        time.Duration
	resetCh         chan time.Duration // hot-reload interval signal
	limiter         *rate.Limiter      // 100 objects/sec scan throttle (Eng Review #8)
	mu              sync.Mutex
	stats           ScrubStats
	lastStatuses    map[string]ShardStatus // "bucket/key" → last observed status
	orphanTombstone map[string]struct{}    // dirs seen as orphan last cycle
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	buckets, err := s.backend.ListBuckets()
	if err != nil {
		slog.Warn("scrub: list buckets failed", "err", err)
		return
	}

	knownDirs := make(map[string]bool)
	repairCount := 0
	for _, bucket := range buckets {
		objCh, err := s.backend.ScanObjects(bucket)
		if err != nil {
			slog.Warn("scrub: scan objects failed", "bucket", bucket, "err", err)
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

			// Track shard dir as known (for orphan sweep).
			total := rec.DataShards + rec.ParityShards
			if total > 0 {
				paths := s.backend.ShardPaths(rec.Bucket, rec.Key, rec.VersionID, total)
				if len(paths) > 0 {
					knownDirs[shardDirFromPath(paths[0])] = true
				}
			}

			metrics.ScrubObjectsCheckedTotal.Inc()
			atomic.AddInt64(&s.stats.ObjectsChecked, 1)

			status := s.verifier.Verify(rec)

			s.mu.Lock()
			s.lastStatuses[rec.Bucket+"/"+rec.Key] = status
			s.mu.Unlock()

			if status.IsHealthy() {
				continue
			}

			errCount := int64(len(status.Missing) + len(status.Corrupt))
			metrics.ScrubShardErrorsTotal.Add(float64(errCount))
			atomic.AddInt64(&s.stats.ShardErrors, errCount)

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

			if err := s.repairer.RepairWithCorrelation(rec, status, correlationID); err != nil {
				metrics.ECDegradedTotal.Inc()
				atomic.AddInt64(&s.stats.Unrepairable, 1)
				slog.Error("scrub: unrepairable", "bucket", rec.Bucket, "key", rec.Key, "err", err)
				continue
			}
			metrics.ScrubRepairedTotal.Inc()
			metrics.HealShardsRepairedTotal.Add(float64(errCount))
			atomic.AddInt64(&s.stats.Repaired, 1)
			repairCount++
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

	s.mu.Lock()
	s.stats.LastRun = time.Now()
	s.mu.Unlock()
}

func (s *BackgroundScrubber) runMigration(ctx context.Context, migrator Migrator, buckets []string) {
	migrateCount := 0
	for _, bucket := range buckets {
		plainCh, err := migrator.ScanPlainObjects(bucket)
		if err != nil {
			slog.Warn("scrub: scan plain objects failed", "bucket", bucket, "err", err)
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
				slog.Error("scrub: plain→EC migration failed", "bucket", rec.Bucket, "key", rec.Key, "err", err)
				continue
			}
			metrics.ScrubPlainMigratedTotal.Inc()
			atomic.AddInt64(&s.stats.PlainMigrated, 1)
			migrateCount++
			slog.Info("scrub: plain→EC migrated", "bucket", rec.Bucket, "key", rec.Key)
		}
	}
}

// Stats returns a snapshot of the current scrub statistics.
func (s *BackgroundScrubber) Stats() ScrubStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
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
