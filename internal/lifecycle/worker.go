package lifecycle

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// ObjectDeleter abstracts object deletion for testability.
// ListObjectVersions signature matches server.ObjectVersionLister so a single
// backend type can satisfy both — the worker passes key as prefix with
// maxKeys=0 (unlimited) and filters to exact-key matches itself.
//
// AbortMultipartUpload + MultipartUploadPartCount are used by MPUWorker for
// AbortIncompleteMultipartUpload evaluation. MultipartUploadPartCount returns
// 0 (no error) when the backend doesn't expose a counter — the MPUWorker
// treats 0 as "weight unknown" and uses 1.
type ObjectDeleter interface {
	DeleteObject(ctx context.Context, bucket, key string) error
	DeleteObjectVersion(bucket, key, versionID string) error
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
	MultipartUploadPartCount(bucket, key, uploadID string) (int, error)
}

// Scrubbable is the subset of scrubber.Scrubbable used by the lifecycle worker.
//
// ScanObjects stays on the interface for backward compatibility with the
// scrubber package; the lifecycle worker itself no longer uses it as of
// Task 9 (it now drives ScanObjectsGrouped instead).
type Scrubbable interface {
	ListBuckets(ctx context.Context) ([]string, error)
	ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error)
	ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error)
	ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error)
}

// Stats tracks lifecycle worker activity.
type Stats struct {
	LastRun        time.Time
	ObjectsChecked int64
	Expired        int64
	VersionsPruned int64
}

// Worker periodically scans buckets and applies lifecycle rules.
//
// See docs/adr/0013-lifecycle-service-lock-free-publication.md for the
// lock-free stats publication shape.
type Worker struct {
	store    *Store
	backend  Scrubbable
	deleter  ObjectDeleter
	interval time.Duration
	limiter  *rate.Limiter
	now      func() time.Time

	// lastRunNano stores time.UnixNano of the most recently completed cycle;
	// 0 means "never run". Stats() translates 0 to a zero-value time.Time{}
	// so admin callers that rely on IsZero see the same behaviour.
	lastRunNano    atomic.Int64
	objectsChecked atomic.Int64
	expired        atomic.Int64
	versionsPruned atomic.Int64
}

// NewWorker creates a lifecycle Worker. interval controls how often rules are applied.
func NewWorker(store *Store, backend Scrubbable, deleter ObjectDeleter, interval time.Duration) *Worker {
	return &Worker{
		store:    store,
		backend:  backend,
		deleter:  deleter,
		interval: interval,
		limiter:  rate.NewLimiter(100, 10), // 100 deletes/sec, burst 10
		now:      time.Now,
	}
}

// Run starts the lifecycle worker loop. It blocks until ctx is cancelled.
// Cancellation is driven by Service.stop cancelling the workerCtx.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.runCycle(ctx)
	for {
		select {
		case <-ticker.C:
			w.runCycle(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// RunCycleForTest is a thin public wrapper around runCycle so package-external
// tests (and the in-package regression test asserting no N×ListVersions calls)
// can drive a single cycle deterministically.
func (w *Worker) RunCycleForTest(ctx context.Context) { w.runCycle(ctx) }

// Stats returns a copy of the current worker statistics.
func (w *Worker) Stats() Stats {
	var lastRun time.Time
	if n := w.lastRunNano.Load(); n != 0 {
		lastRun = time.Unix(0, n)
	}
	return Stats{
		LastRun:        lastRun,
		ObjectsChecked: w.objectsChecked.Load(),
		Expired:        w.expired.Load(),
		VersionsPruned: w.versionsPruned.Load(),
	}
}

func (w *Worker) runCycle(ctx context.Context) {
	now := w.currentTime()
	buckets, err := w.backend.ListBuckets(ctx)
	if err != nil {
		log.Error().Err(err).Msg("lifecycle: list buckets")
		return
	}

	for _, bucket := range buckets {
		if ctx.Err() != nil {
			return
		}
		cfg, err := w.store.Get(bucket)
		if err != nil {
			log.Error().Str("bucket", bucket).Err(err).Msg("lifecycle: get config")
			continue
		}
		if cfg == nil {
			continue
		}

		groups, err := w.backend.ScanObjectsGrouped(bucket)
		if err != nil {
			log.Error().Str("bucket", bucket).Err(err).Msg("lifecycle: scan objects grouped")
			continue
		}

		for g := range groups {
			if ctx.Err() != nil {
				go drainGroups(groups) // drain producer to prevent goroutine leak
				return
			}
			w.objectsChecked.Add(int64(len(g.Versions)))
			w.applyRulesToGroup(ctx, g, cfg.Rules, now)
		}
	}

	w.lastRunNano.Store(now.UnixNano())
}

func drainGroups(ch <-chan storage.ObjectKeyGroup) {
	for range ch {
	}
}

func (w *Worker) currentTime() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// applyRulesToGroup folds Filter + Expiration + NoncurrentVersionExpiration
// evaluation over a single ObjectKeyGroup. Per S3 spec the Filter scopes the
// per-current-version expiration path; NoncurrentVersionExpiration still
// applies to noncurrent versions regardless of the filter match on the
// current version (this is a behavior change from the pre-Task-9 worker,
// which gated both paths on the filter).
func (w *Worker) applyRulesToGroup(ctx context.Context, g storage.ObjectKeyGroup, rules []Rule, now time.Time) {
	if len(g.Versions) == 0 {
		return
	}
	var current *storage.ObjectVersionRecord
	if g.Versions[0].IsLatest {
		current = &g.Versions[0]
	}
	for _, r := range rules {
		if r.Status != StatusEnabled {
			continue
		}
		if current != nil && r.Expiration != nil && MatchFilter(current, g.Key, r.Filter) {
			w.applyExpiration(ctx, g, current, r.Expiration, now)
		}
		if r.NoncurrentVersionExpiration != nil {
			w.applyNoncurrent(ctx, g, r.NoncurrentVersionExpiration, now)
		}
	}
}

func (w *Worker) applyExpiration(ctx context.Context, g storage.ObjectKeyGroup, current *storage.ObjectVersionRecord, exp *Expiration, now time.Time) {
	if current.IsDeleteMarker {
		if exp.ExpiredObjectDeleteMarker != nil && *exp.ExpiredObjectDeleteMarker && len(g.Versions) == 1 {
			w.deleteVersion(ctx, g.Bucket, g.Key, current.VersionID)
		}
		return
	}
	var trigger time.Time
	switch {
	case exp.Date != nil:
		trigger = ExpirationTriggerDate(*exp.Date)
	case exp.Days > 0:
		trigger = ExpirationTriggerDays(current.LastModified, exp.Days)
	default:
		return
	}
	if !now.Before(trigger) {
		w.deleteObject(ctx, g.Bucket, g.Key)
	}
}

func (w *Worker) applyNoncurrent(ctx context.Context, g storage.ObjectKeyGroup, nce *NoncurrentVersionExpiration, now time.Time) {
	if nce.NewerNoncurrentVersions <= 0 && nce.NoncurrentDays <= 0 {
		return
	}
	noncurrentIdx := 0
	for i := range g.Versions {
		v := &g.Versions[i]
		if v.IsLatest {
			continue
		}
		// S3 spec: when both fields are set, both conditions must be met (AND).
		// An unset field (<=0) is treated as "no constraint" (always satisfied).
		beyondCount := nce.NewerNoncurrentVersions <= 0 || noncurrentIdx >= nce.NewerNoncurrentVersions
		beyondAge := nce.NoncurrentDays <= 0 ||
			now.Sub(time.Unix(v.LastModified, 0)) >= time.Duration(nce.NoncurrentDays)*24*time.Hour
		if beyondCount && beyondAge {
			w.deleteVersion(ctx, g.Bucket, g.Key, v.VersionID)
		}
		noncurrentIdx++
	}
}

func (w *Worker) deleteObject(ctx context.Context, bucket, key string) {
	if err := w.limiter.Wait(ctx); err != nil {
		return // ctx cancelled
	}
	if err := w.deleter.DeleteObject(ctx, bucket, key); err != nil {
		log.Error().Str("bucket", bucket).Str("key", key).Err(err).Msg("lifecycle: delete object")
		return
	}
	w.expired.Add(1)
}

func (w *Worker) deleteVersion(ctx context.Context, bucket, key, versionID string) {
	if err := w.limiter.Wait(ctx); err != nil {
		return // ctx cancelled
	}
	if err := w.deleter.DeleteObjectVersion(bucket, key, versionID); err != nil {
		log.Error().Str("bucket", bucket).Str("key", key).Str("versionID", versionID).Err(err).Msg("lifecycle: delete version")
		return
	}
	w.versionsPruned.Add(1)
}
