package lifecycle

import (
	"context"
	"strings"
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
type ObjectDeleter interface {
	DeleteObject(ctx context.Context, bucket, key string) error
	DeleteObjectVersion(bucket, key, versionID string) error
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error)
}

// Scrubbable is the subset of scrubber.Scrubbable used by the lifecycle worker.
type Scrubbable interface {
	ListBuckets(ctx context.Context) ([]string, error)
	ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error)
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

		objs, err := w.backend.ScanObjects(bucket)
		if err != nil {
			log.Error().Str("bucket", bucket).Err(err).Msg("lifecycle: scan objects")
			continue
		}

		for obj := range objs {
			if ctx.Err() != nil {
				go func() {
					for range objs {
					}
				}() // drain producer to prevent goroutine leak
				return
			}
			w.objectsChecked.Add(1)
			w.applyRules(ctx, obj, cfg.Rules, now)
		}
	}

	w.lastRunNano.Store(now.UnixNano())
}

func (w *Worker) currentTime() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

func (w *Worker) applyRules(ctx context.Context, obj scrubber.ObjectRecord, rules []Rule, now time.Time) {
	for _, rule := range rules {
		if rule.Status != StatusEnabled {
			continue
		}
		if rule.Filter != nil && rule.Filter.Prefix != "" {
			if !strings.HasPrefix(obj.Key, rule.Filter.Prefix) {
				continue
			}
		}

		if exp := rule.Expiration; exp != nil && exp.Days > 0 && !obj.IsDeleteMarker {
			objTime := time.Unix(obj.LastModified, 0)
			if now.Sub(objTime) >= time.Duration(exp.Days)*24*time.Hour {
				if err := w.limiter.Wait(ctx); err != nil {
					return // ctx cancelled
				}
				if err := w.deleter.DeleteObject(ctx, obj.Bucket, obj.Key); err != nil {
					log.Error().Str("bucket", obj.Bucket).Str("key", obj.Key).Err(err).Msg("lifecycle: delete object")
				} else {
					w.expired.Add(1)
				}
				continue // skip version expiration for deleted current object
			}
		}

		if nce := rule.NoncurrentVersionExpiration; nce != nil {
			all, err := w.deleter.ListObjectVersions(obj.Bucket, obj.Key, 0)
			if err != nil {
				log.Error().Str("bucket", obj.Bucket).Str("key", obj.Key).Err(err).Msg("lifecycle: list versions")
				continue
			}
			// Narrow to exact-key matches — ListObjectVersions does prefix match.
			versions := all[:0]
			for _, v := range all {
				if v.Key == obj.Key {
					versions = append(versions, v)
				}
			}
			// versions expected newest-first (as returned by ListObjectVersions)
			noncurrentIdx := 0
			for _, v := range versions {
				if v.IsLatest {
					continue
				}
				// S3 spec: when both fields are set, both conditions must be met (AND).
				// An unset field (0) is treated as "no constraint" (always satisfied).
				beyondCount := nce.NewerNoncurrentVersions <= 0 || noncurrentIdx >= nce.NewerNoncurrentVersions
				beyondAge := nce.NoncurrentDays <= 0 || now.Sub(time.Unix(v.LastModified, 0)) >= time.Duration(nce.NoncurrentDays)*24*time.Hour
				if (nce.NewerNoncurrentVersions > 0 || nce.NoncurrentDays > 0) && beyondCount && beyondAge {
					if err := w.limiter.Wait(ctx); err != nil {
						return // ctx cancelled
					}
					if err := w.deleter.DeleteObjectVersion(obj.Bucket, obj.Key, v.VersionID); err != nil {
						log.Error().Str("key", obj.Key).Str("versionID", v.VersionID).Err(err).Msg("lifecycle: delete version")
					} else {
						w.versionsPruned.Add(1)
					}
				}
				noncurrentIdx++
			}
		}
	}
}
