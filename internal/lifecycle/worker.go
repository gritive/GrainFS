package lifecycle

import (
	"context"
	"strings"
	"sync"
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
	DeleteObject(bucket, key string) error
	DeleteObjectVersion(bucket, key, versionID string) error
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error)
}

// Scrubbable is the subset of scrubber.Scrubbable used by the lifecycle worker.
type Scrubbable interface {
	ListBuckets() ([]string, error)
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
type Worker struct {
	store    *Store
	backend  Scrubbable
	deleter  ObjectDeleter
	interval time.Duration
	limiter  *rate.Limiter

	mu     sync.Mutex
	stats  Stats
	cancel context.CancelFunc
}

// NewWorker creates a lifecycle Worker. interval controls how often rules are applied.
func NewWorker(store *Store, backend Scrubbable, deleter ObjectDeleter, interval time.Duration) *Worker {
	return &Worker{
		store:    store,
		backend:  backend,
		deleter:  deleter,
		interval: interval,
		limiter:  rate.NewLimiter(100, 10), // 100 deletes/sec, burst 10
	}
}

// Run starts the lifecycle worker loop. It blocks until ctx is cancelled or Stop is called.
func (w *Worker) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.cancel = cancel
	w.mu.Unlock()

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

// Stop signals the worker to finish the current cycle and exit.
func (w *Worker) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Stats returns a copy of the current worker statistics.
func (w *Worker) Stats() Stats {
	w.mu.Lock()
	lastRun := w.stats.LastRun
	w.mu.Unlock()
	return Stats{
		LastRun:        lastRun,
		ObjectsChecked: atomic.LoadInt64(&w.stats.ObjectsChecked),
		Expired:        atomic.LoadInt64(&w.stats.Expired),
		VersionsPruned: atomic.LoadInt64(&w.stats.VersionsPruned),
	}
}

func (w *Worker) runCycle(ctx context.Context) {
	now := time.Now()
	buckets, err := w.backend.ListBuckets()
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
			atomic.AddInt64(&w.stats.ObjectsChecked, 1)
			w.applyRules(ctx, obj, cfg.Rules, now)
		}
	}

	w.mu.Lock()
	w.stats.LastRun = now
	w.mu.Unlock()
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
				if err := w.deleter.DeleteObject(obj.Bucket, obj.Key); err != nil {
					log.Error().Str("bucket", obj.Bucket).Str("key", obj.Key).Err(err).Msg("lifecycle: delete object")
				} else {
					atomic.AddInt64(&w.stats.Expired, 1)
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
						atomic.AddInt64(&w.stats.VersionsPruned, 1)
					}
				}
				noncurrentIdx++
			}
		}
	}
}
