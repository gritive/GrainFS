package lifecycle

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/storage"
)

// MPUWorker runs on every node, scanning the node-local multipart upload
// index and aborting incomplete uploads matching any rule's
// AbortIncompleteMultipartUpload clause. See spec § "Cluster / Raft —
// split execution model".
type MPUWorker struct {
	store    *Store
	backend  Scrubbable
	deleter  ObjectDeleter
	interval time.Duration
	limiter  *rate.Limiter
	logger   zerolog.Logger
	now      func() time.Time

	aborted atomic.Int64
}

// MPUWorkerOption configures an MPUWorker at construction.
type MPUWorkerOption func(*MPUWorker)

// WithMPUNowForTest overrides the worker's time source. Production code uses
// time.Now. Use only in tests.
func WithMPUNowForTest(now func() time.Time) MPUWorkerOption {
	return func(w *MPUWorker) { w.now = now }
}

// WithMPULogger overrides the default zerolog instance.
func WithMPULogger(l zerolog.Logger) MPUWorkerOption {
	return func(w *MPUWorker) { w.logger = l }
}

// NewMPUWorker takes a shared limiter (owned by Service per spec §
// "Memory and Throughput Bounds"). Both workers passing through the same
// limiter instance enforces "100 deletes/sec per node" regardless of
// which clause drove the delete.
func NewMPUWorker(store *Store, backend Scrubbable, deleter ObjectDeleter, interval time.Duration, limiter *rate.Limiter, opts ...MPUWorkerOption) *MPUWorker {
	w := &MPUWorker{
		store:    store,
		backend:  backend,
		deleter:  deleter,
		interval: interval,
		limiter:  limiter,
		logger:   log.Logger,
		now:      time.Now,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run blocks until ctx is done.
func (w *MPUWorker) Run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	w.runCycle(ctx)
	for {
		select {
		case <-t.C:
			w.runCycle(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// RunCycleForTest synchronously executes one scan cycle.
func (w *MPUWorker) RunCycleForTest(ctx context.Context) { w.runCycle(ctx) }

// AbortedTotal exposes the counter for status/metrics.
func (w *MPUWorker) AbortedTotal() int64 { return w.aborted.Load() }

func (w *MPUWorker) runCycle(ctx context.Context) {
	now := w.now()
	buckets, err := w.backend.ListBuckets(ctx)
	if err != nil {
		w.logger.Error().Err(err).Msg("lifecycle/mpu: list buckets")
		return
	}
	for _, bucket := range buckets {
		if ctx.Err() != nil {
			return
		}
		cfg, err := w.store.Get(bucket)
		if err != nil || cfg == nil {
			continue
		}
		uploads, err := w.backend.ScanLocalMultipartUploads(bucket)
		if err != nil {
			w.logger.Error().Str("bucket", bucket).Err(err).Msg("lifecycle/mpu: scan uploads")
			continue
		}
		for u := range uploads {
			if ctx.Err() != nil {
				go drainUploads(uploads)
				return
			}
			w.applyAbortRules(ctx, u, cfg.Rules, now)
		}
	}
}

func drainUploads(ch <-chan storage.MultipartUploadRecord) {
	for range ch {
	}
}

func (w *MPUWorker) applyAbortRules(ctx context.Context, u storage.MultipartUploadRecord, rules []Rule, now time.Time) {
	for _, r := range rules {
		if r.Status != StatusEnabled || r.AbortIncompleteMultipartUpload == nil {
			continue
		}
		// Filter ignored for AbortMPU beyond Prefix — uploads have no tags/size.
		if r.Filter != nil && r.Filter.Prefix != "" && !strings.HasPrefix(u.Key, r.Filter.Prefix) {
			continue
		}
		threshold := time.Unix(u.InitiatedAt, 0).
			Add(time.Duration(r.AbortIncompleteMultipartUpload.DaysAfterInitiation) * 24 * time.Hour)
		if now.Before(threshold) {
			continue
		}
		// Weighted limiter wait: 1 unit floor, len(parts) units when known.
		// Cap at burst — rate.Limiter.WaitN fails immediately when n > burst
		// rather than waiting, which would skip aborts for uploads with many
		// parts. The steady-state rate is the binding constraint anyway.
		weight := 1
		if n, err := w.deleter.MultipartUploadPartCount(u.Bucket, u.Key, u.UploadID); err == nil && n > 0 {
			weight = n
		}
		if b := w.limiter.Burst(); weight > b {
			weight = b
		}
		if err := w.limiter.WaitN(ctx, weight); err != nil {
			return
		}
		if err := w.deleter.AbortMultipartUpload(ctx, u.Bucket, u.Key, u.UploadID); err != nil {
			w.logger.Error().Str("bucket", u.Bucket).Str("key", u.Key).Str("uploadID", u.UploadID).Err(err).Msg("lifecycle/mpu: abort")
			continue
		}
		w.aborted.Add(1)
		return // rule matched
	}
}
