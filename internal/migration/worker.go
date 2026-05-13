package migration

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// Worker processes StatusRunning migration jobs. It runs on the leader node
// only. New jobs are signalled via Trigger; startup also resumes any jobs
// that were running before a leadership change or crash.
type Worker struct {
	store    *JobStore
	src      Source
	dst      Destination
	proposer Proposer
	trigger  chan struct{}
	interval time.Duration
}

func newWorker(store *JobStore, src Source, dst Destination, proposer Proposer, interval time.Duration) *Worker {
	return &Worker{
		store:    store,
		src:      src,
		dst:      dst,
		proposer: proposer,
		trigger:  make(chan struct{}, 1),
		interval: interval,
	}
}

// Trigger wakes the worker to check for pending jobs. Non-blocking; if the
// worker is already processing a signal is dropped (it will drain the queue
// anyway on the next cycle).
func (w *Worker) Trigger() {
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled. It processes running jobs on startup,
// on every Trigger call, and (if interval > 0) on each ticker tick. The ticker
// ensures cross-node job pickup: if SubmitJob was called on a follower the
// trigger dropped, but the leader's periodic scan will catch the running job.
func (w *Worker) Run(ctx context.Context) {
	w.processAll(ctx)
	var tickC <-chan time.Time
	if w.interval > 0 {
		tick := time.NewTicker(w.interval)
		defer tick.Stop()
		tickC = tick.C
	}
	for {
		select {
		case <-w.trigger:
			w.processAll(ctx)
		case <-tickC:
			w.processAll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) processAll(ctx context.Context) {
	jobs, err := w.store.ListJobs(StatusRunning)
	if err != nil {
		log.Error().Err(err).Msg("migration: list running jobs")
		return
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return
		}
		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *JobState) {
	bucket := job.Bucket
	var totalCopied, totalErrors int64

	if err := w.dst.CreateBucket(ctx, bucket); err != nil {
		if !errors.Is(err, storage.ErrBucketAlreadyExists) {
			log.Error().Str("bucket", bucket).Err(err).Msg("migration: create bucket")
			if propErr := w.proposer.ProposeJobFailed(ctx, bucket, err.Error(), totalErrors); propErr != nil {
				log.Error().Str("bucket", bucket).Err(propErr).Msg("migration: propose job failed")
			}
			return
		}
	}

	cursor, err := w.store.GetCursor(bucket)
	if err != nil {
		log.Error().Str("bucket", bucket).Err(err).Msg("migration: get cursor")
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}
		keys, next, err := w.src.ListObjectsPage(bucket, cursor)
		if err != nil {
			log.Error().Str("bucket", bucket).Err(err).Msg("migration: list page")
			if propErr := w.proposer.ProposeJobFailed(ctx, bucket, err.Error(), totalErrors); propErr != nil {
				log.Error().Str("bucket", bucket).Err(propErr).Msg("migration: propose job failed")
			}
			return
		}
		for _, key := range keys {
			if ctx.Err() != nil {
				return
			}
			if err := w.copyObject(ctx, bucket, key); err != nil {
				log.Warn().Str("bucket", bucket).Str("key", key).Err(err).Msg("migration: copy object")
				totalErrors++
			} else {
				totalCopied++
			}
		}
		cursor = next
		if err := w.store.SaveCursor(bucket, cursor); err != nil {
			log.Error().Str("bucket", bucket).Err(err).Msg("migration: save cursor")
		}
		if next == "" {
			break
		}
	}

	if err := w.proposer.ProposeJobDone(ctx, bucket, totalCopied, totalErrors); err != nil {
		log.Error().Str("bucket", bucket).Err(err).Msg("migration: propose done")
	}
}

func (w *Worker) copyObject(ctx context.Context, bucket, key string) error {
	rc, obj, err := w.src.GetObject(bucket, key)
	if err != nil {
		return err
	}
	defer rc.Close()
	ct := ""
	if obj != nil {
		ct = obj.ContentType
	}
	_, err = w.dst.PutObject(ctx, bucket, key, rc, ct)
	return err
}
