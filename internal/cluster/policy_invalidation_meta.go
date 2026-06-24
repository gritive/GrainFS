package cluster

import (
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/rs/zerolog/log"
)

const metaPolicyInvalidationQueueDepth = 64

// MetaPolicyInvalidationWorker handles nonblocking policy-cache invalidation
// driven by meta post-commit hooks. It owns a buffered channel and a background
// goroutine that drains the channel and calls the injected invalidation callback.
//
// Design rationale: PostCommitHook MUST NOT block (post_commit.go:13). The
// compiled-policy Invalidate and IAM-resolver invalidate each acquire their own
// mutex, so calling them on the FSM apply goroutine could block. The worker
// accepts the bucket name via a buffered channel (nonblocking send with a drop
// on overflow) and drains it in a separate goroutine.
//
// Late-binding: SetInvalidate may be called after Start to wire the actual
// invalidation function once the compiled-policy store is available (e.g.
// after server.New in boot). Until SetInvalidate is called, events are
// silently dropped (the pull-on-miss guarantee ensures eventual consistency).
//
// Lifecycle: call Start before RegisterPostCommit; call Stop at shutdown (it
// closes the channel and waits for the goroutine to drain and exit — no goroutine
// leak). Start/Stop are not concurrency-safe; call each exactly once.
type MetaPolicyInvalidationWorker struct {
	invalidate atomic.Pointer[func(bucket string)]
	ch         chan string
	done       chan struct{}
}

// NewMetaPolicyInvalidationWorker constructs a worker with no invalidation
// callback set. Call SetInvalidate to wire the callback before (or shortly
// after) Start.
func NewMetaPolicyInvalidationWorker() *MetaPolicyInvalidationWorker {
	return &MetaPolicyInvalidationWorker{
		ch:   make(chan string, metaPolicyInvalidationQueueDepth),
		done: make(chan struct{}),
	}
}

// SetInvalidate wires the invalidation callback. Safe to call concurrently and
// may be called after Start. Until called, drain events are silently dropped.
func (w *MetaPolicyInvalidationWorker) SetInvalidate(fn func(bucket string)) {
	w.invalidate.Store(&fn)
}

// Start launches the background drain goroutine. Must be called before the FSM
// apply loop starts (i.e. before MetaRaft.Start()).
func (w *MetaPolicyInvalidationWorker) Start() {
	go w.run()
}

// Stop closes the input channel and blocks until the drain goroutine exits.
// Pending items in the channel are drained and invalidated before the goroutine
// returns (provided SetInvalidate has been called by then).
func (w *MetaPolicyInvalidationWorker) Stop() {
	close(w.ch)
	<-w.done
}

func (w *MetaPolicyInvalidationWorker) run() {
	defer close(w.done)
	for bucket := range w.ch {
		fn := w.invalidate.Load()
		if fn == nil {
			// Callback not yet wired; drop silently — pull-on-miss recovers.
			continue
		}
		(*fn)(bucket)
	}
}

// Hook implements cluster.PostCommitHook. It is called from the FSM apply
// goroutine; it MUST NOT block. It enqueues the bucket name for the drain
// goroutine. If the channel is full (queue overflow) the invalidation is
// dropped with a warning — the compiled-policy store pull-on-miss ensures
// the next authz Allow will re-fetch the committed policy anyway.
func (w *MetaPolicyInvalidationWorker) Hook(cmdType clusterpb.MetaCmdType, payload []byte) {
	switch cmdType {
	case clusterpb.MetaCmdTypeSetBucketPolicy:
		bucket, _, err := decodeMetaSetBucketPolicyCmd(payload)
		if err != nil || bucket == "" {
			log.Warn().Err(err).Msg("policy_invalidation_meta: failed to decode SetBucketPolicy payload; skipping invalidation")
			return
		}
		w.enqueue(bucket)
	case clusterpb.MetaCmdTypeDeleteBucketPolicy:
		bucket, err := decodeMetaDeleteBucketPolicyCmd(payload)
		if err != nil || bucket == "" {
			log.Warn().Err(err).Msg("policy_invalidation_meta: failed to decode DeleteBucketPolicy payload; skipping invalidation")
			return
		}
		w.enqueue(bucket)
	}
}

func (w *MetaPolicyInvalidationWorker) enqueue(bucket string) {
	select {
	case w.ch <- bucket:
	default:
		log.Warn().Str("bucket", bucket).Msg("policy_invalidation_meta: invalidation queue full; dropping bucket invalidation (pull-on-miss will recover)")
	}
}
