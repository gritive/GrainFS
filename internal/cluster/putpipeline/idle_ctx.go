package putpipeline

import (
	"context"
	"sync"
	"time"
)

// idleTimeoutContext returns a context that cancels when no reset() occurs
// within d of the previous progress event (or of creation). reset() is cheap and
// thread-safe; call it on each downstream progress event (e.g. a sink Write the
// peer actually consumed). stop() releases the timer and cancels.
//
// Monotonic cancellation: cancellation is one-way. A reset() that races a fire
// (or arrives after stop()) only re-arms an AfterFunc whose cancel() is
// idempotent, so a fired/stopped ctx can never be revived — it stays Done.
func idleTimeoutContext(parent context.Context, d time.Duration) (context.Context, func(), func()) {
	ctx, cancel := context.WithCancel(parent)
	var mu sync.Mutex
	timer := time.AfterFunc(d, cancel)
	reset := func() {
		mu.Lock()
		timer.Reset(d)
		mu.Unlock()
	}
	stop := func() {
		mu.Lock()
		timer.Stop()
		mu.Unlock()
		cancel()
	}
	return ctx, reset, stop
}
