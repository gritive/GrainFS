package putpipeline

import (
	"context"
	"io"
	"sync"
	"time"
)

// progressReader wraps an io.Reader and calls onProgress after each read that
// yields bytes. It feeds the idle deadline the "client is still uploading"
// signal: while a slow client keeps delivering bytes, the per-PUT idle timer
// keeps resetting and never fires, even before a full stripe has accumulated to
// flush downstream.
type progressReader struct {
	r          io.Reader
	onProgress func()
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if n > 0 && pr.onProgress != nil {
		pr.onProgress()
	}
	return n, err
}

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
