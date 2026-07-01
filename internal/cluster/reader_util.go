package cluster

import (
	"context"
	"io"
)

// readerWithContext wraps an io.Reader so each Read first observes ctx
// cancellation, surfacing ctx.Err() instead of continuing to read.
type readerWithContext struct {
	ctx context.Context
	r   io.Reader
}

func (r readerWithContext) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.r.Read(p)
	}
}
