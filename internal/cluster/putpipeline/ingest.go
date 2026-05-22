package putpipeline

import (
	"context"
	"io"
)

// IngestActor reads an HTTP body into stripe-sized chunks and pushes
// them onto its out channel. One IngestActor goroutine per PUT.
type IngestActor struct {
	out         chan<- StripePlaintext
	stripeBytes int
}

// Run executes the ingest loop for one PUT. Returns the final ETag
// (MD5/XXH3 hex), total bytes read, and any error. Out channel close
// is the caller's responsibility (Pipeline.Put coordinates it).
func (a *IngestActor) Run(ctx context.Context, putID uint64, bucket string, body io.Reader) (etag string, total int64, err error) {
	// TODO Phase 2
	return "", 0, nil
}
