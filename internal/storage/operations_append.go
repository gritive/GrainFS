package storage

import (
	"context"
	"io"
)

// AppendObject routes an S3 Express append through Operations so the HTTP handler
// stays backend-blind (single-path), like every other object op. Capability is
// resolved on the OUTERMOST backend only — byte-identical to the handler's former
// `s.backend.(AppendObjecter)` assertion (o.backend is the same backend the server
// holds). This deliberately does NOT walk the Unwrap() decorator chain: an outer
// wrapper that lacks AppendObject must yield UnsupportedOperationError, not
// silently append through an inner layer. Returns UnsupportedOperationError when
// the outermost backend has no append capability.
func (o *Operations) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error) {
	ap, ok := o.backend.(AppendObjecter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "AppendObject", Reason: UnsupportedReasonNoAdapter}
	}
	return ap.AppendObject(ctx, bucket, key, expectedOffset, r)
}
