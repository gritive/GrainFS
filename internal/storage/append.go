package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// MaxAppendSegments caps appendable segments per object.
// var (not const) to allow test-time override. AWS S3 Express AppendObject 한계 = 10000.
var MaxAppendSegments = 10000

var (
	ErrAppendOffsetMismatch = errors.New("append: write offset does not match object size")
	ErrAppendCapExceeded    = errors.New("append: segment cap reached")
	ErrAppendNotSupported   = errors.New("append: object is not appendable")
)

// AppendObjecter is the optional interface for S3 Express AppendObject support.
// Server-side HTTP handler probes via type assertion; backends without support
// get a clean 501 NotImplemented. Mirrors existing optional-interface pattern
// (Truncatable, ACLSetter, RequestPutter, UserMetadataPutter).
type AppendObjecter interface {
	AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error)
}

// AppendObject appends data to an existing appendable object, or creates a new
// appendable object when expectedOffset == 0 and the object does not exist.
//
// Returns ErrAppendOffsetMismatch if expectedOffset != current object size,
// ErrAppendCapExceeded if the object already has MaxAppendSegments segments,
// ErrAppendNotSupported if the existing object is not appendable.
func (b *LocalBackend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error) {
	existing, err := b.HeadObject(ctx, bucket, key)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, fmt.Errorf("head: %w", err)
	}
	if existing == nil {
		if expectedOffset != 0 {
			return nil, ErrAppendOffsetMismatch
		}
		return b.appendNew(ctx, bucket, key, r)
	}
	if !existing.IsAppendable {
		return nil, ErrAppendNotSupported
	}
	if existing.Size != expectedOffset {
		return nil, ErrAppendOffsetMismatch
	}
	if len(existing.Segments) >= MaxAppendSegments {
		return nil, ErrAppendCapExceeded
	}
	return b.appendExisting(ctx, bucket, key, existing, r)
}

func (b *LocalBackend) appendNew(ctx context.Context, bucket, key string, r io.Reader) (*Object, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *LocalBackend) appendExisting(ctx context.Context, bucket, key string, existing *Object, r io.Reader) (*Object, error) {
	return nil, fmt.Errorf("not implemented")
}
