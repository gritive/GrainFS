// S3 Express AppendObject contract.
//
// AppendObject support is an optional backend capability. The server-side HTTP
// handler probes for it via the AppendObjecter type assertion; a backend without
// support gets a clean 501 NotImplemented. The production implementation lives on
// the cluster data path (internal/cluster); the declarations below are the shared
// contract (interface + error sentinels + segment limit) used across the server,
// cluster, packblob, and pullthrough layers.

package storage

import (
	"context"
	"errors"
	"io"
)

// MaxAppendSegments caps appendable segments per object.
// var (not const) to allow test-time override. AWS S3 Express AppendObject 한계 = 10000.
var MaxAppendSegments = 10000

var (
	ErrAppendOffsetMismatch = errors.New("append: write offset does not match object size")
	ErrAppendCapExceeded    = errors.New("append: segment cap reached")
	ErrAppendNotSupported   = errors.New("append: object is not appendable")
	ErrAppendObjectTooLarge = errors.New("append: object total size cap reached")
)

// AppendObjecter is the optional interface for S3 Express AppendObject support.
// Server-side HTTP handler probes via type assertion; backends without support
// get a clean 501 NotImplemented. Mirrors existing optional-interface pattern
// (ACLSetter, RequestPutter, UserMetadataPutter).
type AppendObjecter interface {
	AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error)
}
