package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

// MutationObserver receives notifications after a successful S3-level
// state-changing operation. The broker (see MutationBroker) fans out to
// every registered observer; each observer decides its own sync/async
// semantics. Methods MUST NOT block the request hot path beyond what the
// observer is documented to do.
//
// Adding a new mutation method here forces every observer (including any
// future ones) to recompile, which is the contract: "if you observe
// mutations, you must handle every kind."
type MutationObserver interface {
	OnObjectWrite(ctx context.Context, bucket, key string, result *storage.PutObjectResult)
	OnObjectDelete(ctx context.Context, bucket, key string, result *storage.DeleteObjectResult)
	OnObjectCopy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, result *storage.CopyObjectResult)
	OnBucketCreate(ctx context.Context, bucket string)
	OnBucketDelete(ctx context.Context, bucket string)
}
