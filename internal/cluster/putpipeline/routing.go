package putpipeline

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

// ShouldUseActor returns true when a PUT request is eligible for the
// actor pipeline. Phase-5 wiring queries this from DistributedBackend
// before dispatching. The criteria are:
//
//   - The target is an external (S3-visible) bucket — internal GrainFS
//     buckets keep the legacy path. This is a routing decision, the
//     ONLY sanctioned use of IsInternalBucket per the guard doc in
//     internal/storage/bucket.go; the pipeline's ETag hashing therefore
//     never has to branch on bucket class.
//   - All placement targets are local (single-node deployment)
//   - An encryptor is wired (the pipeline writes GFSENC3 only)
//   - The request is not a multipart part (multipart goes through its
//     own code path)
//
// Cluster-mode PUT, plaintext deployments, internal buckets, and
// multipart parts fall back to the legacy spool/EC writer code in
// cluster/backend.go.
func ShouldUseActor(ctx context.Context, bucket string, cfg cluster.ECConfig, placement []string, selfID string, encrypted bool, isMultipartPart bool) bool {
	if !encrypted {
		return false
	}
	if isMultipartPart {
		return false
	}
	if storage.IsInternalBucket(bucket) {
		return false
	}
	if cfg.NumShards() == 0 {
		return false
	}
	for _, p := range placement {
		if p != selfID {
			return false
		}
	}
	return true
}
