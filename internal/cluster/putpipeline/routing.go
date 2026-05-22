package putpipeline

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster"
)

// ShouldUseActor returns true when a PUT request is eligible for the
// actor pipeline. Phase-5 wiring queries this from DistributedBackend
// before dispatching. The criteria are:
//
//   - All placement targets are local (single-node deployment)
//   - An encryptor is wired (the pipeline writes GFSENC2 only)
//   - The request is not a multipart part (multipart goes through its
//     own code path)
//
// Cluster-mode PUT, plaintext deployments, and multipart parts fall
// back to the legacy spool/EC writer code in cluster/backend.go.
func ShouldUseActor(ctx context.Context, cfg cluster.ECConfig, placement []string, selfID string, encrypted bool, isMultipartPart bool) bool {
	if !encrypted {
		return false
	}
	if isMultipartPart {
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
