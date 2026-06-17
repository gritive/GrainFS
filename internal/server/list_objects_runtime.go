package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) listBucketObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	// Stamp the authoritative bucket-versioning decision at the S3 edge (mirror
	// PUT) so the per-version LIST derive (S2b PR-B) and the cluster-wide gate
	// activate without the coordinator reading the control-plane bucketver state.
	ctx = s.ctxWithBucketVersioning(ctx, bucket)
	return s.ops.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
}
