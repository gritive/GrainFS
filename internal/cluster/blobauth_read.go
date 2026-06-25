package cluster

import (
	"fmt"
)

// blobAuthReadOn reports whether the bucket's per-version quorum-meta blob tree
// is the BLOB AUTHORITY for reads: true for every versioning-enabled bucket.
// It FAILS CLOSED — on any error reading the versioning state it returns
// (false, err) so callers surface the error rather than silently treating the
// bucket as not-blob-authoritative.
//
// NOTE: the legacy blob-authority tri-state flag + epoch fence it once consulted
// were removed in the blob-authority teardown; this now reads bucket versioning
// directly (blob authority == versioning-enabled).
func (b *DistributedBackend) blobAuthReadOn(bucket string) (bool, error) {
	state, err := b.GetBucketVersioning(bucket)
	if err != nil {
		return false, fmt.Errorf("read versioning state for bucket %q: %w", bucket, err)
	}
	return state == "Enabled", nil
}
