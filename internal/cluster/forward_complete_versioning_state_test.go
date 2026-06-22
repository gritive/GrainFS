package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestForwardCompleteMultipart_CarriesVersioningState proves the forward
// CompleteMultipartUpload path carries the bucket-versioning tri-state across
// the wire and the receiver re-stamps it onto the ctx that reaches the backend
// complete — the prerequisite for the blob-authoritative multipart-complete
// write (the leaf data-group commit backend cannot read bucket versioning
// itself, so the S3 edge stamps it; mirrors PutObjectArgs.versioning_state and
// TestForwardListObjects_CarriesAndReStampsVersioningState).
func TestForwardCompleteMultipart_CarriesVersioningState(t *testing.T) {
	parts := []storage.Part{{PartNumber: 1, ETag: "e1"}}

	stampFromArgs := func(state byte) (enabled, resolved bool) {
		args := buildCompleteMultipartUploadArgs("vbkt", "key", "uid", parts, state)
		ca := raftpb.GetRootAsCompleteMultipartUploadArgs(args, 0)
		require.Equal(t, state, ca.VersioningState(),
			"tri-state must survive the CompleteMultipartUploadArgs wire round-trip")
		require.Equal(t, 1, ca.PartsLength(), "parts must still round-trip alongside the new field")
		ctx := contextWithVersioningState(context.Background(), ca.VersioningState())
		return bucketVersioningFromContext(ctx)
	}

	// UNKNOWN: receiver leaves ctx unresolved → falls back to a local read
	// (mirrors an old peer that omits the field).
	_, resolved := stampFromArgs(versioningStateUnknown)
	require.False(t, resolved,
		"UNKNOWN must leave ctx unresolved so the receiver falls back to a local read")

	// ENABLED: receiver re-stamps ENABLED → the complete runs under the
	// authoritative versioning decision without a local versioning read.
	enabled, resolved := stampFromArgs(versioningStateEnabled)
	require.True(t, resolved && enabled,
		"the ENABLED stamp must cross the wire and re-stamp the receiver ctx Enabled")

	// DISABLED: the authoritative not-enabled decision crosses the wire too,
	// so the receiver keeps the legacy (non-blob) multipart-complete path.
	enabled, resolved = stampFromArgs(versioningStateDisabled)
	require.True(t, resolved && !enabled,
		"the DISABLED stamp must cross the wire and re-stamp the receiver ctx Disabled")
}
