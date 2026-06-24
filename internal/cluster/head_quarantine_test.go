package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestHeadObject_Quarantined proves that HeadObject returns ErrObjectQuarantined
// for a quarantined latest object — matching the GET parity requirement.
func TestHeadObject_Quarantined(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("data"))
	require.NoError(t, b.QuarantineObject(ctx, "b", "k", "", "corrupt-shard", "scrub"))

	_, err := b.HeadObject(ctx, "b", "k")
	require.ErrorIs(t, err, ErrObjectQuarantined, "HeadObject must return ErrObjectQuarantined for a quarantined object")
}

// TestHeadObjectVersion_Quarantined proves that HeadObjectVersion returns
// ErrObjectQuarantined for a quarantined specific version — matching the
// GetObjectVersion parity requirement. Uses a versioning-enabled bucket so the
// put produces a non-empty VersionID that the quarantine gate can match.
func TestHeadObjectVersion_Quarantined(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	require.NoError(t, b.SetBucketVersioning("vb", "Enabled"))

	size := int64(4)
	obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:      "vb",
		Key:         "k",
		Body:        bytes.NewReader([]byte("data")),
		ContentType: "application/octet-stream",
		SizeHint:    &size,
	})
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID, "versioning-enabled bucket must produce a VersionID")

	require.NoError(t, b.QuarantineObject(ctx, "vb", "k", obj.VersionID, "corrupt-shard", "scrub"))

	_, err = b.HeadObjectVersion(ctx, "vb", "k", obj.VersionID)
	require.ErrorIs(t, err, ErrObjectQuarantined, "HeadObjectVersion must return ErrObjectQuarantined for a quarantined version")
}
