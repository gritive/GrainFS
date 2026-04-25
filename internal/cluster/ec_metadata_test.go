package cluster

// ec_metadata_test.go: NodeIDs-in-metadata fallback path for EC GetObject.
//
// When no ring is set up (RingVersion=0) and CmdPutShardPlacement is a no-op,
// GetObject must still reconstruct EC objects using NodeIDs stored in
// PutObjectMetaCmd. These tests verify that path end-to-end.
//
// To force EC active (IsActive requires >= 3 nodes), we set allNodes to
// contain the same selfAddr repeated 3 times. liveNodes() returns 3 entries,
// IsActive(3)=true, and PlacementForNodes routes all shards to selfAddr (local).

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupECBackend creates a backend with EC active (k=1, m=1) but no ring.
// The selfAddr is repeated 3 times in allNodes so IsActive(3)=true and all
// shards are placed locally (PlacementForNodes maps all to selfAddr).
func setupECBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	// Set allNodes to 3 copies of selfAddr: IsActive(3)=true, all shards local.
	backend.shardSvc = svc
	backend.selfAddr = selfAddr
	backend.allNodes = []string{selfAddr, selfAddr, selfAddr}
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})
	return backend
}

// TestEC_NoRing_PutGetRoundTrip verifies that PutObject + GetObject round-trips
// correctly when EC is active but no ring has been initialized.
// This is the regression test for the CmdPutShardPlacement no-op bug:
// placement NodeIDs must be stored in PutObjectMetaCmd so GetObject can find shards.
func TestEC_NoRing_PutGetRoundTrip(t *testing.T) {
	backend := setupECBackend(t)

	// No ring initialized — RingVersion stays 0.
	// CmdPutShardPlacement is a no-op — no placement records in FSM.
	require.NoError(t, backend.CreateBucket("bucket"))
	content := bytes.Repeat([]byte("hello-ec-no-ring-"), 50) // 850 bytes

	_, err := backend.PutObject("bucket", "key", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	// GetObject must succeed using NodeIDs stored in object metadata.
	rc, obj, err := backend.GetObject("bucket", "key")
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got, "content must round-trip via EC with no ring (NodeIDs from metadata)")
}

// TestEC_NoRing_LargeObject verifies a larger object round-trips correctly via EC
// with no ring and no placement records.
func TestEC_NoRing_LargeObject(t *testing.T) {
	backend := setupECBackend(t)

	require.NoError(t, backend.CreateBucket("b"))
	content := bytes.Repeat([]byte("large-ec-object-"), 4096) // 65536 bytes

	_, err := backend.PutObject("b", "big", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)

	rc, obj, err := backend.GetObject("b", "big")
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got, "large object must round-trip via EC with no ring")
}
