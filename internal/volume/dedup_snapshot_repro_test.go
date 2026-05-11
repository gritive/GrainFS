package volume

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDedupSnapshotSecondSnapshot reproduces the bug documented in
// tests/e2e/helpers_test.go: "dedup + snapshots is not implemented in Phase A
// — CoW E2E tests would fail starting from the second snapshot otherwise".
//
// After PR-B lands, this test must pass without error.
func TestDedupSnapshotSecondSnapshot(t *testing.T) {
	mgr := setupDedupManager(t) // existing helper in volume_test.go
	const name = "dedup-snap"
	_, err := mgr.Create(name, int64(64*1024))
	require.NoError(t, err)

	// Write block 0
	_, err = mgr.WriteAt(name, bytes.Repeat([]byte{0xAA}, DefaultBlockSize), 0)
	require.NoError(t, err)

	snap1, err := mgr.CreateSnapshot(name)
	require.NoError(t, err)
	assert.NotEmpty(t, snap1)

	// Overwrite block 0 with different content
	_, err = mgr.WriteAt(name, bytes.Repeat([]byte{0xBB}, DefaultBlockSize), 0)
	require.NoError(t, err)

	// Second snapshot — current code returns "dedup + snapshots not supported".
	snap2, err := mgr.CreateSnapshot(name)
	require.NoError(t, err, "second snapshot must succeed under dedup")
	assert.NotEmpty(t, snap2)

	// Read back snap1 content — must be 0xAA, not 0xBB.
	// (Read path under dedup snapshot is implemented later in this PR.)
	snaps, err := mgr.ListSnapshots(name)
	require.NoError(t, err)
	assert.Len(t, snaps, 2)
}
