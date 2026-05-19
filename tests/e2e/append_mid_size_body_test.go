package e2e

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAppendMidSizeBodyE2E proves AppendObject accepts 8 MiB bodies after
// DefaultMaxForwardBodyBytes was raised from 5 MiB to 64 MiB (matching the
// HTTP layer appendBodyMaxBytes). See design 2026-05-19 § Follow-up 3.
func TestAppendMidSizeBodyE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		tgt := newSingleNodeS3Target()
		runMidSizeAppendCase(t, tgt)
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		tgt := newSharedClusterS3Target(t)
		runMidSizeAppendCase(t, tgt)
	})
}

func runMidSizeAppendCase(t *testing.T, tgt s3Target) {
	t.Helper()
	bucket := "append-mid-" + tgt.name
	tgt.createBkt(t, bucket)
	key := "obj-8mib"
	body := bytes.Repeat([]byte("m"), 8*1024*1024)
	require.NoError(t, putAppend(tgt.pickNode(0), bucket, key, 0, body))
	lastNode := tgt.nodes - 1
	if lastNode < 0 {
		lastNode = 0
	}
	got := getObject(t, tgt.pickNode(lastNode), bucket, key)
	require.Equal(t, body, got)
}
