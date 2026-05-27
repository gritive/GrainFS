package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// largeShardPayload returns a payload at/above walPayloadInlineThreshold so
// appendShardDataWAL takes the metadata-only (no-inline) branch.
func largeShardPayload() []byte {
	return bytes.Repeat([]byte("x"), walPayloadInlineThreshold)
}

// TestAppendShardDataWAL_NoRedundancyForcesFsyncForLargeShard asserts that on a
// single-node deployment (ParityShards==0) a large metadata-only shard write
// forces a direct shard-file fsync: with no parity and no peers, EC
// reconstruction cannot rebuild a page-cache-lost shard, so the WAL's
// metadata-only record is not enough — the file itself must be durable.
func TestAppendShardDataWAL_NoRedundancyForcesFsyncForLargeShard(t *testing.T) {
	enc := testEncryptor(t)
	svc := NewShardService(t.TempDir(), nil,
		WithEncryptor(enc),
		withTestWALEnc(t, enc),
		WithNoRedundancy(func() bool { return true }),
	)

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, largeShardPayload())
	require.NoError(t, err)
	require.True(t, requireFsync, "no-redundancy large shard must fsync the shard file directly")
}

// TestAppendShardDataWAL_NoRedundancyKeepsSmallShardWALOnly asserts the small
// (inlined) payload path is unchanged even under no-redundancy: the WAL inlines
// the full payload, so replay rebuilds the file and no direct fsync is needed.
func TestAppendShardDataWAL_NoRedundancyKeepsSmallShardWALOnly(t *testing.T) {
	enc := testEncryptor(t)
	svc := NewShardService(t.TempDir(), nil,
		WithEncryptor(enc),
		withTestWALEnc(t, enc),
		WithNoRedundancy(func() bool { return true }),
	)

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, []byte("small"))
	require.NoError(t, err)
	require.False(t, requireFsync, "small inlined payload is WAL-covered; no direct fsync")
}

// TestAppendShardDataWAL_RedundancyLargeShardWALOnly asserts the parity>0 case
// is unchanged: a large metadata-only write relies on EC reconstruction, so no
// direct shard fsync is forced.
func TestAppendShardDataWAL_RedundancyLargeShardWALOnly(t *testing.T) {
	enc := testEncryptor(t)
	svc := NewShardService(t.TempDir(), nil,
		WithEncryptor(enc),
		withTestWALEnc(t, enc),
		WithNoRedundancy(func() bool { return false }),
	)

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, largeShardPayload())
	require.NoError(t, err)
	require.False(t, requireFsync, "with parity, large shard relies on EC reconstruction")
}

// TestAppendShardDataWAL_NilNoRedundancyNeverForcesFsync asserts the default
// (no provider wired, as in legacy callers/tests) never forces a direct fsync.
func TestAppendShardDataWAL_NilNoRedundancyNeverForcesFsync(t *testing.T) {
	enc := testEncryptor(t)
	svc := NewShardService(t.TempDir(), nil, WithEncryptor(enc), withTestWALEnc(t, enc)) // no WithNoRedundancy

	requireFsync, err := svc.appendShardDataWAL(context.Background(), "b", "k", 0, largeShardPayload())
	require.NoError(t, err)
	require.False(t, requireFsync, "nil noRedundancy provider must not force fsync")
}
