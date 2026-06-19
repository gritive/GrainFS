package cluster

import (
	"context"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestPutObjectArgs_SoleAuthEpochRoundTrip(t *testing.T) {
	// epochWire 6 → field carries 6.
	args := buildPutObjectArgsWithSSE("b", "k", "", nil, "", nil, "", 0, 0 /*versioningState*/, 6 /*soleAuthEpochWire*/)
	require.Equal(t, uint32(6), raftpb.GetRootAsPutObjectArgs(args, 0).SoleauthEpoch())

	// epochWire 0 → field absent → default 0.
	args = buildPutObjectArgsWithSSE("b", "k", "", nil, "", nil, "", 0, 0, 0)
	require.Equal(t, uint32(0), raftpb.GetRootAsPutObjectArgs(args, 0).SoleauthEpoch())

	// Old peer: args built directly without AddSoleauthEpoch → default 0.
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString("b")
	k := b.CreateString("k")
	raftpb.PutObjectArgsStart(b)
	raftpb.PutObjectArgsAddBucket(b, bk)
	raftpb.PutObjectArgsAddKey(b, k)
	b.Finish(raftpb.PutObjectArgsEnd(b))
	require.Equal(t, uint32(0), raftpb.GetRootAsPutObjectArgs(b.FinishedBytes(), 0).SoleauthEpoch())
}

func TestSoleAuthEpochContextRoundTrip(t *testing.T) {
	epoch, resolved := bucketSoleAuthEpochFromContext(context.Background())
	require.False(t, resolved, "background context must be unresolved")
	require.Equal(t, uint32(0), epoch)

	ctx := ContextWithBucketSoleAuthEpoch(context.Background(), 0)
	epoch, resolved = bucketSoleAuthEpochFromContext(ctx)
	require.True(t, resolved)
	require.Equal(t, uint32(0), epoch)

	ctx = ContextWithBucketSoleAuthEpoch(context.Background(), 5)
	epoch, resolved = bucketSoleAuthEpochFromContext(ctx)
	require.True(t, resolved)
	require.Equal(t, uint32(5), epoch)
}

func TestSoleAuthEpochWireEncoding(t *testing.T) {
	// Unresolved context encodes as absent (0).
	require.Equal(t, uint32(0), soleAuthEpochToWire(context.Background()))

	// Stamped epochs use the +1 encoding.
	require.Equal(t, uint32(1), soleAuthEpochToWire(ContextWithBucketSoleAuthEpoch(context.Background(), 0)))
	require.Equal(t, uint32(6), soleAuthEpochToWire(ContextWithBucketSoleAuthEpoch(context.Background(), 5)))

	// Wire 0 leaves the context unresolved (local fallback).
	_, resolved := bucketSoleAuthEpochFromContext(contextWithSoleAuthEpochWire(context.Background(), 0))
	require.False(t, resolved)

	// Wire n>=1 resolves to epoch n-1.
	epoch, resolved := bucketSoleAuthEpochFromContext(contextWithSoleAuthEpochWire(context.Background(), 1))
	require.True(t, resolved)
	require.Equal(t, uint32(0), epoch)

	epoch, resolved = bucketSoleAuthEpochFromContext(contextWithSoleAuthEpochWire(context.Background(), 6))
	require.True(t, resolved)
	require.Equal(t, uint32(5), epoch)

	// Full round-trip across representative epochs (including max uint32-1).
	for _, n := range []uint32{0, 5, 4294967294} {
		ctx := contextWithSoleAuthEpochWire(context.Background(), soleAuthEpochToWire(ContextWithBucketSoleAuthEpoch(context.Background(), n)))
		got, resolved := bucketSoleAuthEpochFromContext(ctx)
		require.True(t, resolved, "epoch %d must resolve", n)
		require.Equal(t, n, got)
	}
}
