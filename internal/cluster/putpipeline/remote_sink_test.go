package putpipeline

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// fakeShardWAL is a no-op DataWALAppender: the remote-sink tests verify the
// transport + verbatim-store + read-back, not WAL durability (covered in the
// cluster package). The shard FILE is written regardless of the WAL.
type fakeShardWAL struct{}

func (fakeShardWAL) Append(context.Context, datawal.Record) (uint64, error) { return 0, nil }
func (fakeShardWAL) AppendReader(context.Context, datawal.Record, io.Reader) (uint64, error) {
	return 0, nil
}
func (fakeShardWAL) Flush() error { return nil }

func newSinkTestShardService(t *testing.T, tr *transport.TCPTransport, keeper *encrypt.DEKKeeper, clusterID []byte) *cluster.ShardService {
	t.Helper()
	return cluster.NewShardService(t.TempDir(), tr,
		cluster.WithShardDEKKeeper(keeper, clusterID),
		cluster.WithDataWAL(fakeShardWAL{}))
}

// TestRemoteSealedShardSink_RoundTrip proves the sender side of the streaming-EC
// path: sealed-at-source bytes pushed through the sink reach the peer's verbatim
// WriteSealedShard RPC and read back to the original plaintext (no double-encrypt,
// no corruption across the network hop).
func TestRemoteSealedShardSink_RoundTrip(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewTCPTransport("test-cluster-psk")
	tr2 := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	recv := newSinkTestShardService(t, tr2, keeper, clusterID)
	tr2.HandleBody(transport.StreamShardWriteBody, recv.HandleWriteBody())

	// Source seals with the cluster DEK (seal-at-source); sealer uses the same
	// keeper/clusterID so the receiver can decrypt on read.
	sealer := newSinkTestShardService(t, tr1, keeper, clusterID)
	plaintext := bytes.Repeat([]byte("remote sink shard "), 4096)
	sealed, err := sealer.EncodeEncryptedShardBuffer("bkt", "obj", 0, plaintext)
	require.NoError(t, err)

	sink := newRemoteSealedShardSink(ctx, tr1, tr2.LocalAddr(), "bkt", "obj", 0)
	// Two chunks to exercise streaming/backpressure, not a single Write.
	_, err = sink.Write(sealed[:1000])
	require.NoError(t, err)
	_, err = sink.Write(sealed[1000:])
	require.NoError(t, err)
	require.NoError(t, sink.Finalize())

	got, err := recv.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestRemoteSealedShardSink_AbortDoesNotCommit is the critical failure-semantics
// test: a sink Abort (after a partial Write) must leave NO shard committed on the
// peer — the receiver's body Read errors and the write is rejected. This is the
// shardSink self-clean contract verified over the real transport.
func TestRemoteSealedShardSink_AbortDoesNotCommit(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewTCPTransport("test-cluster-psk")
	tr2 := transport.MustNewTCPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	// Explicit recv dir so we can assert FILE ABSENCE, not just a read error: a
	// committed-but-corrupt partial shard would also make ReadLocalShard error
	// (AEAD decrypt fails on truncated bytes), so a read-error assertion alone
	// would stay green even if Abort regressed to a clean Close (commit partial).
	// File absence distinguishes "nothing committed" (the contract) from "corrupt
	// shard committed" (a violation).
	recvDir := t.TempDir()
	recv := cluster.NewShardService(recvDir, tr2,
		cluster.WithShardDEKKeeper(keeper, clusterID),
		cluster.WithDataWAL(fakeShardWAL{}))
	tr2.HandleBody(transport.StreamShardWriteBody, recv.HandleWriteBody())

	sealer := newSinkTestShardService(t, tr1, keeper, clusterID)
	plaintext := bytes.Repeat([]byte("aborted shard "), 4096)
	sealed, err := sealer.EncodeEncryptedShardBuffer("bkt", "obj", 1, plaintext)
	require.NoError(t, err)

	sink := newRemoteSealedShardSink(ctx, tr1, tr2.LocalAddr(), "bkt", "obj", 1)
	_, _ = sink.Write(sealed[:len(sealed)/2]) // partial
	sink.Abort()

	_, statErr := os.Stat(filepath.Join(recvDir, "shards", "bkt", "obj", "shard_1"))
	require.True(t, os.IsNotExist(statErr), "aborted sink must commit NO shard file on the peer (not even a corrupt partial)")
}
