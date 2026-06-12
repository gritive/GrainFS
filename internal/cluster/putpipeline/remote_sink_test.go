package putpipeline

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func newSinkTestShardService(t *testing.T, tr *transport.HTTPTransport, keeper *encrypt.DEKKeeper, clusterID []byte) *cluster.ShardService {
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

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	recv := newSinkTestShardService(t, tr2, keeper, clusterID)
	tr2.RegisterShardWriteHandler(recv.NativeWriteHandler()) // native /shard/write route (Phase 8 N6)
	tr2.HandleBody(transport.StreamShardWriteBody, recv.HandleWriteBody())

	// Source seals with the cluster DEK (seal-at-source); sealer uses the same
	// keeper/clusterID so the receiver can decrypt on read.
	sealer := newSinkTestShardService(t, tr1, keeper, clusterID)
	plaintext := bytes.Repeat([]byte("remote sink shard "), 4096)
	sealed, err := sealer.EncodeEncryptedShardBuffer("bkt", "obj", 0, plaintext)
	require.NoError(t, err)

	sink := newRemoteSealedShardSink(ctx, nil, tr1, tr2.LocalAddr(), "bkt", "obj", 0)
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
// peer. Over HTTP a mid-stream abort surfaces to the server as a clean EOF, so
// the receiver cannot rely on a body-read error; instead Abort omits the
// completeness trailer that Finalize would write, so the receiver's declared-vs-
// received length check rejects the partial. This is the shardSink self-clean
// contract verified over the real transport.
func TestRemoteSealedShardSink_AbortDoesNotCommit(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
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
	tr2.RegisterShardWriteHandler(recv.NativeWriteHandler()) // native /shard/write route (Phase 8 N6)
	tr2.HandleBody(transport.StreamShardWriteBody, recv.HandleWriteBody())

	sealer := newSinkTestShardService(t, tr1, keeper, clusterID)
	plaintext := bytes.Repeat([]byte("aborted shard "), 4096)
	sealed, err := sealer.EncodeEncryptedShardBuffer("bkt", "obj", 1, plaintext)
	require.NoError(t, err)

	sink := newRemoteSealedShardSink(ctx, nil, tr1, tr2.LocalAddr(), "bkt", "obj", 1)
	_, _ = sink.Write(sealed[:len(sealed)/2]) // partial
	sink.Abort()

	_, statErr := os.Stat(filepath.Join(recvDir, "shards", "bkt", "obj", "shard_1"))
	require.True(t, os.IsNotExist(statErr), "aborted sink must commit NO shard file on the peer (not even a corrupt partial)")
}

// TestDriveActor_RemoteDestination proves the DriveActor routes a shard
// registered via registerRemotePut through a remoteSealedShardSink to the peer
// (instead of a local file), and the peer stores it verbatim + reads back to
// plaintext. This is the dormant S2-sender-b-1 routing seam.
func TestDriveActor_RemoteDestination(t *testing.T) {
	ctx := context.Background()
	keeper := testDEKKeeper(t)
	clusterID := testClusterID()

	tr1 := transport.MustNewHTTPTransport("test-cluster-psk")
	tr2 := transport.MustNewHTTPTransport("test-cluster-psk")
	require.NoError(t, tr1.Listen(ctx, "127.0.0.1:0"))
	require.NoError(t, tr2.Listen(ctx, "127.0.0.1:0"))
	defer tr1.Close()
	defer tr2.Close()
	require.NoError(t, tr1.Connect(ctx, tr2.LocalAddr()))

	recv := newSinkTestShardService(t, tr2, keeper, clusterID)
	tr2.RegisterShardWriteHandler(recv.NativeWriteHandler()) // native /shard/write route (Phase 8 N6)
	tr2.HandleBody(transport.StreamShardWriteBody, recv.HandleWriteBody())

	sealer := newSinkTestShardService(t, tr1, keeper, clusterID)
	plaintext := bytes.Repeat([]byte("drive remote shard "), 4096)
	sealed, err := sealer.EncodeEncryptedShardBuffer("bkt", "obj", 0, plaintext)
	require.NoError(t, err)

	in := make(chan EncryptedShardChunk, 4)
	commit := make(chan ShardWriteResult, 4)
	d := &DriveActor{
		in:        in,
		commitCh:  commit,
		pending:   make(map[uint64]*shardWriteState),
		transport: tr1,
	}
	d.registerRemotePut(ctx, nil, 1, "bkt", "obj", 0, tr2.LocalAddr())

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go d.Run(runCtx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: append([]byte(nil), sealed[:1000]...), LastInPut: false}
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: append([]byte(nil), sealed[1000:]...), LastInPut: true}

	select {
	case res := <-commit:
		require.NoError(t, res.Err, "remote shard write through DriveActor must succeed")
	case <-time.After(3 * time.Second):
		t.Fatal("no commit result for remote shard")
	}

	got, err := recv.ReadLocalShard("bkt", "obj", 0)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}
