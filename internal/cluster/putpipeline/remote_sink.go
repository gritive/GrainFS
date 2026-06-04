package putpipeline

import (
	"context"
	"errors"
	"io"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// shardTransport is the subset of the cluster transport the remote shard sink
// needs: stream a request + body to a peer and return the response.
type shardTransport interface {
	CallWithBody(ctx context.Context, addr string, req *transport.Message, body io.Reader) (*transport.Message, error)
}

var errShardSinkAborted = errors.New("remote shard sink: aborted")

// Compile-time assertion that the remote sink satisfies the DriveActor's
// shardSink seam (the S2-sender-b wiring assigns it there for peer placement).
var _ shardSink = (*remoteSealedShardSink)(nil)

// remoteSealedShardSink streams one shard's already-sealed (GFSENC3) bytes to a
// peer's verbatim "WriteSealedShard" RPC. It satisfies shardSink: the DriveActor
// PUSHes ciphertext via Write while transport.CallWithBody PULLs from the paired
// io.Pipe in a background goroutine (Write blocks until the RPC reader consumes,
// which is the natural backpressure — at most one stripe is in flight).
//
// Sealing is done once at the source (CPUPool), so the bytes on the wire and on
// the peer's disk are identical ciphertext (the receiver stores them verbatim,
// never re-encrypting). Failure semantics honor the shardSink contract:
//   - Finalize closes the pipe cleanly (EOF), so the receiver reads the full
//     body and commits; it returns the RPC result. On RPC error the sink has
//     already closed the pipe and the goroutine has returned, so it is
//     self-cleaned — DriveActor will NOT call Abort after a Finalize error.
//   - Abort closes the pipe with an error, so the receiver's body Read returns a
//     NON-EOF error and rejects the write: no partial shard is committed.
//
// The caller MUST pass a ctx with a deadline (e.g. shardRPCTimeout): a dead peer
// that never reads would otherwise block Write forever; ctx cancellation makes
// CallWithBody return, which unblocks Write via the pipe-reader CloseWithError.
//
// INVARIANT (S2-sender-b wiring): shardKey here MUST equal the shardKey the
// CPUPool sealed the AAD with (ShardAADFields). Seal-at-source binds the AAD at
// the coordinator and this envelope carries the storage identity to the peer; if
// they drift the shard stores fine but is silently unreadable until GET. Derive
// both from one source.
type remoteSealedShardSink struct {
	pw   *io.PipeWriter
	done chan error
}

func newRemoteSealedShardSink(ctx context.Context, tr shardTransport, peerAddr, bucket, shardKey string, shardIdx int) *remoteSealedShardSink {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	req := cluster.BuildSealedShardWriteRequest(bucket, shardKey, shardIdx)
	go func() {
		resp, err := tr.CallWithBody(ctx, peerAddr, req, pr)
		if err != nil {
			// Unblock any Write still parked on the pipe: the reader is gone.
			_ = pr.CloseWithError(err)
			done <- err
			return
		}
		_ = pr.Close()
		done <- cluster.CheckShardWriteResponse(resp)
	}()
	return &remoteSealedShardSink{pw: pw, done: done}
}

func (s *remoteSealedShardSink) Write(p []byte) (int, error) { return s.pw.Write(p) }

func (s *remoteSealedShardSink) Finalize() error {
	_ = s.pw.Close() // clean EOF: the receiver reads the full body and commits
	return <-s.done
}

func (s *remoteSealedShardSink) Abort() {
	_ = s.pw.CloseWithError(errShardSinkAborted) // receiver body Read errors -> no commit
	<-s.done
}
