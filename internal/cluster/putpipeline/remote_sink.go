package putpipeline

import (
	"context"
	"errors"
	"io"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// shardTransport is the subset of the cluster transport the remote shard sink
// needs: stream a sealed shard write to a peer over the native route.
type shardTransport interface {
	ShardWrite(ctx context.Context, addr string, req transport.ShardWriteRequest, body io.Reader) error
}

var errShardSinkAborted = errors.New("remote shard sink: aborted")

// Compile-time assertion that the remote sink satisfies the DriveActor's
// shardSink seam (the S2-sender-b wiring assigns it there for peer placement).
var _ shardSink = (*remoteSealedShardSink)(nil)

// remoteSealedShardSink streams one shard's already-sealed (GFSENC3) bytes to a
// peer's verbatim sealed write over the native /shard/write route (sealed=1,
// Phase 8 N6). It satisfies shardSink: the DriveActor PUSHes ciphertext via
// Write while transport.ShardWrite PULLs from the paired io.Pipe in a
// background goroutine (Write blocks until the RPC reader consumes, which is
// the natural backpressure — at most one stripe is in flight).
//
// Sealing is done once at the source (CPUPool); the receiver stores the shard
// ciphertext verbatim, never re-encrypting. The wire body is that ciphertext
// followed by an 8-byte completeness trailer (the payload length) that Finalize
// appends and the receiver strips before storing — so wire = disk + trailer.
// Failure semantics honor the shardSink contract:
//   - Finalize writes the trailer then closes the pipe cleanly (EOF), so the
//     receiver reads the full body, verifies the declared length, and commits;
//     it returns the RPC result. On RPC error the sink has already closed the
//     pipe and the goroutine has returned, so it is self-cleaned — DriveActor
//     will NOT call Abort after a Finalize error.
//   - Abort closes the pipe with an error WITHOUT the trailer. Over HTTP the
//     receiver sees a clean (truncated) EOF, but the missing/short trailer makes
//     the declared length mismatch the bytes received, so it rejects the write:
//     no partial shard is committed.
//
// The caller MUST pass a ctx that cancels on a stall (e.g. an idle-deadline ctx;
// see idleTimeoutContext): a dead peer that never reads would otherwise block
// Write forever; ctx cancellation makes ShardWrite return, which unblocks Write
// via the pipe-reader CloseWithError. The sink reports downstream progress by
// calling reset() after each successful Write (the peer consumed that chunk), so
// the idle deadline only fires when the peer stops making progress, not on a
// slow-but-steady transfer.
//
// INVARIANT (S2-sender-b wiring): shardKey here MUST equal the shardKey the
// CPUPool sealed the AAD with (ShardAADFields). Seal-at-source binds the AAD at
// the coordinator and this envelope carries the storage identity to the peer; if
// they drift the shard stores fine but is silently unreadable until GET. Derive
// both from one source.
type remoteSealedShardSink struct {
	pw      *io.PipeWriter
	done    chan error
	reset   func() // progress signal for the idle deadline; nil if no deadline
	written int64  // payload bytes streamed so far, encoded into the Finalize trailer
}

// newRemoteSealedShardSink streams a sealed shard to peerAddr. reset (may be nil)
// is invoked after each successful Write to signal downstream progress to the
// ctx's idle deadline.
func newRemoteSealedShardSink(ctx context.Context, reset func(), tr shardTransport, peerAddr, bucket, shardKey string, shardIdx int) *remoteSealedShardSink {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	req := transport.ShardWriteRequest{Bucket: bucket, Key: shardKey, ShardIdx: shardIdx, Sealed: true}
	go func() {
		err := tr.ShardWrite(ctx, peerAddr, req, pr)
		if err != nil {
			// Unblock any Write still parked on the pipe: the reader is gone.
			_ = pr.CloseWithError(err)
			done <- err
			return
		}
		_ = pr.Close()
		done <- nil
	}()
	return &remoteSealedShardSink{pw: pw, done: done, reset: reset}
}

func (s *remoteSealedShardSink) Write(p []byte) (int, error) {
	n, err := s.pw.Write(p)
	s.written += int64(n)
	// A successful pipe write means the peer's CallWithBody reader consumed the
	// chunk — real downstream progress, so refresh the idle deadline.
	if err == nil && s.reset != nil {
		s.reset()
	}
	return n, err
}

func (s *remoteSealedShardSink) Finalize() error {
	// Write the completeness trailer (8-byte BE payload length) BEFORE the clean
	// EOF: a mid-stream abort surfaces to the receiver as a clean EOF over HTTP,
	// so the trailer is the only signal that distinguishes a complete shard from
	// a truncated one. Abort never writes it, so the receiver rejects the partial.
	trailer := cluster.AppendSealedShardTrailer(nil, s.written)
	if _, err := s.pw.Write(trailer); err != nil {
		_ = s.pw.CloseWithError(err) // reader gone; unblock the goroutine
		<-s.done
		return err
	}
	_ = s.pw.Close() // clean EOF: the receiver reads the full body and commits
	return <-s.done
}

func (s *remoteSealedShardSink) Abort() {
	_ = s.pw.CloseWithError(errShardSinkAborted) // receiver body Read errors -> no commit
	<-s.done
}
