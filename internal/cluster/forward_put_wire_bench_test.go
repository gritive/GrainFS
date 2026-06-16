package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// BenchmarkForwardPutObjectWire is the end-to-end forward benchmark used to pick
// minForwardStreamBytes. Unlike BenchmarkForwardBodyEncode (sender FlatBuffer
// buffering only), this runs a real forwarded PutObject over TWO real
// HTTPTransports into a real leader-backed ForwardReceiver, so it includes the
// Hertz HTTP request/response processing AND the receiver-side body handling:
//
//   - frame: body rides INSIDE the request FlatBuffer. cliTr.CallBuffered sends
//     it as one buffered request; Hertz buffers the whole metadata+body request,
//     ForwardReceiver.Handle → handlePutObject slices pa.BodyBytes() out of it.
//   - stream: body-less args frame + the body streamed after, via
//     cliTr.ForwardWrite; ForwardReceiver.HandleBody → handlePutObjectStream
//     reads the body from the HTTP stream.
//
// Both end at the same GroupBackend.PutObjectWithRequest, so the per-size DELTA
// between the two sub-benchmarks isolates the transport + encode + body-parsing
// cost that the streaming floor trades. Compare frame vs stream at each size to
// find where streaming stops paying off (the floor crossover).
func BenchmarkForwardPutObjectWire(b *testing.B) {
	gb := newTestGroupBackend(b, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	recvTr := transport.MustNewHTTPTransport("fwd-wire-bench-psk")
	cliTr := transport.MustNewHTTPTransport("fwd-wire-bench-psk")
	b.Cleanup(func() { recvTr.Close(); cliTr.Close() })
	if err := recvTr.Listen(context.Background(), "127.0.0.1:0"); err != nil {
		b.Fatalf("listen: %v", err)
	}
	addr := recvTr.LocalAddr()
	recvTr.RegisterBufferedRoute(transport.RouteForwardProposeGroup, rcv.Handle) // frame data-plane forward
	recvTr.RegisterForwardWriteHandler(rcv.HandleBody)                           // streamed body forward

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	putFrame := func(key string, body []byte) error {
		payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject,
			buildPutObjectArgs("bucket", key, "application/octet-stream", body))
		reply, err := cliTr.CallBuffered(ctx, addr, transport.RouteForwardProposeGroup, payload)
		if err != nil {
			return err
		}
		if st := raftpb.GetRootAsForwardReply(reply, 0).Status(); st != raftpb.ForwardStatusOK {
			return fmt.Errorf("frame status %v", st)
		}
		return nil
	}
	putStream := func(key string, body []byte) error {
		payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject,
			buildPutObjectArgs("bucket", key, "application/octet-stream", nil))
		reply, err := cliTr.ForwardWrite(ctx, addr, payload, bytes.NewReader(body))
		if err != nil {
			return err
		}
		if st := raftpb.GetRootAsForwardReply(reply, 0).Status(); st != raftpb.ForwardStatusOK {
			return fmt.Errorf("stream status %v", st)
		}
		return nil
	}

	// Readiness: Listen returns before Hertz accepts; poll until a real forward
	// succeeds (also proves both routes are wired before timing).
	deadline := time.Now().Add(15 * time.Second)
	for {
		fe := putFrame("warmup-frame", []byte("x"))
		se := putStream("warmup-stream", []byte("x"))
		if fe == nil && se == nil {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("forward routes not ready: frame=%v stream=%v", fe, se)
		}
		time.Sleep(25 * time.Millisecond)
	}

	for _, n := range []int{256 << 10, 512 << 10, 1 << 20, 2 << 20, 4 << 20, 5 << 20} {
		body := bytes.Repeat([]byte("x"), n)
		label := fmt.Sprintf("%dKiB", n>>10)

		b.Run("frame/"+label, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				if err := putFrame("k", body); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("stream/"+label, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(n))
			for i := 0; i < b.N; i++ {
				if err := putStream("k", body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	_ = io.Discard
}
