package raft

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nStreams returns n in-memory paired streams (client + server sides) for lane
// construction/HoL tests that don't need a live transport.
func nStreams(n int) (cl, sv []io.ReadWriteCloser) {
	cl = make([]io.ReadWriteCloser, n)
	sv = make([]io.ReadWriteCloser, n)
	for i := 0; i < n; i++ {
		c, s := net.Pipe()
		cl[i], sv[i] = c, s
	}
	return cl, sv
}

func TestRaftConnLane_DefaultIsSinglePoolNeutral(t *testing.T) {
	cl, _ := nStreams(4)
	rc := NewRaftConn("peer", cl, nil, RaftConnConfig{}) // BulkLaneStreams == 0
	// Neutral default: control and bulk are the SAME pool over ALL streams, i.e.
	// identical to the pre-S2c single round-robin picker.
	require.Same(t, rc.controlLane, rc.bulkLane)
	assert.Len(t, rc.controlLane.streams, 4)
	_ = rc.Close()
}

func TestRaftConnLane_SplitIsDisjoint(t *testing.T) {
	cl, _ := nStreams(4)
	rc := NewRaftConn("peer", cl, nil, RaftConnConfig{BulkLaneStreams: 1})
	require.NotSame(t, rc.controlLane, rc.bulkLane)
	assert.Len(t, rc.controlLane.streams, 3) // first n-k
	assert.Len(t, rc.bulkLane.streams, 1)    // last k
	for _, b := range rc.bulkLane.streams {
		for _, c := range rc.controlLane.streams {
			assert.NotSame(t, b, c)
		}
	}
	_ = rc.Close()
}

func TestRaftConnLane_SplitClampsInvalid(t *testing.T) {
	cl, _ := nStreams(2)
	// k >= len(streams) is invalid → fall back to single shared pool (neutral).
	rc := NewRaftConn("peer", cl, nil, RaftConnConfig{BulkLaneStreams: 2})
	require.Same(t, rc.controlLane, rc.bulkLane)
	_ = rc.Close()
}

func TestRaftConnLane_BulkStallDoesNotBlockControl(t *testing.T) {
	// 2 streams: stream[0] = control, stream[1] = bulk (BulkLaneStreams=1). The
	// server reads+echoes ONLY the control stream; the bulk stream is never read,
	// so a CallBulk blocks in its sendFrame Write. A control Call must still
	// complete — proving the bulk stall cannot HoL-block the control lane.
	cliC, svC := net.Pipe()
	cliB, svB := net.Pipe()
	t.Cleanup(func() { cliC.Close(); svC.Close(); cliB.Close(); svB.Close() })

	rc := NewRaftConn("peer", []io.ReadWriteCloser{cliC, cliB}, nil, RaftConnConfig{BulkLaneStreams: 1})
	rc.StartReaders()
	t.Cleanup(func() { _ = rc.Close() })

	go serveEchoControlStream(svC)
	// svB is intentionally never read → any write to cliB blocks.

	// Hold the bulk lane: a CallBulk blocks on the unread bulk stream.
	bulkCtx, bulkCancel := context.WithCancel(context.Background())
	t.Cleanup(bulkCancel)
	bulkDone := make(chan struct{})
	go func() { _, _ = rc.CallBulk(bulkCtx, []byte("bulk-blocked")); close(bulkDone) }()

	// A control Call must return promptly despite the stalled bulk lane.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := rc.Call(ctx, []byte("ping"))
	require.NoError(t, err, "control Call HoL-blocked by stalled bulk lane")
	assert.Equal(t, "echo:ping", string(resp))

	// The bulk Call is still blocked (lane isolation held); cancel unblocks it.
	select {
	case <-bulkDone:
		t.Fatal("bulk Call returned unexpectedly — was it not actually stalled?")
	default:
	}
	bulkCancel()
}

// serveEchoControlStream reads opRequest frames off s and replies opResponse
// "echo:<payload>" with the same corrID. Minimal hand-rolled framing mirroring
// raft_conn.go's wire format.
func serveEchoControlStream(s io.ReadWriteCloser) {
	hdr := make([]byte, frameHeaderSize)
	for {
		if _, err := io.ReadFull(s, hdr); err != nil {
			return
		}
		frameLen := binary.BigEndian.Uint32(hdr[0:4])
		corrID := binary.BigEndian.Uint64(hdr[6:14])
		bodyLen := int(frameLen) - (frameHeaderSize - 4)
		body := make([]byte, bodyLen)
		if bodyLen > 0 {
			if _, err := io.ReadFull(s, body); err != nil {
				return
			}
		}
		reply := append([]byte("echo:"), body...)
		out := make([]byte, frameHeaderSize+len(reply))
		binary.BigEndian.PutUint32(out[0:4], uint32(frameHeaderSize-4+len(reply)))
		out[4] = opResponse
		out[5] = 0
		binary.BigEndian.PutUint64(out[6:14], corrID)
		copy(out[frameHeaderSize:], reply)
		if _, err := s.Write(out); err != nil {
			return
		}
	}
}
