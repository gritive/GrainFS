package raft

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// S5 acceptance criterion 1 (deferred from S2c-1): lane assignment under a REAL
// split. The DRIVER classifies entries-bearing AppendEntries → ps.rc.CallBulk
// (bulk lane) and RequestVote → ps.rc.Call (control lane). On single-lane QUIC
// CallBulk==Call, so no existing test distinguishes the two; a mis-wire
// (group_transport_quic.go AppendEntries → ps.rc.Call) would only HoL-block under
// load. This test sets a real split (BulkLaneStreams>0) over identifiable streams
// and asserts at the SENDER which lane carried each RPC — and goes RED if the
// entries-AE is mis-classified onto the control lane.

// laneWriteRecorder records per-stream-index write counts across the tapped
// client streams handed to the RaftConn lanes.
type laneWriteRecorder struct {
	mu     sync.Mutex
	writes []int
}

func (r *laneWriteRecorder) add(i int) { r.mu.Lock(); r.writes[i]++; r.mu.Unlock() }
func (r *laneWriteRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.writes))
	copy(out, r.writes)
	return out
}

// tappedStream wraps a client stream and records that a frame was written to its
// lane (by stream index) so the test can observe lane routing at the sender.
type tappedStream struct {
	inner io.ReadWriteCloser
	idx   int
	rec   *laneWriteRecorder
}

func (s *tappedStream) Read(p []byte) (int, error)  { return s.inner.Read(p) }
func (s *tappedStream) Write(p []byte) (int, error) { s.rec.add(s.idx); return s.inner.Write(p) }
func (s *tappedStream) Close() error                { return s.inner.Close() }

// laneFakeCarrier hands out the pre-built tapped client streams in OpenStream
// order, mirroring how a real MuxCarrier yields the per-peer stream pool.
type laneFakeCarrier struct {
	mu      sync.Mutex
	streams []io.ReadWriteCloser
	idx     int
	addr    string
}

func (c *laneFakeCarrier) OpenStream(context.Context) (io.ReadWriteCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.idx >= len(c.streams) {
		return nil, fmt.Errorf("laneFakeCarrier: out of streams")
	}
	s := c.streams[c.idx]
	c.idx++
	return s, nil
}
func (c *laneFakeCarrier) AcceptStream(context.Context) (io.ReadWriteCloser, error) {
	return nil, fmt.Errorf("laneFakeCarrier: AcceptStream not used (dialer side)")
}
func (c *laneFakeCarrier) RemoteAddr() string { return c.addr }
func (c *laneFakeCarrier) Close(error) error  { return nil }

// laneFakeTransport is a muxDriverTransport whose GetOrConnectMux returns the
// fake carrier. The legacy Call path must NOT be hit in mux mode.
type laneFakeTransport struct{ carrier *laneFakeCarrier }

func (t *laneFakeTransport) Call(context.Context, string, *transport.Message) (*transport.Message, error) {
	return nil, fmt.Errorf("laneFakeTransport: legacy Call must not be used in mux mode")
}
func (t *laneFakeTransport) Handle(transport.StreamType, transport.StreamHandler) {}
func (t *laneFakeTransport) SetMuxConnHandler(transport.MuxConnHandler)           {}
func (t *laneFakeTransport) EvictMux(string, transport.MuxCarrier)                {}
func (t *laneFakeTransport) GetOrConnectMux(context.Context, string) (transport.MuxCarrier, error) {
	return t.carrier, nil
}

// laneRoutingReplyHandler is the server-side RPC handler: decode the group RPC and
// return a valid encoded reply so the synchronous sender call returns.
func laneRoutingReplyHandler(payload []byte) ([]byte, error) {
	_, inner, err := extractGroupID(payload)
	if err != nil {
		return nil, err
	}
	rpcType, _, err := decodeRPC(inner)
	if err != nil {
		return nil, err
	}
	switch rpcType {
	case rpcTypeAppendEntries:
		return encodeRPC(rpcTypeAppendEntriesReply, &AppendEntriesReply{Term: 1, Success: true})
	case rpcTypeRequestVote:
		return encodeRPC(rpcTypeRequestVoteReply, &RequestVoteReply{Term: 1, VoteGranted: true})
	default:
		return nil, fmt.Errorf("laneRoutingReplyHandler: unexpected rpc type %q", rpcType)
	}
}

func deltaOf(after, before []int) []int {
	d := make([]int, len(after))
	for i := range after {
		d[i] = after[i] - before[i]
	}
	return d
}

func TestGroupRaftSender_EntriesAEUsesBulkLane_VoteUsesControlLane(t *testing.T) {
	const n, k = 4, 1 // control = streams[0:3], bulk = streams[3:4]
	const peer = "peer:1"

	rec := &laneWriteRecorder{writes: make([]int, n)}
	cliStreams := make([]io.ReadWriteCloser, n)
	svStreams := make([]io.ReadWriteCloser, n)
	for i := 0; i < n; i++ {
		c, s := net.Pipe()
		cliStreams[i] = &tappedStream{inner: c, idx: i, rec: rec}
		svStreams[i] = s
	}
	t.Cleanup(func() {
		for i := 0; i < n; i++ {
			_ = cliStreams[i].Close()
			_ = svStreams[i].Close()
		}
	})

	// Server RaftConn over the raw server pipe sides (single lane is fine: the
	// receiver replies on the arrival stream and never picks a lane).
	server := NewRaftConn("server", svStreams, nil, RaftConnConfig{RPCHandler: laneRoutingReplyHandler})
	server.StartReaders()
	t.Cleanup(func() { _ = server.Close() })

	carrier := &laneFakeCarrier{streams: cliStreams, addr: peer}
	mux := NewGroupRaftMux(&laneFakeTransport{carrier: carrier})
	mux.EnableMux(n, 10*time.Millisecond)
	mux.SetMuxBulkLaneStreams(k) // MUST precede the first muxConnFor dial
	sender := mux.ForGroup("g1")

	// Warm-up: first RequestVote triggers the dial (openMuxStreams writes an init
	// frame on every stream) — exclude those writes from the measured deltas.
	_, err := sender.RequestVote(peer, &RequestVoteArgs{Term: 1, CandidateID: "cand"})
	require.NoError(t, err, "warm-up RequestVote")

	// Entries-bearing AppendEntries MUST land on the bulk lane (stream index 3).
	before := rec.snapshot()
	_, err = sender.AppendEntries(peer, &AppendEntriesArgs{
		Term:     1,
		LeaderID: "leader",
		Entries:  []LogEntry{{Term: 1, Index: 1, Command: []byte("payload")}},
	})
	require.NoError(t, err, "entries-bearing AppendEntries")
	aeDelta := deltaOf(rec.snapshot(), before)
	assert.Greaterf(t, aeDelta[n-1], 0, "entries-AE must write on the bulk lane (stream %d); deltas=%v", n-1, aeDelta)
	for i := 0; i < n-k; i++ {
		assert.Equalf(t, 0, aeDelta[i], "entries-AE must NOT write on control-lane stream %d; deltas=%v", i, aeDelta)
	}

	// RequestVote MUST land on the control lane, never the bulk lane.
	before = rec.snapshot()
	_, err = sender.RequestVote(peer, &RequestVoteArgs{Term: 1, CandidateID: "cand"})
	require.NoError(t, err, "RequestVote")
	rvDelta := deltaOf(rec.snapshot(), before)
	assert.Equalf(t, 0, rvDelta[n-1], "RequestVote must NOT write on the bulk lane; deltas=%v", rvDelta)
	ctrl := 0
	for i := 0; i < n-k; i++ {
		ctrl += rvDelta[i]
	}
	assert.Greaterf(t, ctrl, 0, "RequestVote must write on a control-lane stream; deltas=%v", rvDelta)
}
