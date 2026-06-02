// Package raft — raft_conn.go
//
// RaftConn is a multiplexed bidirectional connection used exclusively
// for per-group and meta raft RPCs. Unlike the per-message stream open/close
// pattern in internal/transport/quic.go, RaftConn maintains a small pool of
// long-lived streams and frames messages on them. Each frame carries a
// correlation ID; responses dispatch via a connection-level pending map.
//
// Wire format (per frame on a stream):
//
//	+----+--------+----+----+--------+---------+
//	| 4B | length | OP | EC | corrID | payload |
//	+----+--------+----+----+--------+---------+
//	  BE   uint32   1B   1B   8B BE     N bytes
//
// length = bytes following the length field (OP+EC+corrID+payload).
//
// OP codes: see opRequest / opResponse / opResponseError / opNotify /
// opHeartbeatBatch / opHeartbeatReplyBatch below.
//
// EC: reserved (currently 0).
//
// corrID: caller-allocated, conn-level unique. 0 means Notify (one-way).
//
// payload: raft RPC envelope (opaque bytes). Encoders/decoders are owned
// by the caller — RaftConn does not interpret payload contents.
//
// HoL avoidance: streams[N] (default N=4) split into a control lane (Call/
// Notify/heartbeat) and a bulk lane (CallBulk, entries-bearing AppendEntries),
// each round-robining its own stream subset; the zero-split default is a single
// shared pool over all streams (the original round-robin). Pending map is at the
// conn level so responses can arrive on any stream.
package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// Wire op codes.
const (
	opStreamInit          uint8 = 0x00 // sent once per stream after Open/Accept to make the stream visible to peer
	opRequest             uint8 = 0x01
	opResponse            uint8 = 0x02
	opResponseError       uint8 = 0x03
	opNotify              uint8 = 0x04
	opHeartbeatBatch      uint8 = 0x05
	opHeartbeatReplyBatch uint8 = 0x06
)

// MaxFrameSize bounds frame allocation. Validated before any io.ReadFull.
// 16 MB is enough for any practical raft RPC envelope (entries-bearing
// AppendEntries with batched commands). Snapshot install does not flow
// through RaftConn.
const MaxFrameSize = 16 * 1024 * 1024

const frameHeaderSize = 4 + 1 + 1 + 8 // length + OP + EC + corrID

// Errors.
var (
	ErrConnClosed         = errors.New("raft_conn: connection closed")
	ErrFrameTooBig        = errors.New("raft_conn: frame exceeds MaxFrameSize")
	ErrUnknownOp          = errors.New("raft_conn: unknown op code")
	ErrHandlerOverloaded  = errors.New("raft_conn: handler pool overloaded")
	ErrNoResponse         = errors.New("raft_conn: handler returned nil without error")
	ErrFrameTruncated     = errors.New("raft_conn: frame truncated")
	ErrInvalidFrameHeader = errors.New("raft_conn: invalid frame header")
)

// callResult is what Call goroutines block on. Either msg is set (success) or
// err is set (failure). Never both.
type callResult struct {
	payload []byte // raw response payload (caller decodes)
	err     error
}

// RPCHandler dispatches an inbound Request frame. It returns either a payload
// (success) or an error. Returning nil payload + nil error is treated as
// ErrNoResponse and surfaced to the caller as opResponseError.
type RPCHandler func(payload []byte) ([]byte, error)

// HeartbeatBatchHandler dispatches an inbound HeartbeatBatch frame. It returns
// the batch reply payload (encoded by caller). The reply is sent as
// opHeartbeatReplyBatch with the same corrID.
type HeartbeatBatchHandler func(payload []byte) []byte

// HeartbeatReplyHandler dispatches an inbound HeartbeatReplyBatch frame.
// The corrID was issued by the local coalescer; the handler maps it back
// to the per-group caller channels.
type HeartbeatReplyHandler func(corrID uint64, payload []byte)

// NotifyHandler dispatches an inbound Notify frame (one-way).
type NotifyHandler func(payload []byte)

// RaftStream wraps one bidirectional byte-stream carrier within a RaftConn pool.
// The carrier is an io.ReadWriteCloser — a *quic.Stream (QUIC mux driver) or a
// net.Conn (TCP control-plane driver, S2b). RaftConn does not know which.
type RaftStream struct {
	parent *RaftConn
	stream io.ReadWriteCloser
	sendMu sync.Mutex
}

// lanePool is a round-robin pool of streams for ONE lane (control or bulk).
// Splitting the streams into lanes keeps a large entries-bearing AppendEntries
// (bulk) from head-of-line blocking a heartbeat/vote (control) on the same
// stream. Lane selection is sender-local: the peer replies on the arrival
// stream and applies no lane policy, so no wire change or stream-index
// correspondence between dialer and acceptor is needed.
type lanePool struct {
	streams []*RaftStream
	next    atomic.Uint64
}

func (p *lanePool) pick() *RaftStream {
	if len(p.streams) == 1 {
		return p.streams[0]
	}
	idx := p.next.Add(1) % uint64(len(p.streams))
	return p.streams[idx]
}

// RaftConn maintains a pool of long-lived byte-streams to one peer for raft RPCs.
type RaftConn struct {
	peerAddr    string
	streams     []*RaftStream
	closeHook   func(error) error // tears down the underlying carrier on break (may be nil)
	controlLane *lanePool         // Call/Notify/heartbeat lane
	bulkLane    *lanePool         // CallBulk (entries-bearing AppendEntries) lane
	nextID      atomic.Uint64     // conn-level corrID generator
	pending     sync.Map          // corrID(uint64) -> chan callResult
	closed      atomic.Bool
	closeErr    atomic.Pointer[error]
	closeOnce   sync.Once
	closeChan   chan struct{}
	handlerSem  chan struct{} // bounded handler pool

	// Handlers (set by owner before startReaders).
	rpcHandler     RPCHandler
	hbBatchHandler HeartbeatBatchHandler
	hbReplyHandler HeartbeatReplyHandler
	notifyHandler  NotifyHandler
	onBroken       func(*RaftConn, error) // invoked once when conn becomes broken
}

// RaftConnConfig configures a new RaftConn.
type RaftConnConfig struct {
	HandlerPoolSize int // bounded inbound handler workers; default 64
	RPCHandler      RPCHandler
	HBBatchHandler  HeartbeatBatchHandler
	HBReplyHandler  HeartbeatReplyHandler
	NotifyHandler   NotifyHandler
	OnBroken        func(*RaftConn, error)
	// BulkLaneStreams dedicates the last N streams to the bulk lane (CallBulk);
	// the rest serve the control lane. 0 (or >= len(streams)) = a single shared
	// lane over all streams = the pre-S2c round-robin picker (neutral default;
	// QUIC keeps this). The TCP control-plane driver (S4) sets a split.
	BulkLaneStreams int
}

// NewRaftConn wraps a set of already-established byte-stream carriers (one per
// pool slot) for raft RPCs to peerAddr. The caller (carrier driver) opens or
// accepts the streams; closeHook tears down the underlying transport when the
// conn breaks (QUIC: conn.CloseWithError over the shared *quic.Conn; a single
// net.Conn carrier may pass nil and rely on per-stream Close). Call StartReaders
// after construction.
func NewRaftConn(peerAddr string, streams []io.ReadWriteCloser, closeHook func(error) error, cfg RaftConnConfig) *RaftConn {
	if cfg.HandlerPoolSize <= 0 {
		cfg.HandlerPoolSize = 64
	}
	rc := &RaftConn{
		peerAddr:       peerAddr,
		streams:        make([]*RaftStream, len(streams)),
		closeHook:      closeHook,
		closeChan:      make(chan struct{}),
		handlerSem:     make(chan struct{}, cfg.HandlerPoolSize),
		rpcHandler:     cfg.RPCHandler,
		hbBatchHandler: cfg.HBBatchHandler,
		hbReplyHandler: cfg.HBReplyHandler,
		notifyHandler:  cfg.NotifyHandler,
		onBroken:       cfg.OnBroken,
	}
	for i, s := range streams {
		rc.streams[i] = &RaftStream{parent: rc, stream: s}
	}
	// Lane assignment. k<=0 or k>=N → a single shared pool over all streams,
	// which is byte-identical to the pre-S2c single round-robin picker (the
	// QUIC-neutral default). Otherwise the last k streams form the bulk lane.
	if k := cfg.BulkLaneStreams; k <= 0 || k >= len(rc.streams) {
		shared := &lanePool{streams: rc.streams}
		rc.controlLane = shared
		rc.bulkLane = shared
	} else {
		rc.controlLane = &lanePool{streams: rc.streams[:len(rc.streams)-k]}
		rc.bulkLane = &lanePool{streams: rc.streams[len(rc.streams)-k:]}
	}
	return rc
}

// StartReaders launches one reader goroutine per stream. Must be called after
// construction (the carrier driver establishes the streams before NewRaftConn).
func (rc *RaftConn) StartReaders() {
	for _, s := range rc.streams {
		go s.readLoop()
	}
}

// PeerAddr returns the remote address.
func (rc *RaftConn) PeerAddr() string { return rc.peerAddr }

// Closed reports whether the conn is closed.
func (rc *RaftConn) Closed() bool { return rc.closed.Load() }

// Call sends a control-lane Request frame (RequestVote, small control RPCs) and
// blocks until a Response/ResponseError arrives or ctx is cancelled, returning
// the response payload. Entries-bearing AppendEntries must use CallBulk so a
// large transfer cannot head-of-line block a control frame on the same stream.
func (rc *RaftConn) Call(ctx context.Context, payload []byte) ([]byte, error) {
	return rc.call(ctx, rc.controlLane, payload)
}

// CallBulk is Call on the bulk lane: for entries-bearing AppendEntries (up to
// MaxFrameSize). On a single-lane RaftConn (BulkLaneStreams==0, the QUIC default)
// the bulk lane is the same shared pool as the control lane, so CallBulk behaves
// exactly as Call — the split is only active on the TCP control-plane driver.
func (rc *RaftConn) CallBulk(ctx context.Context, payload []byte) ([]byte, error) {
	return rc.call(ctx, rc.bulkLane, payload)
}

func (rc *RaftConn) call(ctx context.Context, lane *lanePool, payload []byte) ([]byte, error) {
	if rc.closed.Load() {
		if errPtr := rc.closeErr.Load(); errPtr != nil {
			return nil, *errPtr
		}
		return nil, ErrConnClosed
	}
	id := rc.nextID.Add(1)
	ch := make(chan callResult, 1)
	rc.pending.Store(id, ch)
	defer rc.pending.Delete(id)

	if err := rc.sendFrame(lane.pick(), opRequest, id, payload); err != nil {
		return nil, err
	}

	select {
	case res := <-ch:
		return res.payload, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rc.closeChan:
		if errPtr := rc.closeErr.Load(); errPtr != nil {
			return nil, *errPtr
		}
		return nil, ErrConnClosed
	}
}

// Notify sends a one-way Notify frame. corrID is 0.
func (rc *RaftConn) Notify(payload []byte) error {
	if rc.closed.Load() {
		return ErrConnClosed
	}
	return rc.sendFrame(rc.controlLane.pick(), opNotify, 0, payload)
}

// NextHeartbeatCorrID reserves a fresh corrID without sending. Pair with
// SendHeartbeatBatchWithCorrID. Splitting alloc from send lets the caller
// register the inflight batch BEFORE the frame can be answered (otherwise a
// fast receiver can reply before the lookup map has the entry).
func (rc *RaftConn) NextHeartbeatCorrID() uint64 {
	return rc.nextID.Add(1)
}

// SendHeartbeatBatchWithCorrID issues a heartbeat batch with the given corrID.
// The corrID must come from a prior NextHeartbeatCorrID call on this same
// RaftConn.
func (rc *RaftConn) SendHeartbeatBatchWithCorrID(corrID uint64, payload []byte) error {
	if rc.closed.Load() {
		return ErrConnClosed
	}
	return rc.sendFrame(rc.controlLane.pick(), opHeartbeatBatch, corrID, payload)
}

// sendFrame writes one frame on the given stream under sendMu.
func (rc *RaftConn) sendFrame(s *RaftStream, op uint8, corrID uint64, payload []byte) error {
	if uint64(len(payload))+uint64(frameHeaderSize-4) > uint64(MaxFrameSize) {
		return ErrFrameTooBig
	}
	hdr := make([]byte, frameHeaderSize)
	frameLen := uint32(frameHeaderSize - 4 + len(payload))
	binary.BigEndian.PutUint32(hdr[0:4], frameLen)
	hdr[4] = op
	hdr[5] = 0 // EC reserved
	binary.BigEndian.PutUint64(hdr[6:14], corrID)

	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if rc.closed.Load() {
		return ErrConnClosed
	}
	if _, err := s.stream.Write(hdr); err != nil {
		rc.markBroken(err)
		return err
	}
	if len(payload) > 0 {
		if _, err := s.stream.Write(payload); err != nil {
			rc.markBroken(err)
			return err
		}
	}
	return nil
}

// readLoop is one stream's reader goroutine. On any error or EOF it marks the
// conn broken (which fans out errors to all pending) and exits.
func (s *RaftStream) readLoop() {
	rc := s.parent
	hdr := make([]byte, frameHeaderSize)
	for {
		if _, err := io.ReadFull(s.stream, hdr); err != nil {
			rc.markBroken(err)
			return
		}
		frameLen := binary.BigEndian.Uint32(hdr[0:4])
		op := hdr[4]
		// hdr[5] = EC reserved
		corrID := binary.BigEndian.Uint64(hdr[6:14])

		bodyLen := int64(frameLen) - int64(frameHeaderSize-4)
		if bodyLen < 0 || bodyLen > MaxFrameSize {
			rc.markBroken(ErrFrameTooBig)
			return
		}
		var payload []byte
		if bodyLen > 0 {
			payload = make([]byte, bodyLen)
			if _, err := io.ReadFull(s.stream, payload); err != nil {
				rc.markBroken(err)
				return
			}
		}

		switch op {
		case opStreamInit:
			// no-op: peer signaling stream visibility
			continue
		case opRequest:
			rc.dispatchRequest(s, corrID, payload)
		case opResponse:
			rc.dispatchResponse(corrID, payload, nil)
		case opResponseError:
			rc.dispatchResponse(corrID, nil, fmt.Errorf("remote: %s", string(payload)))
		case opNotify:
			if rc.notifyHandler != nil {
				rc.notifyHandler(payload)
			}
		case opHeartbeatBatch:
			rc.dispatchHBBatch(s, corrID, payload)
		case opHeartbeatReplyBatch:
			if rc.hbReplyHandler != nil {
				rc.hbReplyHandler(corrID, payload)
			}
		default:
			rc.markBroken(ErrUnknownOp)
			return
		}
	}
}

// dispatchRequest runs the user-supplied RPCHandler under bounded handler pool.
func (rc *RaftConn) dispatchRequest(s *RaftStream, corrID uint64, payload []byte) {
	select {
	case rc.handlerSem <- struct{}{}:
	default:
		_ = rc.sendErrorFrame(s, corrID, ErrHandlerOverloaded)
		return
	}
	go func() {
		defer func() {
			<-rc.handlerSem
			if r := recover(); r != nil {
				_ = rc.sendErrorFrame(s, corrID, fmt.Errorf("handler panic: %v", r))
			}
		}()
		if rc.rpcHandler == nil {
			_ = rc.sendErrorFrame(s, corrID, ErrNoResponse)
			return
		}
		resp, err := rc.rpcHandler(payload)
		if err != nil {
			_ = rc.sendErrorFrame(s, corrID, err)
			return
		}
		if resp == nil {
			_ = rc.sendErrorFrame(s, corrID, ErrNoResponse)
			return
		}
		_ = rc.sendFrame(s, opResponse, corrID, resp)
	}()
}

func (rc *RaftConn) dispatchResponse(corrID uint64, payload []byte, err error) {
	v, ok := rc.pending.LoadAndDelete(corrID)
	if !ok {
		return // caller already gone or never existed
	}
	ch := v.(chan callResult)
	select {
	case ch <- callResult{payload: payload, err: err}:
	default:
		// buffered chan size 1, this should not happen, but drop safely
	}
}

func (rc *RaftConn) dispatchHBBatch(s *RaftStream, corrID uint64, payload []byte) {
	if rc.hbBatchHandler == nil {
		_ = rc.sendErrorFrame(s, corrID, ErrNoResponse)
		return
	}
	select {
	case rc.handlerSem <- struct{}{}:
	default:
		_ = rc.sendErrorFrame(s, corrID, ErrHandlerOverloaded)
		return
	}
	go func() {
		defer func() {
			<-rc.handlerSem
			if r := recover(); r != nil {
				_ = rc.sendErrorFrame(s, corrID, fmt.Errorf("hb-batch panic: %v", r))
			}
		}()
		reply := rc.hbBatchHandler(payload)
		if reply == nil {
			_ = rc.sendErrorFrame(s, corrID, ErrNoResponse)
			return
		}
		_ = rc.sendFrame(s, opHeartbeatReplyBatch, corrID, reply)
	}()
}

func (rc *RaftConn) sendErrorFrame(s *RaftStream, corrID uint64, err error) error {
	msg := err.Error()
	return rc.sendFrame(s, opResponseError, corrID, []byte(msg))
}

// markBroken transitions the conn to closed, fans error to all pending callers,
// and invokes onBroken. Idempotent.
func (rc *RaftConn) markBroken(err error) {
	rc.closeOnce.Do(func() {
		if err == nil {
			err = ErrConnClosed
		}
		rc.closeErr.Store(&err)
		rc.closed.Store(true)
		close(rc.closeChan)

		// Fan out error to all pending callers.
		rc.pending.Range(func(k, v any) bool {
			ch := v.(chan callResult)
			select {
			case ch <- callResult{err: err}:
			default:
			}
			rc.pending.Delete(k)
			return true
		})

		// Best-effort: close streams + tear down the underlying carrier.
		for _, s := range rc.streams {
			if s.stream != nil {
				_ = s.stream.Close()
			}
		}
		if rc.closeHook != nil {
			_ = rc.closeHook(err)
		}

		if rc.onBroken != nil {
			rc.onBroken(rc, err)
		}
	})
}

// Close closes the connection cleanly. Idempotent.
func (rc *RaftConn) Close() error {
	rc.markBroken(ErrConnClosed)
	return nil
}

// Wait blocks until the conn is broken or ctx is done.
func (rc *RaftConn) Wait(ctx context.Context) error {
	select {
	case <-rc.closeChan:
		if errPtr := rc.closeErr.Load(); errPtr != nil {
			return *errPtr
		}
		return ErrConnClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
