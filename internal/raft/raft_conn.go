// Package raft — raft_conn.go
//
// RaftConn is a multiplexed bidirectional QUIC connection used exclusively
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
// HoL avoidance: streams[N] (default N=4) round-robin. Pending map is at
// the conn level so responses can arrive on any stream.
package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
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

// RaftStream wraps one bidirectional QUIC stream within a RaftConn pool.
type RaftStream struct {
	parent *RaftConn
	stream *quic.Stream
	sendMu sync.Mutex
}

// RaftConn maintains a pool of long-lived QUIC streams to one peer for raft RPCs.
type RaftConn struct {
	conn       *quic.Conn
	peerAddr   string
	streams    []*RaftStream
	next       atomic.Uint64 // RR stream picker
	nextID     atomic.Uint64 // conn-level corrID generator
	pending    sync.Map      // corrID(uint64) -> chan callResult
	closed     atomic.Bool
	closeErr   atomic.Pointer[error]
	closeOnce  sync.Once
	closeChan  chan struct{}
	handlerSem chan struct{} // bounded handler pool

	// Handlers (set by owner before startReaders).
	rpcHandler     RPCHandler
	hbBatchHandler HeartbeatBatchHandler
	hbReplyHandler HeartbeatReplyHandler
	notifyHandler  NotifyHandler
	onBroken       func(*RaftConn, error) // invoked once when conn becomes broken
}

// RaftConnConfig configures a new RaftConn.
type RaftConnConfig struct {
	PoolSize        int // number of streams; default 4
	HandlerPoolSize int // bounded inbound handler workers; default 64
	RPCHandler      RPCHandler
	HBBatchHandler  HeartbeatBatchHandler
	HBReplyHandler  HeartbeatReplyHandler
	NotifyHandler   NotifyHandler
	OnBroken        func(*RaftConn, error)
}

// NewRaftConn wraps an established QUIC connection and prepares the stream
// pool. The caller must invoke OpenOutboundStreams (dialer side) or
// AcceptInboundStreams (acceptor side) before StartReaders.
func NewRaftConn(conn *quic.Conn, cfg RaftConnConfig) *RaftConn {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 4
	}
	if cfg.HandlerPoolSize <= 0 {
		cfg.HandlerPoolSize = 64
	}
	rc := &RaftConn{
		conn:           conn,
		peerAddr:       conn.RemoteAddr().String(),
		streams:        make([]*RaftStream, cfg.PoolSize),
		closeChan:      make(chan struct{}),
		handlerSem:     make(chan struct{}, cfg.HandlerPoolSize),
		rpcHandler:     cfg.RPCHandler,
		hbBatchHandler: cfg.HBBatchHandler,
		hbReplyHandler: cfg.HBReplyHandler,
		notifyHandler:  cfg.NotifyHandler,
		onBroken:       cfg.OnBroken,
	}
	for i := range rc.streams {
		rc.streams[i] = &RaftStream{parent: rc}
	}
	return rc
}

// OpenOutboundStreams opens N bidirectional streams as the dialer.
//
// quic-go behavior: a newly-opened stream is not visible to the peer's
// AcceptStream until the first byte is written. We send a one-byte
// opStreamInit frame so the peer's AcceptStream returns and the stream
// reader can begin draining frames. The init frame is consumed by the
// peer's reader and not surfaced to handlers.
func (rc *RaftConn) OpenOutboundStreams(ctx context.Context) error {
	for i, s := range rc.streams {
		stream, err := rc.conn.OpenStreamSync(ctx)
		if err != nil {
			rc.closeOpenedSoFar(i)
			return fmt.Errorf("open stream %d: %w", i, err)
		}
		s.stream = stream
		if err := writeInitFrame(stream); err != nil {
			rc.closeOpenedSoFar(i + 1)
			return fmt.Errorf("init stream %d: %w", i, err)
		}
	}
	return nil
}

// writeInitFrame sends a single opStreamInit frame so the peer's AcceptStream
// returns. corrID=0, payload empty.
func writeInitFrame(stream *quic.Stream) error {
	hdr := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], uint32(frameHeaderSize-4))
	hdr[4] = opStreamInit
	hdr[5] = 0
	binary.BigEndian.PutUint64(hdr[6:14], 0)
	_, err := stream.Write(hdr)
	return err
}

// AcceptInboundStreams accepts N bidirectional streams as the acceptor.
func (rc *RaftConn) AcceptInboundStreams(ctx context.Context) error {
	for i, s := range rc.streams {
		stream, err := rc.conn.AcceptStream(ctx)
		if err != nil {
			rc.closeOpenedSoFar(i)
			return fmt.Errorf("accept stream %d: %w", i, err)
		}
		s.stream = stream
	}
	return nil
}

func (rc *RaftConn) closeOpenedSoFar(n int) {
	for i := 0; i < n; i++ {
		if s := rc.streams[i].stream; s != nil {
			_ = s.Close()
		}
	}
}

// StartReaders launches one reader goroutine per stream. Must be called after
// OpenOutboundStreams or AcceptInboundStreams.
func (rc *RaftConn) StartReaders() {
	for _, s := range rc.streams {
		go s.readLoop()
	}
}

// pickStream returns a stream via round-robin.
func (rc *RaftConn) pickStream() *RaftStream {
	if len(rc.streams) == 1 {
		return rc.streams[0]
	}
	idx := rc.next.Add(1) % uint64(len(rc.streams))
	return rc.streams[idx]
}

// PeerAddr returns the remote address.
func (rc *RaftConn) PeerAddr() string { return rc.peerAddr }

// Closed reports whether the conn is closed.
func (rc *RaftConn) Closed() bool { return rc.closed.Load() }

// Call sends a Request frame and blocks until a Response/ResponseError arrives
// or ctx is cancelled. It returns the response payload bytes.
func (rc *RaftConn) Call(ctx context.Context, payload []byte) ([]byte, error) {
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

	if err := rc.sendFrame(rc.pickStream(), opRequest, id, payload); err != nil {
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
	return rc.sendFrame(rc.pickStream(), opNotify, 0, payload)
}

// SendHeartbeatBatch issues a heartbeat batch with a fresh corrID.
// Returns the corrID used so the caller can match the eventual reply.
func (rc *RaftConn) SendHeartbeatBatch(payload []byte) (uint64, error) {
	if rc.closed.Load() {
		return 0, ErrConnClosed
	}
	id := rc.nextID.Add(1)
	if err := rc.sendFrame(rc.pickStream(), opHeartbeatBatch, id, payload); err != nil {
		return 0, err
	}
	return id, nil
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

		// Best-effort: close streams + conn.
		for _, s := range rc.streams {
			if s.stream != nil {
				_ = s.stream.Close()
			}
		}
		if rc.conn != nil {
			_ = rc.conn.CloseWithError(0, err.Error())
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

// drainSeconds is a helper used in tests to ensure all readers wind down.
func (rc *RaftConn) drainSeconds(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && !rc.closed.Load() {
		time.Sleep(10 * time.Millisecond)
	}
}
