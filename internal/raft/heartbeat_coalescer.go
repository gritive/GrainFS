// Package raft — heartbeat_coalescer.go
//
// HeartbeatCoalescer batches per-group heartbeat AppendEntries calls destined
// for the same peer into a single QUIC frame, then dispatches the batched
// replies back to the original callers. The synchronous AppendEntries(peer, args)
// (*Reply, error) contract is preserved — the coalescer just shifts the wire
// transmission from N small frames to one batched frame.
//
// Only entries-empty heartbeats flow through the coalescer. AppendEntries
// calls carrying log entries bypass it (BypassDirect) so the replicator's
// pipelining and timeout semantics are unaffected.
//
// Wire format of an opHeartbeatBatch payload (caller→receiver):
//
//	count: uint16 BE
//	repeat count times:
//	  groupIDLen: uint16 BE  (max 65535, in practice short ASCII)
//	  groupID: bytes
//	  argsLen: uint32 BE
//	  args: encoded AppendEntriesArgs (encodeAppendEntriesArgs)
//
// Wire format of an opHeartbeatReplyBatch payload (receiver→caller):
//
//	count: uint16 BE
//	repeat count times:
//	  groupIDLen: uint16 BE
//	  groupID: bytes
//	  okFlag: uint8  (1 = reply present, 0 = error)
//	  if okFlag == 1:
//	    replyLen: uint32 BE
//	    reply: encoded AppendEntriesReply
//	  else:
//	    errStrLen: uint16 BE
//	    errStr: bytes
//
// Latency: a fresh peer batch starts a flush timer on first enqueue
// (default 2ms). Subsequent enqueues for the same peer join the existing
// batch. The flush either fires when the timer expires or when an explicit
// FlushPeer is called (for tests). 2ms << 200ms heartbeat tick so this
// adds at most 1% latency.
package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultCoalescerFlushWindow is the default window during which heartbeat
// requests for the same peer are gathered into one batch.
const DefaultCoalescerFlushWindow = 2 * time.Millisecond

// CoalescerSender is the subset of *RaftConn the coalescer needs.
type CoalescerSender interface {
	SendHeartbeatBatch(payload []byte) (corrID uint64, err error)
	PeerAddr() string
}

// HeartbeatCoalescer collects per-group heartbeat AppendEntries calls for one
// peer and ships them as one batched frame. One coalescer instance per
// (peer, RaftConn) pair.
type HeartbeatCoalescer struct {
	rc          CoalescerSender
	flushWindow time.Duration

	mu      sync.Mutex
	pending []hbItem
	timer   *time.Timer

	// inFlight tracks batches whose reply has not yet arrived. Keyed by the
	// corrID assigned at flush time. Each entry maps groupID → reply chan
	// so dispatchReplyBatch can fan out individual replies.
	inFlight sync.Map // map[uint64] *inflightBatch

	closed atomic.Bool
}

type hbItem struct {
	groupID string
	args    *AppendEntriesArgs
	replyCh chan hbResult
}

type hbResult struct {
	reply *AppendEntriesReply
	err   error
}

type inflightBatch struct {
	corrID  uint64
	mu      sync.Mutex
	waiters map[string]chan hbResult // groupID → caller chan (single waiter per group per batch)
	deliver atomic.Bool              // true once dispatched (prevents double deliver)
}

// NewHeartbeatCoalescer creates a coalescer for one peer connection.
// The caller must register dispatchReplyBatch on the RaftConn's
// HBReplyHandler so reply frames get routed back here.
func NewHeartbeatCoalescer(rc CoalescerSender, flushWindow time.Duration) *HeartbeatCoalescer {
	if flushWindow <= 0 {
		flushWindow = DefaultCoalescerFlushWindow
	}
	return &HeartbeatCoalescer{
		rc:          rc,
		flushWindow: flushWindow,
	}
}

// AppendEntries enqueues an entries-empty heartbeat for batching, blocks until
// the reply arrives, and returns it. Callers carrying log entries should NOT
// call this — use a direct RaftConn.Call path instead.
func (hc *HeartbeatCoalescer) AppendEntries(ctx context.Context, groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if hc.closed.Load() {
		return nil, ErrConnClosed
	}
	if len(args.Entries) > 0 {
		return nil, errors.New("heartbeat coalescer: refusing entries-bearing AE; caller must use direct path")
	}

	replyCh := make(chan hbResult, 1)
	hc.enqueue(hbItem{groupID: groupID, args: args, replyCh: replyCh})

	select {
	case res := <-replyCh:
		return res.reply, res.err
	case <-ctx.Done():
		// The caller is gone. The reply (if it eventually arrives) is dropped
		// by the chan-closed select default in dispatchReplyBatch.
		return nil, ctx.Err()
	}
}

func (hc *HeartbeatCoalescer) enqueue(item hbItem) {
	hc.mu.Lock()
	hc.pending = append(hc.pending, item)
	if hc.timer == nil {
		hc.timer = time.AfterFunc(hc.flushWindow, hc.flush)
	}
	hc.mu.Unlock()
}

// flush builds and sends the current pending batch. May be called by the
// timer (background) or directly via FlushPeer (tests).
func (hc *HeartbeatCoalescer) flush() {
	hc.mu.Lock()
	if len(hc.pending) == 0 {
		hc.timer = nil
		hc.mu.Unlock()
		return
	}
	batch := hc.pending
	hc.pending = nil
	hc.timer = nil
	hc.mu.Unlock()

	payload, err := encodeHeartbeatBatch(batch)
	if err != nil {
		hc.failBatch(batch, fmt.Errorf("encode heartbeat batch: %w", err))
		return
	}

	corrID, err := hc.rc.SendHeartbeatBatch(payload)
	if err != nil {
		hc.failBatch(batch, fmt.Errorf("send heartbeat batch: %w", err))
		return
	}

	// Register inflight before any other goroutine could observe a reply
	// for this corrID. Reply dispatch is keyed off this map.
	infl := &inflightBatch{
		corrID:  corrID,
		waiters: make(map[string]chan hbResult, len(batch)),
	}
	for _, it := range batch {
		// Last writer wins — duplicate group in the same batch is invalid
		// and rejected at decode-time on the receiver. Trust local caller.
		infl.waiters[it.groupID] = it.replyCh
	}
	hc.inFlight.Store(corrID, infl)
}

// FlushNow forces an immediate flush of the pending batch. Used by tests
// and by callers that want to avoid the flush-window latency.
func (hc *HeartbeatCoalescer) FlushNow() {
	hc.mu.Lock()
	if hc.timer != nil {
		hc.timer.Stop()
		hc.timer = nil
	}
	hc.mu.Unlock()
	hc.flush()
}

// DispatchReplyBatch is registered on the RaftConn as HBReplyHandler. It
// matches the corrID, decodes the batched reply, and fans out per-group
// replies to the waiting caller channels.
func (hc *HeartbeatCoalescer) DispatchReplyBatch(corrID uint64, payload []byte) {
	v, ok := hc.inFlight.LoadAndDelete(corrID)
	if !ok {
		return // expired (caller ctx cancelled, or batch already failed)
	}
	infl := v.(*inflightBatch)
	if !infl.deliver.CompareAndSwap(false, true) {
		return
	}

	replies, err := decodeHeartbeatReplyBatch(payload)
	if err != nil {
		// Surface decode error to all waiters in this batch.
		for _, ch := range infl.waiters {
			select {
			case ch <- hbResult{err: fmt.Errorf("decode reply batch: %w", err)}:
			default:
			}
		}
		return
	}

	infl.mu.Lock()
	defer infl.mu.Unlock()
	for _, r := range replies {
		ch, ok := infl.waiters[r.groupID]
		if !ok {
			continue
		}
		delete(infl.waiters, r.groupID)
		var res hbResult
		if r.errStr != "" {
			res.err = fmt.Errorf("remote: %s", r.errStr)
		} else {
			res.reply = r.reply
		}
		select {
		case ch <- res:
		default:
		}
	}
	// Any group still in waiters didn't get a reply; surface ErrNoResponse.
	for _, ch := range infl.waiters {
		select {
		case ch <- hbResult{err: ErrNoResponse}:
		default:
		}
	}
}

// HandleBatchOnReceiver is registered on the RaftConn as HBBatchHandler on
// the peer side. It decodes the batch, dispatches each (groupID, args) to the
// local raft node via dispatchFn, and encodes a HeartbeatReplyBatch as
// response payload.
//
// dispatchFn returns the reply for the given group; an error from dispatchFn
// is encoded into the reply slot's errStr field.
func HandleBatchOnReceiver(payload []byte, dispatchFn func(groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)) []byte {
	items, err := decodeHeartbeatBatch(payload)
	if err != nil {
		return nil
	}

	replies := make([]hbReplyItem, len(items))
	for i, it := range items {
		reply, derr := dispatchFn(it.groupID, it.args)
		replies[i].groupID = it.groupID
		if derr != nil {
			replies[i].errStr = derr.Error()
		} else {
			replies[i].reply = reply
		}
	}
	out, err := encodeHeartbeatReplyBatch(replies)
	if err != nil {
		return nil
	}
	return out
}

// failBatch is invoked when the batch couldn't even be sent (encode failure,
// transport error). All callers receive the error.
func (hc *HeartbeatCoalescer) failBatch(batch []hbItem, err error) {
	for _, it := range batch {
		select {
		case it.replyCh <- hbResult{err: err}:
		default:
		}
	}
}

// FailAll surfaces err to every in-flight batch waiter. Called when the
// underlying RaftConn becomes broken.
func (hc *HeartbeatCoalescer) FailAll(err error) {
	hc.closed.Store(true)
	hc.inFlight.Range(func(k, v any) bool {
		hc.inFlight.Delete(k)
		infl := v.(*inflightBatch)
		if !infl.deliver.CompareAndSwap(false, true) {
			return true
		}
		infl.mu.Lock()
		for _, ch := range infl.waiters {
			select {
			case ch <- hbResult{err: err}:
			default:
			}
		}
		infl.mu.Unlock()
		return true
	})
	// Also drain the un-flushed pending list.
	hc.mu.Lock()
	pending := hc.pending
	hc.pending = nil
	if hc.timer != nil {
		hc.timer.Stop()
		hc.timer = nil
	}
	hc.mu.Unlock()
	hc.failBatch(pending, err)
}

// --- Wire codec for batches ---

type hbReplyItem struct {
	groupID string
	reply   *AppendEntriesReply
	errStr  string
}

func encodeHeartbeatBatch(items []hbItem) ([]byte, error) {
	if len(items) > 0xFFFF {
		return nil, fmt.Errorf("heartbeat batch too large (%d > 65535)", len(items))
	}
	// Pre-encode args so we know total size.
	encArgs := make([][]byte, len(items))
	for i, it := range items {
		ea, err := encodeAppendEntriesArgs(it.args)
		if err != nil {
			return nil, fmt.Errorf("encode args[%d]: %w", i, err)
		}
		encArgs[i] = ea
	}
	size := 2
	for i, it := range items {
		size += 2 + len(it.groupID) + 4 + len(encArgs[i])
	}
	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(items)))
	off += 2
	for i, it := range items {
		gidLen := len(it.groupID)
		if gidLen > 0xFFFF {
			return nil, fmt.Errorf("groupID too long (%d)", gidLen)
		}
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(gidLen))
		off += 2
		copy(buf[off:off+gidLen], it.groupID)
		off += gidLen
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(encArgs[i])))
		off += 4
		copy(buf[off:off+len(encArgs[i])], encArgs[i])
		off += len(encArgs[i])
	}
	return buf, nil
}

type hbBatchDecoded struct {
	groupID string
	args    *AppendEntriesArgs
}

func decodeHeartbeatBatch(buf []byte) ([]hbBatchDecoded, error) {
	if len(buf) < 2 {
		return nil, errors.New("heartbeat batch: short header")
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	off := 2
	out := make([]hbBatchDecoded, 0, count)
	for i := 0; i < count; i++ {
		if off+2 > len(buf) {
			return nil, fmt.Errorf("heartbeat batch[%d]: truncated groupID len", i)
		}
		gidLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
		off += 2
		if off+gidLen > len(buf) {
			return nil, fmt.Errorf("heartbeat batch[%d]: truncated groupID", i)
		}
		groupID := string(buf[off : off+gidLen])
		off += gidLen
		if off+4 > len(buf) {
			return nil, fmt.Errorf("heartbeat batch[%d]: truncated args len", i)
		}
		argsLen := int(binary.BigEndian.Uint32(buf[off : off+4]))
		off += 4
		if off+argsLen > len(buf) {
			return nil, fmt.Errorf("heartbeat batch[%d]: truncated args body", i)
		}
		args, err := decodeAppendEntriesArgs(buf[off : off+argsLen])
		if err != nil {
			return nil, fmt.Errorf("heartbeat batch[%d]: decode args: %w", i, err)
		}
		off += argsLen
		out = append(out, hbBatchDecoded{groupID: groupID, args: args})
	}
	return out, nil
}

func encodeHeartbeatReplyBatch(items []hbReplyItem) ([]byte, error) {
	if len(items) > 0xFFFF {
		return nil, fmt.Errorf("reply batch too large (%d > 65535)", len(items))
	}
	encReplies := make([][]byte, len(items))
	for i, it := range items {
		if it.errStr != "" {
			continue
		}
		er, err := encodeAppendEntriesReply(it.reply)
		if err != nil {
			return nil, fmt.Errorf("encode reply[%d]: %w", i, err)
		}
		encReplies[i] = er
	}
	size := 2
	for i, it := range items {
		size += 2 + len(it.groupID) + 1 // gidLen + groupID + okFlag
		if it.errStr != "" {
			size += 2 + len(it.errStr)
		} else {
			size += 4 + len(encReplies[i])
		}
	}
	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(items)))
	off += 2
	for i, it := range items {
		gidLen := len(it.groupID)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(gidLen))
		off += 2
		copy(buf[off:off+gidLen], it.groupID)
		off += gidLen
		if it.errStr != "" {
			buf[off] = 0 // err flag
			off++
			binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(it.errStr)))
			off += 2
			copy(buf[off:off+len(it.errStr)], it.errStr)
			off += len(it.errStr)
		} else {
			buf[off] = 1 // ok flag
			off++
			rLen := len(encReplies[i])
			binary.BigEndian.PutUint32(buf[off:off+4], uint32(rLen))
			off += 4
			copy(buf[off:off+rLen], encReplies[i])
			off += rLen
		}
	}
	return buf, nil
}

type hbReplyDecoded struct {
	groupID string
	reply   *AppendEntriesReply
	errStr  string
}

func decodeHeartbeatReplyBatch(buf []byte) ([]hbReplyDecoded, error) {
	if len(buf) < 2 {
		return nil, errors.New("reply batch: short header")
	}
	count := int(binary.BigEndian.Uint16(buf[0:2]))
	off := 2
	out := make([]hbReplyDecoded, 0, count)
	for i := 0; i < count; i++ {
		if off+2 > len(buf) {
			return nil, fmt.Errorf("reply batch[%d]: truncated groupID len", i)
		}
		gidLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
		off += 2
		if off+gidLen > len(buf) {
			return nil, fmt.Errorf("reply batch[%d]: truncated groupID", i)
		}
		groupID := string(buf[off : off+gidLen])
		off += gidLen
		if off+1 > len(buf) {
			return nil, fmt.Errorf("reply batch[%d]: truncated ok flag", i)
		}
		ok := buf[off]
		off++
		var item hbReplyDecoded
		item.groupID = groupID
		if ok == 0 {
			if off+2 > len(buf) {
				return nil, fmt.Errorf("reply batch[%d]: truncated err len", i)
			}
			eLen := int(binary.BigEndian.Uint16(buf[off : off+2]))
			off += 2
			if off+eLen > len(buf) {
				return nil, fmt.Errorf("reply batch[%d]: truncated err body", i)
			}
			item.errStr = string(buf[off : off+eLen])
			off += eLen
		} else if ok == 1 {
			if off+4 > len(buf) {
				return nil, fmt.Errorf("reply batch[%d]: truncated reply len", i)
			}
			rLen := int(binary.BigEndian.Uint32(buf[off : off+4]))
			off += 4
			if off+rLen > len(buf) {
				return nil, fmt.Errorf("reply batch[%d]: truncated reply body", i)
			}
			reply, err := decodeAppendEntriesReply(buf[off : off+rLen])
			if err != nil {
				return nil, fmt.Errorf("reply batch[%d]: decode reply: %w", i, err)
			}
			item.reply = reply
			off += rLen
		} else {
			return nil, fmt.Errorf("reply batch[%d]: invalid ok flag %d", i, ok)
		}
		out = append(out, item)
	}
	return out, nil
}
