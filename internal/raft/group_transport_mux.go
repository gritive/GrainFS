// Package raft — group_transport_mux.go
//
// Mux-mode wiring for per-group raft RPCs. When mux mode is enabled,
// GroupRaftMux dials a separate (mux-ALPN) connection per peer,
// wraps it in a RaftConn, and routes:
//
//   - Heartbeat AppendEntries (entries-empty) → HeartbeatCoalescer (batched)
//   - Entries-bearing AppendEntries           → RaftConn.CallBulk (bulk lane, single frame)
//   - RequestVote                              → RaftConn.Call (control lane, direct)
//
// Wire payload INSIDE a mux frame (opRequest / opHeartbeatBatch / etc) is
// the same `[groupIDLen|groupID|rpcEnvelope(FB)]` shape used by the legacy
// path (prefixGroupID + encodeRPC). This lets the same encode/decode helpers
// serve both transports unchanged.
//
// Receiver side: SetMuxConnHandler is registered on transport. For each
// accepted mux conn, we wrap it in a RaftConn with handlers wired to dispatch
// opRequest → handleMuxRequest (vote/append) and opHeartbeatBatch →
// HandleBatchOnReceiver (which calls back into per-group nodes).
package raft

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
)

// openMuxStreams opens n bidirectional streams on the carrier (dialer side) and
// writes an opStreamInit frame on each so the peer's AcceptStream returns. Returns
// io.ReadWriteCloser handles for NewRaftConn. The init frame is a QUIC-stream-
// visibility concern (a freshly opened quic stream is invisible to the peer until
// the first byte) that is harmless on other carriers.
func openMuxStreams(ctx context.Context, carrier transport.MuxCarrier, n int) ([]io.ReadWriteCloser, error) {
	streams := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		s, err := carrier.OpenStream(ctx)
		if err != nil {
			closeStreams(streams)
			return nil, fmt.Errorf("open stream %d: %w", i, err)
		}
		if err := writeInitFrame(s); err != nil {
			_ = s.Close()
			closeStreams(streams)
			return nil, fmt.Errorf("init stream %d: %w", i, err)
		}
		streams = append(streams, s)
	}
	return streams, nil
}

// acceptMuxStreams accepts n bidirectional streams on the carrier (acceptor side).
func acceptMuxStreams(ctx context.Context, carrier transport.MuxCarrier, n int) ([]io.ReadWriteCloser, error) {
	streams := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		s, err := carrier.AcceptStream(ctx)
		if err != nil {
			closeStreams(streams)
			return nil, fmt.Errorf("accept stream %d: %w", i, err)
		}
		streams = append(streams, s)
	}
	return streams, nil
}

func closeStreams(streams []io.ReadWriteCloser) {
	for _, s := range streams {
		_ = s.Close()
	}
}

// writeInitFrame sends a single opStreamInit frame (corrID=0, empty payload) so
// the peer's AcceptStream returns. Moved here from raft_conn.go: it is a
// QUIC-stream-visibility concern, not part of the carrier-agnostic RaftConn.
func writeInitFrame(w io.Writer) error {
	hdr := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], uint32(frameHeaderSize-4))
	hdr[4] = opStreamInit
	hdr[5] = 0
	binary.BigEndian.PutUint64(hdr[6:14], 0)
	_, err := w.Write(hdr)
	return err
}

// EnableMux flips the GroupRaftMux into mux mode. Must be called before
// any sender call returns through this mux. flushWindow is the heartbeat
// coalescer flush window; 0 falls back to DefaultCoalescerFlushWindow.
// poolSize is the per-peer RaftConn stream pool; 0 falls back to 4.
func (m *GroupRaftMux) EnableMux(poolSize int, flushWindow time.Duration) {
	if poolSize <= 0 {
		poolSize = 4
	}
	m.muxPoolSize = poolSize
	m.muxFlushWindow = flushWindow
	m.muxEnabled.Store(true)

	// Register the receiver-side handler exactly once. Idempotent.
	m.muxRegisterOnce.Do(func() {
		m.tr.SetMuxConnHandler(m.handleInboundMuxConn)
	})
}

// MuxEnabled reports whether mux mode is active.
func (m *GroupRaftMux) MuxEnabled() bool { return m.muxEnabled.Load() }

// SetMuxBulkLaneStreams configures how many of the per-peer pool streams are
// dedicated to the bulk lane (CallBulk / entries-bearing AppendEntries) on the
// OUTBOUND RaftConn. 0 keeps the single-lane default; a positive value dedicates
// the last k streams to the bulk lane (TCP control-plane wiring, S4). Set before
// the first muxConnFor dial.
func (m *GroupRaftMux) SetMuxBulkLaneStreams(n int) { m.muxBulkLaneStreams = n }

// muxConnFor returns (or dials) a *muxPeerState for addr. Idempotent and
// goroutine-safe.
func (m *GroupRaftMux) muxConnFor(ctx context.Context, addr string) (*muxPeerState, error) {
	m.muxMu.RLock()
	if ps, ok := m.muxPeers[addr]; ok {
		m.muxMu.RUnlock()
		if ps.broken.Load() {
			// stale: re-dial below
		} else {
			return ps, nil
		}
	} else {
		m.muxMu.RUnlock()
	}

	// Race-safe creation: lock, double-check, dial outside lock, then store.
	carrier, err := m.tr.GetOrConnectMux(ctx, addr)
	if err != nil {
		return nil, err
	}
	// Open the carrier streams before constructing the RaftConn. If this fails
	// there is no RaftConn yet (so no OnBroken can fire); just evict and return.
	streams, err := openMuxStreams(ctx, carrier, m.muxPoolSize)
	if err != nil {
		// Tear down the carrier before evicting: EvictMux only deletes the cache
		// entry, so without this close the (now unreferenced) carrier would leak.
		// Old flow closed it via rc.Close()->markBroken->CloseWithError; preserve that.
		_ = carrier.Close(err)
		m.tr.EvictMux(addr, carrier)
		return nil, fmt.Errorf("open mux streams to %s: %w", addr, err)
	}

	ps := &muxPeerState{addr: addr, transport: m.tr, conn: carrier}
	rc := NewRaftConn(carrier.RemoteAddr(), streams, func(cause error) error {
		return carrier.Close(cause)
	}, RaftConnConfig{
		// Bulk-lane split for the OUTBOUND conn (0 = single-lane, set by S4 TCP
		// wiring). Inbound (handleInboundMuxConn) replies on the arrival stream and
		// never picks a lane, so it stays single-lane.
		BulkLaneStreams: m.muxBulkLaneStreams,
		RPCHandler: func(payload []byte) ([]byte, error) {
			return m.handleMuxRequest(payload)
		},
		HBBatchHandler: func(payload []byte) []byte {
			return HandleBatchOnReceiver(payload, m.dispatchToLocalGroup)
		},
		HBReplyHandler: func(corrID uint64, payload []byte) {
			if ps.hc != nil {
				ps.hc.DispatchReplyBatch(corrID, payload)
			}
		},
		OnBroken: func(_ *RaftConn, brokenErr error) {
			ps.broken.Store(true)
			if ps.hc != nil {
				ps.hc.FailAll(brokenErr)
			}
			m.tr.EvictMux(addr, carrier)
			m.muxMu.Lock()
			if cur := m.muxPeers[addr]; cur == ps {
				delete(m.muxPeers, addr)
			}
			m.muxMu.Unlock()
		},
	})
	// Set ps.rc + ps.hc BEFORE StartReaders so a reader that immediately breaks
	// (OnBroken) sees a valid coalescer.
	ps.rc = rc
	ps.hc = NewHeartbeatCoalescer(rc, m.muxFlushWindow)
	rc.StartReaders()

	m.muxMu.Lock()
	if existing, ok := m.muxPeers[addr]; ok && !existing.broken.Load() {
		// lost the race; close ours
		m.muxMu.Unlock()
		_ = rc.Close()
		return existing, nil
	}
	m.muxPeers[addr] = ps
	m.muxMu.Unlock()
	return ps, nil
}

// handleMuxRequest is registered as the inbound RaftConn.RPCHandler for
// non-batch messages. It decodes the same `[gidLen|gid|envelope]` payload as
// the legacy path and dispatches to the right local node + RPC type.
//
// The magic groupID metaGroupID ("__meta__") routes to the meta-raft node
// registered via RegisterMetaNode. Wire format is identical to a regular
// group call; the gid string itself is the discriminator. Senders use
// rpcType* (not metaRPC*) constants so the inner switch stays unified —
// the meta-raft Node has the same HandleRequestVote / HandleAppendEntries
// methods as a per-group Node.
func (m *GroupRaftMux) handleMuxRequest(payload []byte) ([]byte, error) {
	groupID, body, err := extractGroupID(payload)
	if err != nil {
		return nil, fmt.Errorf("mux: extract group id: %w", err)
	}
	node, err := m.lookupNode(groupID)
	if err != nil {
		return nil, err
	}

	rpcType, data, err := decodeRPC(body)
	if err != nil {
		return nil, fmt.Errorf("mux: decode rpc: %w", err)
	}
	switch rpcType {
	case rpcTypeRequestVote:
		args, err := decodeRequestVoteArgs(data)
		if err != nil {
			return nil, err
		}
		reply := node.HandleRequestVote(args)
		return encodeRPC(rpcTypeRequestVoteReply, reply)
	case rpcTypeAppendEntries:
		args, err := decodeAppendEntriesArgs(data)
		if err != nil {
			return nil, err
		}
		reply := node.HandleAppendEntries(args)
		return encodeRPC(rpcTypeAppendEntriesReply, reply)
	default:
		return nil, fmt.Errorf("mux: unsupported rpc %s", rpcType)
	}
}

// lookupNode resolves a groupID to its local RaftV2Handler. metaGroupID
// routes to the atomic meta-raft pointer; everything else hits the nodes
// sync.Map. The meta-raft node is v1-only on this mux — meta-raft v2 goes
// through the cluster-layer StreamControl bridge (PR 27) and does not appear
// here. Returns "mux: unknown group <id>" so senders can detect mixed-version
// peers via the unknown-group sentinel and fall back to the legacy
// StreamMetaRaft path (codex P1 #6).
func (m *GroupRaftMux) lookupNode(groupID string) (RaftV2Handler, error) {
	if groupID == metaGroupID {
		if box := m.metaNode.Load(); box != nil {
			return box.h, nil
		}
		return nil, fmt.Errorf("mux: unknown group %s", groupID)
	}
	v, ok := m.nodes.Load(groupID)
	if !ok {
		return nil, fmt.Errorf("mux: unknown group %s", groupID)
	}
	return v.(RaftV2Handler), nil
}

// dispatchToLocalGroup is invoked per-batch-item on the receiver side for
// coalesced heartbeat batches. It looks up the local raft Node (including
// metaGroupID -> metaNode, so meta heartbeats riding the shared coalescer
// route correctly) and invokes HandleAppendEntries directly. No envelope
// decode; args is already decoded by decodeHeartbeatBatch.
//
// codex P0 #1: this path is the SECOND place a __meta__ branch is required.
// handleMuxRequest covers direct calls; this covers coalesced heartbeats.
// Missing the branch here = silent meta heartbeat drop.
func (m *GroupRaftMux) dispatchToLocalGroup(groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	node, err := m.lookupNode(groupID)
	if err != nil {
		return nil, err
	}
	return node.HandleAppendEntries(args), nil
}

// handleInboundMuxConn owns an accepted mux conn. It wraps the conn in a
// RaftConn, accepts the dialer's stream pool, and starts the reader loop.
// The conn lives until either side closes it.
func (m *GroupRaftMux) handleInboundMuxConn(ctx context.Context, carrier transport.MuxCarrier) {
	streams, err := acceptMuxStreams(ctx, carrier, m.muxPoolSize)
	if err != nil {
		_ = carrier.Close(errors.New("accept mux streams failed"))
		return
	}
	rc := NewRaftConn(carrier.RemoteAddr(), streams, func(cause error) error {
		return carrier.Close(cause)
	}, RaftConnConfig{
		RPCHandler: func(payload []byte) ([]byte, error) {
			return m.handleMuxRequest(payload)
		},
		HBBatchHandler: func(payload []byte) []byte {
			return HandleBatchOnReceiver(payload, m.dispatchToLocalGroup)
		},
		// Inbound mux conn never receives reply batches (we never initiate
		// heartbeats from inbound side); reply path is for outbound coalescer.
	})
	rc.StartReaders()
	// Block until the conn breaks or the transport closes (ctx cancelled).
	_ = rc.Wait(ctx)
}

// muxPeerState is the per-peer outbound mux state.
type muxPeerState struct {
	addr      string
	transport muxDriverTransport
	conn      transport.MuxCarrier
	rc        *RaftConn
	hc        *HeartbeatCoalescer
	broken    atomic.Bool
}
