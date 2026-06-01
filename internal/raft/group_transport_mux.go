// Package raft — group_transport_mux.go
//
// Mux-mode wiring for per-group raft RPCs. When mux mode is enabled,
// GroupRaftQUICMux dials a separate (mux-ALPN) QUIC connection per peer,
// wraps it in a RaftConn, and routes:
//
//   - Heartbeat AppendEntries (entries-empty) → HeartbeatCoalescer (batched)
//   - Entries-bearing AppendEntries           → RaftConn.Call (direct, single frame)
//   - RequestVote                              → RaftConn.Call (direct)
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
	"fmt"
	"io"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
)

// openQUICMuxStreams opens n bidirectional QUIC streams on conn (dialer side) and
// writes an opStreamInit frame on each so the peer's AcceptStream returns. Returns
// carrier-agnostic io.ReadWriteCloser handles for NewRaftConn. QUIC-specific: a
// freshly opened quic stream is invisible to the peer until the first byte.
func openQUICMuxStreams(ctx context.Context, conn *quic.Conn, n int) ([]io.ReadWriteCloser, error) {
	streams := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		s, err := conn.OpenStreamSync(ctx)
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

// acceptQUICMuxStreams accepts n bidirectional QUIC streams on conn (acceptor side).
func acceptQUICMuxStreams(ctx context.Context, conn *quic.Conn, n int) ([]io.ReadWriteCloser, error) {
	streams := make([]io.ReadWriteCloser, 0, n)
	for i := 0; i < n; i++ {
		s, err := conn.AcceptStream(ctx)
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

// EnableMux flips the GroupRaftQUICMux into mux mode. Must be called before
// any sender call returns through this mux. flushWindow is the heartbeat
// coalescer flush window; 0 falls back to DefaultCoalescerFlushWindow.
// poolSize is the per-peer RaftConn stream pool; 0 falls back to 4.
func (m *GroupRaftQUICMux) EnableMux(poolSize int, flushWindow time.Duration) {
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
func (m *GroupRaftQUICMux) MuxEnabled() bool { return m.muxEnabled.Load() }

// muxConnFor returns (or dials) a *muxPeerState for addr. Idempotent and
// goroutine-safe.
func (m *GroupRaftQUICMux) muxConnFor(ctx context.Context, addr string) (*muxPeerState, error) {
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
	conn, err := m.tr.GetOrConnectMux(ctx, addr)
	if err != nil {
		return nil, err
	}
	// Open the carrier streams before constructing the RaftConn. If this fails
	// there is no RaftConn yet (so no OnBroken can fire); just evict and return.
	streams, err := openQUICMuxStreams(ctx, conn, m.muxPoolSize)
	if err != nil {
		// Tear down the conn before evicting: EvictMux only deletes the cache
		// entry, so without this close the (now unreferenced) conn would leak.
		// Old flow closed it via rc.Close()->markBroken->CloseWithError; preserve that.
		_ = conn.CloseWithError(0, err.Error())
		m.tr.EvictMux(addr, conn)
		return nil, fmt.Errorf("open mux streams to %s: %w", addr, err)
	}

	ps := &muxPeerState{addr: addr, transport: m.tr, conn: conn}
	rc := NewRaftConn(conn.RemoteAddr().String(), streams, func(cause error) error {
		return conn.CloseWithError(0, cause.Error())
	}, RaftConnConfig{
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
			m.tr.EvictMux(addr, conn)
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
func (m *GroupRaftQUICMux) handleMuxRequest(payload []byte) ([]byte, error) {
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
func (m *GroupRaftQUICMux) lookupNode(groupID string) (RaftV2Handler, error) {
	if groupID == metaGroupID {
		if mn := m.metaNode.Load(); mn != nil {
			return mn, nil
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
func (m *GroupRaftQUICMux) dispatchToLocalGroup(groupID string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	node, err := m.lookupNode(groupID)
	if err != nil {
		return nil, err
	}
	return node.HandleAppendEntries(args), nil
}

// handleInboundMuxConn owns an accepted mux conn. It wraps the conn in a
// RaftConn, accepts the dialer's stream pool, and starts the reader loop.
// The conn lives until either side closes it.
func (m *GroupRaftQUICMux) handleInboundMuxConn(ctx context.Context, conn *quic.Conn) {
	streams, err := acceptQUICMuxStreams(ctx, conn, m.muxPoolSize)
	if err != nil {
		_ = conn.CloseWithError(0, "accept mux streams failed")
		return
	}
	rc := NewRaftConn(conn.RemoteAddr().String(), streams, func(cause error) error {
		return conn.CloseWithError(0, cause.Error())
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
	conn      *quic.Conn
	rc        *RaftConn
	hc        *HeartbeatCoalescer
	broken    atomic.Bool
}
