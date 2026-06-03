package raft

import (
	"context"
	"fmt"
	"strings"
)

// Meta-raft over the shared mux carrier.
//
// These exported methods let the cluster-layer meta-raft transport
// (cluster.RaftV2MetaTransport) send meta-raft RPCs over the SAME persistent
// per-peer mux carrier that group-raft uses, instead of the connection-per-RPC
// transport.Call path (which does a fresh TLS handshake per RPC — the cluster
// PUT throughput bottleneck). The wire path is identical to GroupRaftSender:
// payload is prefixed with the magic groupID metaGroupID ("__meta__"), the
// receiver's handleMuxRequest routes it to the node registered via
// RegisterMetaNode. This is the single meta-raft mux send path; it lives here
// (on GroupRaftMux) so the cluster-layer v2 transport reuses the group-raft
// carrier implementation. (Historically a v1 raft.MetaRaftTransport carried
// the same logic inline; it was the legacy per-RPC transport, removed once v2
// took over StreamMetaRaft.)
//
// Callers decide fallback via IsMuxFallbackErr: on a fallback-worthy error
// (dial failure, mux-disabled peer, closed conn, ctx budget) the caller retries
// on the legacy Call path with a fresh context; other errors propagate.

// SendMetaAppendEntries sends a meta-raft AppendEntries over the mux carrier.
// Empty-entries heartbeats ride the shared coalescer; entries-bearing AE goes
// direct on the bulk lane via RaftConn.CallBulk (same split as GroupRaftSender).
func (m *GroupRaftMux) SendMetaAppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	ps, err := m.muxConnFor(ctx, peer)
	if err != nil {
		return nil, err
	}
	if len(args.Entries) == 0 {
		reply, hcErr := ps.hc.AppendEntries(ctx, metaGroupID, args)
		if hcErr == nil {
			return reply, nil
		}
		// Coalescer error is rare; fall through to a direct bulk-lane call.
	}
	env, err := encodeRPC(rpcTypeAppendEntries, args)
	if err != nil {
		return nil, err
	}
	respBytes, err := ps.rc.CallBulk(ctx, prefixGroupID(metaGroupID, env))
	if err != nil {
		return nil, err
	}
	rpcType, data, err := decodeRPC(respBytes)
	if err != nil {
		return nil, fmt.Errorf("meta mux AppendEntries reply: %w", err)
	}
	// Defensive: refuse a cross-typed reply so a misbehaving peer cannot poison
	// meta-raft term state (mirrors the legacy-path check).
	if rpcType != rpcTypeAppendEntriesReply {
		return nil, fmt.Errorf("meta mux AppendEntries: unexpected reply type %s", rpcType)
	}
	return decodeAppendEntriesReply(data)
}

// SendMetaRequestVote sends a meta-raft RequestVote over the mux carrier
// (control lane via RaftConn.Call).
func (m *GroupRaftMux) SendMetaRequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	ps, err := m.muxConnFor(ctx, peer)
	if err != nil {
		return nil, err
	}
	env, err := encodeRPC(rpcTypeRequestVote, args)
	if err != nil {
		return nil, err
	}
	respBytes, err := ps.rc.Call(ctx, prefixGroupID(metaGroupID, env))
	if err != nil {
		return nil, err
	}
	rpcType, data, err := decodeRPC(respBytes)
	if err != nil {
		return nil, fmt.Errorf("meta mux RequestVote reply: %w", err)
	}
	if rpcType != rpcTypeRequestVoteReply {
		return nil, fmt.Errorf("meta mux RequestVote: unexpected reply type %s", rpcType)
	}
	return decodeRequestVoteReply(data)
}

// IsMuxFallbackErr reports whether a mux-path error should fall back to the
// legacy per-RPC Call path. Exported wrapper over the internal classifier so
// the cluster-layer meta transport classifies mux errors with the same rules
// the send path here uses.
func IsMuxFallbackErr(err error) bool { return isMuxFallbackErr(err) }

// isMuxFallbackErr decides whether a mux-path error should fall back to
// legacy StreamMetaRaft. Whitelist (fallback-worthy):
//   - ctx deadline exceeded — mux-attempt budget exhausted; legacy gets fresh
//     ctx (codex P1 #4)
//   - "mux: unknown group __meta__" remote — peer mux-enabled but on a binary
//     without RegisterMetaNode (codex P1 #6, mixed-version cluster)
//   - "raft_conn: connection closed" / "raft_conn: frame exceeds" — mux conn
//     itself is broken
//   - dial errors ("dial mux", "no mux handler") — peer doesn't speak mux ALPN
//
// Everything else propagates. Specifically:
//   - "raft_conn: handler pool overloaded" — peer is signalling backpressure;
//     legacy fallback would just amplify the load (codex P1 #3)
//   - "raft_conn: unknown op code" / "unsupported rpc" — protocol mismatch,
//     not recoverable by retrying on a different transport
//   - "mux: decode rpc" / "decode reply batch" — corruption, retry won't help
//   - "remote: handler panic" — peer-side bug; surface to caller
//
// Genuine RPC failures (term mismatch, etc.) are encoded inside the reply
// payload, not surfaced as transport errors, so they don't reach this gate.
func isMuxFallbackErr(err error) bool {
	if err == nil {
		return false
	}
	if errIsCtxBudget(err) {
		return true
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "mux: unknown group"):
		return true
	case strings.Contains(s, "raft_conn: connection closed"):
		return true
	case strings.Contains(s, "raft_conn: frame exceeds"):
		return true
	case strings.Contains(s, "no mux handler"):
		return true
	case strings.Contains(s, "dial mux"):
		return true
	case strings.Contains(s, "open mux streams"):
		return true
	case strings.Contains(s, "GetOrConnectMux"):
		return true
	}
	// Backpressure & protocol errors propagate so the caller sees the truth
	// (and so we don't double the load on an overloaded peer).
	return false
}

func errIsCtxBudget(err error) bool {
	return err == context.DeadlineExceeded || (err != nil && strings.Contains(err.Error(), "context deadline exceeded"))
}
