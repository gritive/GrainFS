package raft

import (
	"context"
	"fmt"
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
// RegisterMetaNode. The internal logic mirrors what v1 MetaRaftTransport's
// mux path did; it lives here (on GroupRaftMux) so both the v1 transport and
// the cluster-layer v2 transport share one implementation.
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
// the cluster-layer meta transport can make the same decision the v1 transport
// makes.
func IsMuxFallbackErr(err error) bool { return isMuxFallbackErr(err) }
