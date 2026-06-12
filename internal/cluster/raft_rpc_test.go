package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestRaftRPCTimeout_BelowElectionTimeout guards the load-bearing invariant that
// the per-RPC raft timeout stays below the data-raft election timeout: an
// in-flight heartbeat (AppendEntries) RPC must be able to complete before a
// follower's election timer fires, or the cluster suffers spurious elections.
//
// v2RaftRPCTimeout drives the RPCs to the data-raft node (serveruntime/run.go
// wires NewRaftRPCTransport with the v2 data node), whose election timer
// randomizes in [base, 2*base) with base = raft.DefaultElectionTimeout when
// --raft-election-timeout is unset — so DefaultElectionTimeout is the MINIMUM
// election timeout, the value the per-RPC timeout must stay under.
//
// This is a COMPILE-TIME guarantee over the constants. It does NOT prove runtime
// safety: a pathologically low --raft-election-timeout (below v2RaftRPCTimeout)
// would still violate the invariant; the boot check (--raft-election-timeout >=
// 3x --raft-heartbeat-interval) is the runtime backstop.
func TestRaftRPCTimeout_BelowElectionTimeout(t *testing.T) {
	require.Less(t, v2RaftRPCTimeout, raft.DefaultElectionTimeout,
		"raft per-RPC timeout (%s) must stay below the minimum election timeout (%s) so an in-flight heartbeat completes before a spurious election",
		v2RaftRPCTimeout, raft.DefaultElectionTimeout)
}
