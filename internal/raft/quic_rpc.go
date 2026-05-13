package raft

import "time"

const (
	// raftRPCTimeout must be shorter than the minimum election timeout (150ms)
	// so heartbeat goroutines can reconnect before a follower starts a spurious
	// election.
	raftRPCTimeout      = 80 * time.Millisecond
	raftSnapshotTimeout = 60 * time.Second
)

const (
	rpcTypeRequestVote          = "RequestVote"
	rpcTypeRequestVoteReply     = "RequestVoteReply"
	rpcTypeAppendEntries        = "AppendEntries"
	rpcTypeAppendEntriesReply   = "AppendEntriesReply"
	rpcTypeInstallSnapshot      = "InstallSnapshot"
	rpcTypeInstallSnapshotReply = "InstallSnapshotReply"
	rpcTypeTimeoutNow           = "TimeoutNow"
	rpcTypeTimeoutNowReply      = "TimeoutNowReply"
)
