package raft

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
