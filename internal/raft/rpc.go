package raft

// Transport sends RPCs to peer nodes. Implementations are responsible for
// network details (QUIC, TCP, etc). The actor goroutine never calls Transport
// methods directly; outbound RPCs are sent by separate worker goroutines that
// the Transport implementation owns. PR 4 only defines the interface; the
// outbound side is wired in PR 5.
type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	// SendInstallSnapshot ships a snapshot blob to a peer whose nextIndex
	// has fallen below the leader's compaction floor (Raft §6.3 / §7).
	// PR 15 sends the entire snapshot in one call; chunked transmission is
	// out of scope.
	SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
	// SendTimeoutNow triggers an immediate election on the transfer target
	// (Raft §3.10). The leader calls this after selecting the most-caught-up
	// peer and steps down regardless of whether the RPC succeeds.
	SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error)
}
