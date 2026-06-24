package transport

// StreamType is an INTERNAL admission/metrics key only (TrafficLimiter
// classes via ClassOf). It never travels on the wire: every RPC family rides
// its own native HTTP route, and the route tables in http_buffered_route.go /
// http_gossip_route.go map each route to its StreamType purely for inbound
// admission classing.
type StreamType byte

const (
	StreamControl             StreamType = 0x01 // Raft messages (votes, heartbeats, AppendEntries)
	StreamData                StreamType = 0x02 // Shard transfers (bulk data)
	StreamAdmin               StreamType = 0x03 // Cluster management, health checks
	StreamReceipt             StreamType = 0x04 // Heal-receipt rolling-window gossip (Phase 16 Slice 2, one-way)
	StreamReceiptQuery        StreamType = 0x05 // Heal-receipt broadcast-fallback RPC (Phase 16 Slice 2, request/response)
	StreamProposeForward      StreamType = 0x06 // Follower → leader ProposeForward RPC (consistent hash ring) — UNCHANGED for wire-compat
	StreamMetaRaft            StreamType = 0x07 // meta-Raft control-plane RPCs (membership, shard-map)
	StreamProposeGroupForward StreamType = 0x08 // Per-group object operation forwarding with groupID + op + args frame
	StreamGroupRaft           StreamType = 0x09 // Per-group Raft RPCs (RequestVote, AppendEntries), payload prefixed with [4B groupIDLen][groupID][raftRPC]
	StreamReadIndex           StreamType = 0x0A // Follower → leader ReadIndex RPC; response: [8B commitIndex big-endian][4B errLen][errBytes]
	StreamMetaProposeForward  StreamType = 0x0B // Follower → meta-Raft leader proposal forwarding
	// 0x0C retired.
	StreamGroupForwardBody StreamType = 0x0D // Per-group forwarded write metadata frame followed by raw request body bytes
	// 0x0E retired: was StreamMetaJoin (legacy KEK-challenge cluster-join admin RPC).
	StreamGroupForwardRead StreamType = 0x0F // Per-group forwarded read metadata reply followed by raw response body bytes
	StreamShardWriteBody   StreamType = 0x10 // ShardService write metadata frame followed by raw shard bytes
	StreamShardReadBody    StreamType = 0x11 // ShardService read metadata reply followed by raw shard bytes
	// 0x12 retired: was StreamCapabilityExchange (mux-era protocol version handshake).
	// 0x13 retired: was StreamAuditShip (follower → leader S3 audit event push).
	StreamDataGroupProposeForward StreamType = 0x14 // Follower → data-group leader metadata proposal forwarding
	StreamReadAppendSegment       StreamType = 0x15 // Non-owner → owner append-segment blob read (request frame + raw segment bytes reply)
	// 0x16 retired: was StreamMetaJoinChallenge (legacy KEK-challenge nonce request).
	StreamCapabilityProbe       StreamType = 0x17 // Peer → peer signed-assertion capability query (Task 1b)
	StreamKEKDiskSpaceProbe     StreamType = 0x18 // Leader → peer keystore-directory free-bytes probe (KEK rotation Task 5)
	StreamKEKLeaseSnapshotProbe StreamType = 0x19 // Leader → peer in-flight KEK lease count probe (KEK prune Task 8)
	StreamAppliedIndexProbe     StreamType = 0x1A // Leader → voter applied-index barrier probe (PR-2a §8b); req/resp magic-tagged binary
	// 0x1B retired: was StreamIndexGroupProposeForward (zero non-test uses).
	StreamMetaReadIndex StreamType = 0x1C // Follower → meta-Raft leader ReadIndex RPC; response: [8B commitIndex big-endian][4B errLen][errBytes]
)

type StreamClass byte

const (
	StreamClassControl StreamClass = iota
	StreamClassMeta
	StreamClassData
	StreamClassBulk
)

func ClassOf(st StreamType) StreamClass {
	switch st {
	case StreamMetaRaft, StreamMetaProposeForward, StreamReadIndex, StreamMetaReadIndex:
		return StreamClassMeta
	case StreamData, StreamProposeForward, StreamProposeGroupForward, StreamGroupRaft, StreamDataGroupProposeForward:
		return StreamClassData
	case StreamGroupForwardBody, StreamGroupForwardRead, StreamShardWriteBody, StreamShardReadBody, StreamReadAppendSegment:
		return StreamClassBulk
	default:
		return StreamClassControl
	}
}
