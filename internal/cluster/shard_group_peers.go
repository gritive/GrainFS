package cluster

import "net"

// ShardGroupPeerSet centralizes the identity rules for ShardGroupEntry.PeerIDs.
// New entries should store node IDs. Legacy/static entries may still store
// raft addresses, so callers pass local aliases until old metadata naturally
// ages out.
type ShardGroupPeerSet struct {
	peers []string
}

type ResolvedShardGroupPeer struct {
	Input      string
	NodeID     string
	RaftAddr   string
	Legacy     bool
	Unresolved bool
}

// NewShardGroupPeerSet returns a peer identity view for a shard group entry.
// The peer slice is not mutated.
func NewShardGroupPeerSet(entry ShardGroupEntry) ShardGroupPeerSet {
	return ShardGroupPeerSet{peers: entry.PeerIDs}
}

// MatchLocal returns the exact peer ID in this group that identifies the local
// process. localID should be the node ID; aliases are compatibility identifiers
// such as this process' raft address from legacy/static group entries.
func (s ShardGroupPeerSet) MatchLocal(localID string, aliases ...string) (string, bool) {
	for _, peer := range s.peers {
		if peer == localID {
			return peer, true
		}
	}
	for _, peer := range s.peers {
		for _, alias := range aliases {
			if alias != "" && peer == alias {
				return peer, true
			}
		}
	}
	return "", false
}

// ForwardOrder returns peer IDs with any local identity moved to the end.
func (s ShardGroupPeerSet) ForwardOrder(localID string, aliases ...string) []string {
	out := make([]string, 0, len(s.peers))
	local := make([]string, 0, 1)
	for _, peer := range s.peers {
		if peer == localID {
			local = append(local, peer)
			continue
		}
		matchedAlias := false
		for _, alias := range aliases {
			if alias != "" && peer == alias {
				matchedAlias = true
				break
			}
		}
		if matchedAlias {
			local = append(local, peer)
			continue
		}
		out = append(out, peer)
	}
	return append(out, local...)
}

func ResolveShardGroupPeers(book NodeAddressBook, entry ShardGroupEntry) []ResolvedShardGroupPeer {
	out := make([]ResolvedShardGroupPeer, 0, len(entry.PeerIDs))
	for _, peer := range entry.PeerIDs {
		out = append(out, ResolveShardGroupPeer(book, peer))
	}
	return out
}

func ResolveShardGroupPeer(book NodeAddressBook, peer string) ResolvedShardGroupPeer {
	resolved := ResolvedShardGroupPeer{Input: peer}
	if book == nil {
		resolved.NodeID = peer
		return resolved
	}
	for _, node := range book.Nodes() {
		if node.ID == peer {
			resolved.NodeID = node.ID
			resolved.RaftAddr = node.Address
			return resolved
		}
	}
	if nodeID, ok := ResolveNodeIDByAddress(book, peer); ok {
		resolved.NodeID = nodeID
		resolved.RaftAddr = peer
		resolved.Legacy = true
		return resolved
	}
	if _, _, err := net.SplitHostPort(peer); err == nil {
		resolved.RaftAddr = peer
		resolved.Legacy = true
		resolved.Unresolved = true
		return resolved
	}
	resolved.NodeID = peer
	return resolved
}
