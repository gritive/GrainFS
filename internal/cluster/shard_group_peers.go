package cluster

import (
	"bytes"
	"crypto/sha256"
	"net"
	"sort"
)

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

// AllMatchLocal reports whether every peer entry refers to the local process.
// Some single-node EC topologies intentionally repeat the local peer to satisfy
// shard-width calculations; those are still local-only for pwrite purposes.
func (s ShardGroupPeerSet) AllMatchLocal(localID string, aliases ...string) bool {
	if len(s.peers) == 0 {
		return false
	}
	for _, peer := range s.peers {
		if peer == localID {
			continue
		}
		matchedAlias := false
		for _, alias := range aliases {
			if alias != "" && peer == alias {
				matchedAlias = true
				break
			}
		}
		if !matchedAlias {
			return false
		}
	}
	return true
}

// OwnerPeer returns the deterministic single writer for group-scoped RMW
// operations. It uses rendezvous hashing over the group ID and peer identity,
// deduping repeated single-node placement slots before ranking them.
func (s ShardGroupPeerSet) OwnerPeer(groupID string) (string, bool) {
	if groupID == "" || len(s.peers) == 0 {
		return "", false
	}
	type ranked struct {
		peer  string
		score [32]byte
	}
	seen := make(map[string]struct{}, len(s.peers))
	items := make([]ranked, 0, len(s.peers))
	for _, peer := range s.peers {
		if peer == "" {
			continue
		}
		if _, ok := seen[peer]; ok {
			continue
		}
		seen[peer] = struct{}{}
		items = append(items, ranked{
			peer:  peer,
			score: sha256.Sum256([]byte(groupID + "/" + peer)),
		})
	}
	if len(items) == 0 {
		return "", false
	}
	sort.Slice(items, func(i, j int) bool {
		if cmp := bytes.Compare(items[i].score[:], items[j].score[:]); cmp != 0 {
			return cmp < 0
		}
		return items[i].peer < items[j].peer
	})
	return items[0].peer, true
}

// OwnerMatchesLocal reports whether the deterministic owner identifies this
// process. aliases cover legacy shard-group PeerIDs that stored raft addresses.
func (s ShardGroupPeerSet) OwnerMatchesLocal(groupID, localID string, aliases ...string) bool {
	owner, ok := s.OwnerPeer(groupID)
	if !ok {
		return false
	}
	if owner == localID {
		return true
	}
	for _, alias := range aliases {
		if alias != "" && owner == alias {
			return true
		}
	}
	return false
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
