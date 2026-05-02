package cluster

import (
	"fmt"
	"net"
)

// NodeAddressResolver resolves a cluster node identifier to its dialable QUIC
// address. MetaFSM implements this; tests can use fakes.
type NodeAddressResolver interface {
	ResolveNodeAddress(idOrAddr string) (string, bool)
}

// ResolveNodeAddress resolves nodeID to a dialable address while preserving
// backward compatibility for legacy shard-group PeerIDs that already stored
// raft addresses.
func ResolveNodeAddress(book NodeAddressBook, idOrAddr string) (string, bool) {
	if idOrAddr == "" {
		return "", false
	}
	if r, ok := book.(NodeAddressResolver); ok {
		if addr, ok := r.ResolveNodeAddress(idOrAddr); ok {
			return addr, true
		}
	}
	if book != nil {
		for _, n := range book.Nodes() {
			if n.ID == idOrAddr || n.Address == idOrAddr {
				if n.Address != "" {
					return n.Address, true
				}
			}
		}
	}
	if _, _, err := net.SplitHostPort(idOrAddr); err == nil {
		return idOrAddr, true
	}
	return "", false
}

// ResolveNodeAddresses resolves every peer in order. The returned slice is a
// fresh allocation even when every input is already an address.
func ResolveNodeAddresses(book NodeAddressBook, peers []string) ([]string, error) {
	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		addr, ok := ResolveNodeAddress(book, peer)
		if !ok {
			return nil, fmt.Errorf("node %q not found in address book", peer)
		}
		out = append(out, addr)
	}
	return out, nil
}

// ResolveNodeAddress resolves idOrAddr against the FSM's node map.
func (f *MetaFSM) ResolveNodeAddress(idOrAddr string) (string, bool) {
	if idOrAddr == "" {
		return "", false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, n := range f.nodes {
		if n.ID == idOrAddr || n.Address == idOrAddr {
			if n.Address != "" {
				return n.Address, true
			}
		}
	}
	if _, _, err := net.SplitHostPort(idOrAddr); err == nil {
		return idOrAddr, true
	}
	return "", false
}
