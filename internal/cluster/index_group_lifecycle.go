package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// IndexGroupLifecycleConfig collects the wiring needed to instantiate a local
// object-index raft group. It mirrors GroupLifecycleConfig (group_lifecycle.go:22)
// but builds an indexGroup instead of a GroupBackend.
//
// Unlike GroupLifecycleConfig there is no FSMStore here: the index group's
// *MetaFSM is in-memory and persists only via raft snapshots under groupDir, so
// the per-node shared FSM-state DB is never consumed (carrying it would be a
// footgun — a required-but-unused field). The durable state is the raft-v2 log +
// snapshots under <DataDir>/groups/<id>/, whose store is closed by the v2Close
// func instantiateLocalIndexGroup now returns.
type IndexGroupLifecycleConfig struct {
	NodeID   string
	DataDir  string
	KEKStore *encrypt.KEKStore
	// Forward is the optional leader-forward hook. nil ⇒ solo / leader-local
	// (the proposer proposes locally). A later slice wires the real forward.
	Forward   indexGroupForwardFunc
	Transport groupTransport
	AddrBook  NodeAddressBook
	// Raft tuning. Zero values use raft.DefaultConfig defaults.
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	// SnapshotInterval, when > 0, makes the index group's apply loop fire a
	// periodic snapshot every SnapshotInterval applied command entries so the
	// raft log is compacted under load. 0 (the default) disables it.
	SnapshotInterval uint64
}

// instantiateLocalIndexGroup boots a raft.Node + MetaFSM + indexGroup for one
// object-index group. It mirrors instantiateLocalGroup (group_lifecycle.go:80)
// but returns an indexGroup (NOT started — the caller calls Start) plus the
// v2Close func for the raft-v2 store so the manager can close it on shutdown.
func instantiateLocalIndexGroup(cfg IndexGroupLifecycleConfig, entry IndexGroupEntry) (*indexGroup, func() error, error) {
	if entry.ID == "" {
		return nil, nil, fmt.Errorf("instantiateLocalIndexGroup: empty group ID")
	}
	if err := raft.ValidateGroupID(entry.ID); err != nil {
		return nil, nil, fmt.Errorf("instantiateLocalIndexGroup: %w", err)
	}
	if cfg.NodeID == "" {
		return nil, nil, fmt.Errorf("instantiateLocalIndexGroup: empty NodeID")
	}

	groupDir := filepath.Join(cfg.DataDir, "groups", entry.ID)
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("index group %s: mkdir: %w", entry.ID, err)
	}

	// peers = all PeerIDs except self
	peers := make([]string, 0, len(entry.PeerIDs))
	for _, p := range entry.PeerIDs {
		if p != cfg.NodeID {
			peers = append(peers, p)
		}
	}

	rcfg := raft.DefaultConfig(cfg.NodeID, peers)
	if cfg.ElectionTimeout > 0 {
		rcfg.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.HeartbeatTimeout > 0 {
		rcfg.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	// LOAD-BEARING: keying election priority by the group ID staggers leader
	// preference across the N index groups so they don't all pile their leaders
	// onto one node — the whole point of sharding the object index. Do NOT drop
	// this when "mirroring" instantiateLocalGroup (group_lifecycle.go:122).
	rcfg.ElectionPriorityKey = entry.ID

	// v2Close (raft-v2 BadgerDB close) is RETURNED so the IndexGroupManager can
	// close it on shutdown after closing the node — fixing the Slice-4a leak where
	// indexGroup.Close() closed only the node and the store was orphaned. The solo
	// tests close it directly.
	node, v2Close, err := newRaftNode(rcfg, groupDir)
	if err != nil {
		return nil, nil, fmt.Errorf("index group %s: newRaftNode: %w", entry.ID, err)
	}
	if cfg.Transport != nil {
		tr := cfg.Transport
		if cfg.AddrBook != nil {
			tr = resolvingGroupTransport{inner: tr, addrBook: cfg.AddrBook}
		}
		node.SetTransport(tr.RequestVote, tr.AppendEntries)
	} else {
		// Single-node / in-process — RPCs always fail (no peers anyway).
		node.SetTransport(
			func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
				return nil, fmt.Errorf("no transport")
			},
			func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
				return nil, fmt.Errorf("no transport")
			},
		)
	}

	fsm := NewMetaFSM()
	fsm.SetKEKStore(cfg.KEKStore)

	// Not started: Start launches the node + apply loop. The caller owns Start
	// (and Close), mirroring the indexGroup unit tests.
	g := newIndexGroup(node, fsm, cfg.Forward)
	g.setSnapshotInterval(cfg.SnapshotInterval)
	return g, v2Close, nil
}
