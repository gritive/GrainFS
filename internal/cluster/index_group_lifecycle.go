package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// IndexGroupLifecycleConfig collects the wiring needed to instantiate a local
// object-index raft group. It mirrors GroupLifecycleConfig (group_lifecycle.go:22)
// but builds an indexGroup instead of a GroupBackend.
type IndexGroupLifecycleConfig struct {
	NodeID  string
	DataDir string
	// FSMStore is the per-node shared FSM-state DB. The data-group path threads
	// it into NewGroupBackend's BadgerDB-backed *FSM; the index group's *MetaFSM
	// is in-memory and persists only via raft snapshots under groupDir, so this
	// field is reserved for parity with GroupLifecycleConfig and validated
	// non-nil (mirroring instantiateLocalGroup's nil-check) but not otherwise
	// consumed here.
	FSMStore *badger.DB
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
// but returns an indexGroup (NOT started — the caller calls Start).
func instantiateLocalIndexGroup(cfg IndexGroupLifecycleConfig, entry IndexGroupEntry) (*indexGroup, error) {
	if entry.ID == "" {
		return nil, fmt.Errorf("instantiateLocalIndexGroup: empty group ID")
	}
	if err := raft.ValidateGroupID(entry.ID); err != nil {
		return nil, fmt.Errorf("instantiateLocalIndexGroup: %w", err)
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("instantiateLocalIndexGroup: empty NodeID")
	}
	if cfg.FSMStore == nil {
		return nil, fmt.Errorf("instantiateLocalIndexGroup: nil FSMStore")
	}

	groupDir := filepath.Join(cfg.DataDir, "groups", entry.ID)
	if err := os.MkdirAll(groupDir, 0o755); err != nil {
		return nil, fmt.Errorf("index group %s: mkdir: %w", entry.ID, err)
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

	// v2Close (raft-v2 BadgerDB close) is discarded here: indexGroup.Close()
	// closes only the node (Slice-4a design), and the solo tests own the store
	// close externally. The boot/shutdown-wiring slice must capture and close it
	// — until then this is the deferred close-ownership gap (see task report).
	node, _, err := newRaftNode(rcfg, groupDir)
	if err != nil {
		return nil, fmt.Errorf("index group %s: newRaftNode: %w", entry.ID, err)
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
	return g, nil
}

// InstantiateLocalIndexGroup is the exported wrapper for
// instantiateLocalIndexGroup. Slice 4b boot-wires the index groups through it
// (mirrors InstantiateLocalGroup).
func InstantiateLocalIndexGroup(cfg IndexGroupLifecycleConfig, entry IndexGroupEntry) (*indexGroup, error) {
	return instantiateLocalIndexGroup(cfg, entry)
}
