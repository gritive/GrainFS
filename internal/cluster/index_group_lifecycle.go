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
	// GroupMux, when set, is the per-server group-raft mux that production boot
	// uses to carry per-group raft RPCs (mirrors the data-group wiring at
	// boot_phases_storage_runtime.go:608/623). When non-nil it OVERRIDES Transport:
	// the outbound transport is GroupMux.ForGroup(entry.ID) and the node is
	// registered for inbound RPCs via GroupMux.Register(entry.ID, node). The 4a
	// unit tests leave it nil (they wire an in-process Transport or run solo), so
	// those paths stay byte-identical.
	GroupMux *raft.GroupRaftMux
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
	// Set to mirror instantiateLocalGroup (group_lifecycle.go:122), but NOTE:
	// ElectionPriorityKey is currently an INERT raft.Config field — it is defined
	// (types.go:284) and copied (raftfactory.go:48) but NEVER read by the raft node.
	// The election timeout RNG seeds on cfg.ID only (node.go:254), so per-group
	// leader staggering is NOT in effect; leader distribution across the N index
	// groups is probabilistic. This matters for 4b-2 perf (leaders could pile on one
	// node → degenerate all-forward topology), NOT for 4b-1 correctness (writes
	// route/replicate/read-back correctly regardless of which node leads a group).
	// Recorded as a 4b-2 BLOCKER in TODOS. Pre-existing repo-wide (data groups too).
	rcfg.ElectionPriorityKey = entry.ID

	// v2Close (raft-v2 BadgerDB close) is RETURNED so the IndexGroupManager can
	// close it on shutdown after closing the node — fixing the Slice-4a leak where
	// indexGroup.Close() closed only the node and the store was orphaned. The solo
	// tests close it directly.
	node, v2Close, err := newRaftNode(rcfg, groupDir)
	if err != nil {
		return nil, nil, fmt.Errorf("index group %s: newRaftNode: %w", entry.ID, err)
	}
	// Production boot (GroupMux set) carries per-group raft RPCs over the shared
	// group-raft mux, exactly like data groups: OUTBOUND via ForGroup(id) wired
	// here (the transport must be live before Start so elections can send), and
	// INBOUND via Register(id, node) which the manager performs AFTER Start (so a
	// STARTED node is registered, mirroring the data-group boot ordering at
	// boot_phases_storage_runtime.go:623). When GroupMux is nil the 4a paths apply
	// unchanged (explicit in-process Transport, else the no-transport stub).
	var tr groupTransport = cfg.Transport
	if cfg.GroupMux != nil {
		tr = cfg.GroupMux.ForGroup(entry.ID)
	}
	if tr != nil {
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
