package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

// errShutdownTimeout signals snapshot/close did not finish within the configured
// deadline. WAL is intact; recovery happens at next startup. Returned by
// shutdownLocalGroup.
var errShutdownTimeout = errors.New("group lifecycle: shutdown timed out")

// GroupLifecycleConfig collects the wiring needed to instantiate a local group.
type GroupLifecycleConfig struct {
	NodeID    string
	DataDir   string
	ShardSvc  *ShardService // may be nil for in-process / single-node tests
	EC        ECConfig
	LogStore  raft.LogStore // optional — if nil, BadgerLogStore is created at {groupDir}/raft
	Transport groupTransport
	AddrBook  NodeAddressBook
	// Raft tuning. Zero values use raft.DefaultConfig defaults.
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
}

// groupTransport is the optional wiring for raft RPCs. Tests with no peers can
// pass nil; production wires a transport that dispatches per-group RPCs over
// QUIC streams labeled with the group ID.
type groupTransport interface {
	RequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	AppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
}

type resolvingGroupTransport struct {
	inner    groupTransport
	addrBook NodeAddressBook
}

func (t resolvingGroupTransport) RequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	addr, err := t.resolve(peer)
	if err != nil {
		return nil, err
	}
	return t.inner.RequestVote(addr, args)
}

func (t resolvingGroupTransport) AppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	addr, err := t.resolve(peer)
	if err != nil {
		return nil, err
	}
	return t.inner.AppendEntries(addr, args)
}

func (t resolvingGroupTransport) resolve(peer string) (string, error) {
	if t.addrBook == nil {
		return peer, nil
	}
	addr, ok := ResolveNodeAddress(t.addrBook, peer)
	if !ok {
		return "", fmt.Errorf("group raft transport: resolve peer %q: node not found in address book", peer)
	}
	return addr, nil
}

// instantiateLocalGroup boots BadgerDB + raft.Node + GroupBackend for a group.
// Idempotent across restarts (re-uses existing data directory).
//
// Returns error on BadgerDB open failure or raft start failure. Caller (serve.go)
// reacts to error by terminating the process — voter failure is fatal.
func instantiateLocalGroup(cfg GroupLifecycleConfig, entry ShardGroupEntry) (*GroupBackend, error) {
	if entry.ID == "" {
		return nil, fmt.Errorf("instantiateLocalGroup: empty group ID")
	}
	if err := raft.ValidateGroupID(entry.ID); err != nil {
		return nil, fmt.Errorf("instantiateLocalGroup: %w", err)
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("instantiateLocalGroup: empty NodeID")
	}

	groupDir := filepath.Join(cfg.DataDir, "groups", entry.ID)
	if err := os.MkdirAll(filepath.Join(groupDir, "blobs"), 0o755); err != nil {
		return nil, fmt.Errorf("group %s: mkdir blobs: %w", entry.ID, err)
	}

	dbDir := filepath.Join(groupDir, "badger")
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, fmt.Errorf("group %s: mkdir badger: %w", entry.ID, err)
	}
	db, err := badger.Open(badger.DefaultOptions(dbDir).WithLogger(nil).WithNumCompactors(2))
	if err != nil {
		return nil, fmt.Errorf("group %s: open badger: %w", entry.ID, err)
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

	logStore := cfg.LogStore
	if logStore == nil {
		raftDir := filepath.Join(groupDir, "raft")
		ls, lerr := raft.NewBadgerLogStore(raftDir)
		if lerr != nil {
			_ = db.Close()
			return nil, fmt.Errorf("group %s: open raft log store: %w", entry.ID, lerr)
		}
		logStore = ls
	}

	node := raft.NewNode(rcfg, logStore)
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
	node.Start()

	// SetShardService requires self first in allNodes for correct self-skip in
	// WriteShard/ReadShard. PickVoters sorts alphabetically so we reorder here.
	peerIDsSelfFirst := make([]string, 0, len(entry.PeerIDs))
	peerIDsSelfFirst = append(peerIDsSelfFirst, cfg.NodeID)
	for _, p := range entry.PeerIDs {
		if p != cfg.NodeID {
			peerIDsSelfFirst = append(peerIDsSelfFirst, p)
		}
	}

	gb, err := NewGroupBackend(GroupBackendConfig{
		ID:       entry.ID,
		Root:     groupDir,
		DB:       db,
		Node:     node,
		LogStore: logStore,
		ShardSvc: cfg.ShardSvc,
		PeerIDs:  peerIDsSelfFirst,
		EC:       cfg.EC,
	})
	if err != nil {
		node.Close()
		_ = db.Close()
		return nil, fmt.Errorf("group %s: NewGroupBackend: %w", entry.ID, err)
	}
	return gb, nil
}

// groupCloser is the minimal interface shutdownLocalGroup needs. Both
// *GroupBackend and test-only slow wrappers satisfy it.
type groupCloser interface {
	ID() string
	Close() error
}

// shutdownLocalGroup snapshots and closes a group with a timeout. On timeout
// returns errShutdownTimeout — BadgerDB/raft finish in the background and WAL
// remains intact for next-startup recovery.
//
// ctx is reserved for future hooks (e.g., explicit snapshot before close); the
// timeout governs the actual close call.
func shutdownLocalGroup(ctx context.Context, gc groupCloser, timeout time.Duration) error {
	_ = ctx
	if gc == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() {
		done <- gc.Close()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errShutdownTimeout
	}
}

// InstantiateLocalGroup is the exported wrapper for instantiateLocalGroup.
// Used by cmd/grainfs/serve.go.
func InstantiateLocalGroup(cfg GroupLifecycleConfig, entry ShardGroupEntry) (*GroupBackend, error) {
	return instantiateLocalGroup(cfg, entry)
}

// ShutdownLocalGroup is the exported wrapper for shutdownLocalGroup.
// Used by cmd/grainfs/serve.go.
func ShutdownLocalGroup(ctx context.Context, gb *GroupBackend, timeout time.Duration) error {
	return shutdownLocalGroup(ctx, gb, timeout)
}
