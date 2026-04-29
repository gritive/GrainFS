package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gritive/GrainFS/internal/raft"
)

// MetaTransport abstracts RPC delivery for the meta-Raft group.
type MetaTransport interface {
	SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
}

// MetaRaftConfig configures a MetaRaft instance.
type MetaRaftConfig struct {
	NodeID    string
	Peers     []string // addresses of other meta-Raft peers
	DataDir   string   // directory for BadgerDB; meta store lives at DataDir/meta_raft
	Transport MetaTransport
}

// MetaRaft wraps a raft.Node dedicated to cluster control-plane operations.
type MetaRaft struct {
	node    *raft.Node
	store   *raft.BadgerLogStore
	snapMgr *raft.SnapshotManager
	fsm     *MetaFSM
	cfg     MetaRaftConfig
	cancel  context.CancelFunc
	done    chan struct{}

	applyMu     sync.Mutex
	applyCV     *sync.Cond
	lastApplied uint64
}

// NewMetaRaft constructs a MetaRaft from config. The node is not started yet;
// call Bootstrap then Start. Transport may be nil here and set later via
// SetTransport (needed when the QUIC transport requires the node handle first).
func NewMetaRaft(cfg MetaRaftConfig) (*MetaRaft, error) {
	storePath := cfg.DataDir + "/meta_raft"
	store, err := raft.NewBadgerLogStore(storePath)
	if err != nil {
		return nil, fmt.Errorf("meta_raft: open store: %w", err)
	}

	fsm := NewMetaFSM()
	snapMgr := raft.NewSnapshotManager(store, fsm, raft.SnapshotConfig{
		Threshold:    1024,
		TrailingLogs: 512,
	})

	nodeCfg := raft.DefaultConfig(cfg.NodeID, cfg.Peers)
	node := raft.NewNode(nodeCfg, store)

	m := &MetaRaft{
		node:    node,
		store:   store,
		snapMgr: snapMgr,
		fsm:     fsm,
		cfg:     cfg,
		done:    make(chan struct{}),
	}
	m.applyCV = sync.NewCond(&m.applyMu)

	if cfg.Transport != nil {
		m.wireTransport(cfg.Transport)
	}
	return m, nil
}

// Node returns the underlying raft.Node for external transport wiring.
func (m *MetaRaft) Node() *raft.Node { return m.node }

// SetTransport wires a MetaTransport into the Raft node after construction.
// Must be called before Start.
func (m *MetaRaft) SetTransport(t MetaTransport) {
	m.cfg.Transport = t
	m.wireTransport(t)
}

func (m *MetaRaft) wireTransport(t MetaTransport) {
	m.node.SetTransport(t.SendRequestVote, t.SendAppendEntries)
	m.node.SetInstallSnapshotTransport(t.SendInstallSnapshot)
}

// Bootstrap marks the store as bootstrapped. Idempotent.
func (m *MetaRaft) Bootstrap() error {
	err := m.node.Bootstrap()
	if err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
		return fmt.Errorf("meta_raft: bootstrap: %w", err)
	}
	return nil
}

// Start launches the Raft node and the FSM apply loop.
func (m *MetaRaft) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.node.Start()
	go m.runApplyLoop(ctx)
	return nil
}

// Close shuts down the node and waits for the apply loop to exit.
func (m *MetaRaft) Close() error {
	if m.cancel != nil {
		m.cancel()
	}
	m.node.Close()
	<-m.done
	return m.store.Close()
}

// Join adds node with id/addr as a learner then promotes it to voter,
// and records it in the FSM.
func (m *MetaRaft) Join(ctx context.Context, id, addr string) error {
	if err := m.node.AddLearner(id, addr); err != nil {
		return fmt.Errorf("meta_raft: AddLearner %s: %w", id, err)
	}
	if err := m.node.PromoteToVoter(id); err != nil {
		return fmt.Errorf("meta_raft: PromoteToVoter %s: %w", id, err)
	}
	return m.ProposeAddNode(ctx, MetaNodeEntry{ID: id, Address: addr, Role: 0})
}

// ProposeAddNode encodes an AddNode command and proposes it to the cluster,
// blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeAddNode(ctx context.Context, entry MetaNodeEntry) error {
	payload, err := encodeMetaAddNodeCmd(entry)
	if err != nil {
		return fmt.Errorf("meta_raft: encode AddNode: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeAddNode, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// waitApplied blocks until the FSM apply loop has processed the entry at idx.
func (m *MetaRaft) waitApplied(ctx context.Context, idx uint64) error {
	done := make(chan struct{})
	go func() {
		m.applyMu.Lock()
		for m.lastApplied < idx {
			m.applyCV.Wait()
		}
		m.applyMu.Unlock()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runApplyLoop reads committed entries from the node's apply channel and
// applies them to the FSM. Exits when ctx is cancelled.
func (m *MetaRaft) runApplyLoop(ctx context.Context) {
	defer close(m.done)
	applyCh := m.node.ApplyCh()
	for {
		select {
		case entry, ok := <-applyCh:
			if !ok {
				return
			}
			if entry.Type == raft.LogEntryCommand && len(entry.Command) > 0 {
				if err := m.fsm.applyCmd(entry.Command); err != nil {
					_ = err
				}
				m.snapMgr.MaybeTrigger(entry.Index, entry.Term)
			}
			m.applyMu.Lock()
			if entry.Index > m.lastApplied {
				m.lastApplied = entry.Index
			}
			m.applyCV.Broadcast()
			m.applyMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
