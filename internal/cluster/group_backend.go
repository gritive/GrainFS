package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

// GroupBackend is the per-group data plane: BadgerDB + raft.Node + ShardService
// for one shard group. Each node instantiates one GroupBackend per group it is
// a voter of. The ClusterCoordinator (DistributedBackend in the legacy
// single-backend deployment) routes incoming PUT/GET to the right GroupBackend
// via DataGroupManager.
//
// GroupBackend embeds *DistributedBackend so all data-plane methods (PutObject,
// GetObject, CreateBucket, etc.) are reused. Cluster-wide concerns
// (BucketAssigner, Router wiring, LoadReporter) are NOT wired on GroupBackend
// instances — only on the coordinator.
type GroupBackend struct {
	*DistributedBackend
	groupID   string
	logStore  raft.LogStore // owned: closed on GroupBackend.Close (nil if wrapped)
	wrapped   bool          // true → Close is no-op (caller owns lifecycle)
	closed    atomic.Bool
	closeOnce sync.Once
}

// GroupBackendConfig wires GroupBackend dependencies. DB/Node/ShardSvc are
// owned by the GroupBackend after construction (Close releases DB and Node;
// ShardSvc is shared across groups and not closed here).
type GroupBackendConfig struct {
	ID       string
	Root     string
	DB       *badger.DB
	Node     *raft.Node
	LogStore raft.LogStore // optional — owned by GroupBackend (closed on Close)
	ShardSvc *ShardService // may be nil for in-process / single-node tests
	PeerIDs  []string      // EC node pool = group voter set
	EC       ECConfig
}

// WrapDistributedBackend wraps an EXISTING DistributedBackend as a GroupBackend.
// Used to register group-0 (legacy single-backend deployment) into
// DataGroupManager without doubly-opening a BadgerDB.
//
// The wrapped DistributedBackend's lifecycle is owned by the caller — Close()
// on this GroupBackend will NOT close the wrapped instance (logStore=nil,
// closeOnce protects against double-close on the embedded type).
func WrapDistributedBackend(groupID string, b *DistributedBackend) *GroupBackend {
	return &GroupBackend{
		DistributedBackend: b,
		groupID:            groupID,
		wrapped:            true,
	}
}

// NewGroupBackend creates a GroupBackend by wrapping a fresh DistributedBackend
// instance and wiring per-group dependencies (ShardService, EC config).
//
// The wrapped DistributedBackend has data-plane state (db, node, fsm, etc.)
// but cluster-wide setup (SetBucketAssigner, SetRouter) is NOT called —
// callers must use the cluster coordinator for those.
func NewGroupBackend(cfg GroupBackendConfig) (*GroupBackend, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("GroupBackend: empty ID")
	}
	if cfg.Root == "" || cfg.DB == nil || cfg.Node == nil {
		return nil, fmt.Errorf("GroupBackend %s: Root/DB/Node required", cfg.ID)
	}

	dist, err := NewDistributedBackend(cfg.Root, cfg.DB, cfg.Node)
	if err != nil {
		return nil, fmt.Errorf("GroupBackend %s: NewDistributedBackend: %w", cfg.ID, err)
	}

	if cfg.ShardSvc != nil {
		dist.SetShardService(cfg.ShardSvc, cfg.PeerIDs)
	}
	// EC config is per-group; activates only when len(PeerIDs) >= MinECNodes.
	dist.SetECConfig(cfg.EC)

	return &GroupBackend{
		DistributedBackend: dist,
		groupID:            cfg.ID,
		logStore:           cfg.LogStore,
	}, nil
}

// ID returns the group ID.
func (g *GroupBackend) ID() string { return g.groupID }

// RaftNode returns the underlying raft.Node — used by DataGroupPlanExecutor for
// AddVoter/RemoveVoter membership operations.
func (g *GroupBackend) RaftNode() *raft.Node { return g.node }

// Close shuts down BadgerDB and raft.Node. Idempotent — safe to call multiple
// times. The wrapped DistributedBackend.Close() handles BadgerDB; we close
// raft.Node ourselves before that.
func (g *GroupBackend) Close() error {
	var err error
	g.closeOnce.Do(func() {
		g.closed.Store(true)
		if g.wrapped {
			// Caller owns DB/Node lifecycle.
			return
		}
		if g.node != nil {
			g.node.Close()
		}
		err = g.DistributedBackend.Close() // closes meta BadgerDB
		if g.logStore != nil {
			if cErr := g.logStore.Close(); cErr != nil && err == nil {
				err = cErr
			}
		}
	})
	return err
}

// IsClosed reports whether Close has been called.
func (g *GroupBackend) IsClosed() bool { return g.closed.Load() }
