package cluster

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/storage"
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
	groupID         string
	peerIDs         []string
	logStore        raft.LogStore               // owned: closed on GroupBackend.Close (nil if wrapped)
	vlogEntry       *resourcewatch.RegisteredDB // owned: deregistered on Close (nil if wrapped)
	wrapped         bool                        // true → Close is no-op (caller owns lifecycle)
	closed          atomic.Bool
	closeOnce       sync.Once
	testLeaderProbe raftLeaderProbe // test-only override for leaderProbe(); nil in production
}

// raftLeaderProbe is the minimal interface LocalExecution.ResolveWrite needs to
// consult leadership. Satisfied by *raft.Node in production and by fakes in
// unit tests.
type raftLeaderProbe interface {
	IsLeader() bool
}

// leaderProbe returns the IsLeader source for ResolveWrite. Exists to allow
// tests to inject a fake via testLeaderProbe; production callers should use
// RaftNode() directly.
func (g *GroupBackend) leaderProbe() raftLeaderProbe {
	if g.testLeaderProbe != nil {
		return g.testLeaderProbe
	}
	if g.node == nil {
		return nil
	}
	return g.node
}

// newGroupBackendWithRaftForTest is a test-only constructor returning a
// GroupBackend whose leaderProbe() reflects the provided fake. Production
// callers must use NewGroupBackend / WrapDistributedBackend.
func newGroupBackendWithRaftForTest(p raftLeaderProbe) *GroupBackend {
	return &GroupBackend{testLeaderProbe: p}
}

// GroupBackendConfig wires GroupBackend dependencies. DB/Node/ShardSvc are
// owned by the GroupBackend after construction (Close releases DB and Node;
// ShardSvc is shared across groups and not closed here).
type GroupBackendConfig struct {
	ID        string
	Root      string
	DB        *badger.DB
	Node      RaftNode
	LogStore  raft.LogStore               // optional — owned by GroupBackend (closed on Close)
	VlogEntry *resourcewatch.RegisteredDB // optional — owned by GroupBackend (deregistered on Close)
	ShardSvc  *ShardService               // may be nil for in-process / single-node tests
	PeerIDs   []string                    // EC node pool = group voter set
	EC        ECConfig
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

	// selfAddr must equal this node's raft ID so WriteShard/ReadShard self-skip
	// is correct. instantiateLocalGroup ensures cfg.PeerIDs[0] == cfg.NodeID.
	dist.selfAddr = cfg.Node.ID()
	if cfg.ShardSvc != nil {
		dist.SetShardService(cfg.ShardSvc, cfg.PeerIDs)
	}
	// EC config is per-group; invalid profiles fail fast against the group's peers.
	dist.SetECConfig(cfg.EC)
	// Bucket existence is trusted from the router; per-group DB has no bucket keys.
	dist.bypassBucketCheck = true

	return &GroupBackend{
		DistributedBackend: dist,
		groupID:            cfg.ID,
		peerIDs:            cloneStringSlice(cfg.PeerIDs),
		logStore:           cfg.LogStore,
		vlogEntry:          cfg.VlogEntry,
	}, nil
}

// ID returns the group ID.
func (g *GroupBackend) ID() string { return g.groupID }

func (g *GroupBackend) ShardGroup(id string) (ShardGroupEntry, bool) {
	if id != g.groupID || len(g.peerIDs) == 0 {
		return ShardGroupEntry{}, false
	}
	return ShardGroupEntry{ID: g.groupID, PeerIDs: cloneStringSlice(g.peerIDs)}, true
}

func (g *GroupBackend) placementContext(ctx context.Context) context.Context {
	if _, ok := PlacementGroupEntryFromContext(ctx); ok {
		return ctx
	}
	if _, ok := PlacementGroupFromContext(ctx); ok {
		return ctx
	}
	if g.shardSvc == nil {
		return ctx
	}
	if len(g.peerIDs) > 0 {
		return ContextWithPlacementGroupEntry(ctx, ShardGroupEntry{
			ID:      g.groupID,
			PeerIDs: g.peerIDs,
		})
	}
	return ContextWithPlacementGroup(ctx, g.groupID)
}

func (g *GroupBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return g.DistributedBackend.PutObject(g.placementContext(ctx), bucket, key, r, contentType)
}

func (g *GroupBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return g.DistributedBackend.CreateMultipartUpload(g.placementContext(ctx), bucket, key, contentType)
}

func (g *GroupBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return g.DistributedBackend.UploadPart(g.placementContext(ctx), bucket, key, uploadID, partNumber, r)
}

func (g *GroupBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return g.DistributedBackend.CompleteMultipartUpload(g.placementContext(ctx), bucket, key, uploadID, parts)
}

// Node returns the RaftNode interface for this group. Prefer this over
// RaftNode() for membership operations; it works for both v1 and v2.
func (g *GroupBackend) Node() RaftNode { return g.node }

// RaftNode returns the underlying *raft.Node via type assertion. Returns nil
// when the node is a v2 adapter (GRAINFS_RAFT_V2=cluster). Use Node() for
// membership operations; use RaftNode() only for v1-specific methods
// (JointSnapshotState, CompactLog, SetInstallSnapshotTransport, etc.).
func (g *GroupBackend) RaftNode() *raft.Node {
	v1, _ := g.node.(*raft.Node)
	return v1
}

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
		if g.vlogEntry != nil {
			resourcewatch.DeregisterDB(g.vlogEntry)
		}
	})
	return err
}

// IsClosed reports whether Close has been called.
func (g *GroupBackend) IsClosed() bool { return g.closed.Load() }
