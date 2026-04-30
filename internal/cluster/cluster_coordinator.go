package cluster

import (
	"errors"

	"github.com/gritive/GrainFS/internal/storage"
)

// DefaultMaxForwardBodyBytes is the 5 MB hard cap on PutObject and UploadPart
// body size enforced by ClusterCoordinator before encoding the FBS args. The
// cap is the trade-off for the single-message wire model — chunked streaming
// would have removed it but at the cost of a transport refactor.
const DefaultMaxForwardBodyBytes = 5 * 1024 * 1024

// ErrCoordinatorNoRouter is returned when routeBucket is called on a
// coordinator that was constructed without a router (test/solo-node configs
// that should not be reaching the routing path).
var ErrCoordinatorNoRouter = errors.New("coordinator: router not configured")

// ClusterCoordinator implements storage.Backend by routing bucket-scoped ops
// to the per-group raft leader and delegating cluster-wide ops to the base
// (meta-FSM-backed) backend.
//
// Wiring (set in serve.go):
//   - base    : DistributedBackend (cluster-wide bucket ops via meta-FSM)
//   - groups  : DataGroupManager  (per-group GroupBackend lookup)
//   - router  : Router            (bucket → groupID, snapshot from meta-FSM)
//   - meta    : ShardGroupSource  (groupID → peer list, snapshot from meta-FSM)
//   - forward : ForwardSender     (0x08 wire dialer; nil disables forwarding)
//   - selfID  : this node's ID    (drives self-leader and self-voter checks)
type ClusterCoordinator struct {
	base    storage.Backend
	groups  *DataGroupManager
	router  *Router
	meta    ShardGroupSource
	forward *ForwardSender
	selfID  string

	maxBody int64
}

// NewClusterCoordinator constructs a coordinator with the 5 MB default body
// cap. groups/router/meta may be nil for tests that exercise only cluster-wide
// delegations; routeBucket returns ErrCoordinatorNoRouter when reached without
// a router.
func NewClusterCoordinator(
	base storage.Backend,
	groups *DataGroupManager,
	router *Router,
	meta ShardGroupSource,
	selfID string,
) *ClusterCoordinator {
	return &ClusterCoordinator{
		base:    base,
		groups:  groups,
		router:  router,
		meta:    meta,
		selfID:  selfID,
		maxBody: DefaultMaxForwardBodyBytes,
	}
}

// WithForwardSender attaches the QUIC dialer used to send 0x08 forward calls
// to peer nodes. Returns the receiver for builder-style chaining in serve.go.
func (c *ClusterCoordinator) WithForwardSender(s *ForwardSender) *ClusterCoordinator {
	c.forward = s
	return c
}

// --- Cluster-wide delegations (4 ops) ---
//
// These bypass routing entirely. CreateBucket and friends are always served by
// the meta-Raft (via base = DistributedBackend), keeping bucket-creation
// linearizable across the cluster regardless of which group later owns it.

func (c *ClusterCoordinator) CreateBucket(bucket string) error { return c.base.CreateBucket(bucket) }
func (c *ClusterCoordinator) HeadBucket(bucket string) error   { return c.base.HeadBucket(bucket) }
func (c *ClusterCoordinator) DeleteBucket(bucket string) error { return c.base.DeleteBucket(bucket) }
func (c *ClusterCoordinator) ListBuckets() ([]string, error)   { return c.base.ListBuckets() }

// --- Routing helper ---

// routeTarget captures everything an op needs to dispatch a bucket-scoped
// call: which group owns the bucket, peers in attempt order, and whether
// self can short-circuit the wire.
type routeTarget struct {
	groupID      string
	peers        []string
	selfIsLeader bool
	selfIsVoter  bool
}

// routeBucket resolves bucket → group → peer list for the bucket-scoped ops in
// T6/T7. Returns:
//   - ErrCoordinatorNoRouter if router is nil (config error)
//   - storage.ErrNoSuchBucket if no shard-group is assigned to the bucket
//   - ErrUnknownGroup if the assigned group is missing from meta-FSM
//
// selfIsLeader is true only when self is a voter AND the local GroupBackend's
// raft.Node currently holds leadership — used by op handlers to skip the wire
// and call the local backend directly (perf hint, not a correctness gate).
func (c *ClusterCoordinator) routeBucket(bucket string) (*routeTarget, error) {
	if c.router == nil {
		return nil, ErrCoordinatorNoRouter
	}
	dg, err := c.router.RouteKey(bucket, "")
	if err != nil || dg == nil {
		return nil, storage.ErrNoSuchBucket
	}
	if c.meta == nil {
		return nil, ErrUnknownGroup
	}
	entry, ok := c.meta.ShardGroup(dg.ID())
	if !ok || len(entry.PeerIDs) == 0 {
		return nil, ErrUnknownGroup
	}
	t := &routeTarget{
		groupID: entry.ID,
		peers:   PeersForForward(entry, c.selfID),
	}
	for _, p := range entry.PeerIDs {
		if p == c.selfID {
			t.selfIsVoter = true
			break
		}
	}
	if t.selfIsVoter && c.groups != nil {
		if dg2 := c.groups.Get(entry.ID); dg2 != nil && dg2.Backend() != nil &&
			dg2.Backend().RaftNode() != nil && dg2.Backend().RaftNode().IsLeader() {
			t.selfIsLeader = true
		}
	}
	return t, nil
}
