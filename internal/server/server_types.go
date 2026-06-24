package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/alertssvc"
	"github.com/gritive/GrainFS/internal/server/incidentsvc"
	"github.com/gritive/GrainFS/internal/server/receiptsvc"
	"github.com/gritive/GrainFS/internal/storage"
)

// ClusterInfo provides cluster state for the monitoring dashboard. Snapshot
// folds optional topology/liveness data into one call.
type ClusterInfo interface {
	NodeID() string
	State() string
	Term() uint64
	LeaderID() string
	Peers() []string
	LivePeers() []string
	Snapshot() cluster.ClusterStatus
	ObjectIndexSummary(bucket string) cluster.ObjectIndexSummary
	PlacementReport(bucket, key string, maxRows int) cluster.PlacementReport
	// CapabilityEvidence reports each peer's currently-known capability
	// readiness as `peer → capability → ready`. Empty map when no gate is
	// wired (e.g. tests using a stub). Used by /v1/cluster/capabilities so
	// callers can wait for gossip propagation before sending gated traffic.
	CapabilityEvidence() map[string]map[string]bool
}

type ClusterMembership interface {
	RemoveVoter(ctx context.Context, id string) error
}

type JoinClusterFunc func(nodeID, raftAddr, peers, clusterKey string) error

// ExpandPlacementResult reports the outcome of a topology-generation growth
// (S7-7). NoOp is true when no new candidate groups were present, so no
// generation was recorded.
type ExpandPlacementResult struct {
	Base     []string `json:"base"`
	Expanded []string `json:"expanded"`
	Added    []string `json:"added"`
	Removed  []string `json:"removed,omitempty"`
	NoOp     bool     `json:"no_op"`
}

// ExpandPlacementFunc records the current shard groups as a new placement
// generation so object placement starts using the groups formed since boot
// (S7-7). Injected by serveruntime, which holds the coordinator + meta-raft.
type ExpandPlacementFunc func(ctx context.Context) (ExpandPlacementResult, error)

type ReadIndexer interface {
	ReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error
}

type RaftSnapshotter interface {
	TriggerRaftSnapshot(ctx context.Context) (raft.SnapshotResult, error)
	RaftSnapshotStatus() (raft.SnapshotStatus, error)
}

// PolicyAuthorizer is the policy seam that Layer 1 (S3 iamCheck) funnels
// through. *s3auth.Authorizer satisfies it; in production it is wrapped by
// *pdp.Decorator so the external PDP is chained with deny-override on the
// S3 data plane. Only Authorize is needed here (AuthorizePrincipal is a
// control-plane-only entry, not called on this field).
type PolicyAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend          storage.Backend
	ops              *storage.Operations
	readIndexer      ReadIndexer
	raftSnapshots    RaftSnapshotter
	dataDir          string
	scrubber         *scrubber.BackgroundScrubber
	verifier         *s3auth.CachingVerifier
	protocolCredAuth *protocolCredentialAuth
	iamStore         *iam.Store
	iamAudit         *iam.AuditLogger
	authz            *s3auth.RequestAuthorizer
	policyAuthorizer PolicyAuthorizer
	mutations        *MutationBroker

	hertz       *server.Hertz
	tlsListener *HotTLSListener // §5 T43: SIGHUP-driven cert reload
	hub         *Hub
	policyStore *CompiledPolicyStore

	lifecycle *lifecycle.Service

	cluster         ClusterInfo
	membership      ClusterMembership
	joinCluster     JoinClusterFunc
	expandPlacement ExpandPlacementFunc
	balancer        BalancerInfo
	evStore         *eventstore.Store
	alerts          *alertssvc.State
	receiptAPI      *receipt.API
	incidentStore   incident.StateStore
	mutationGate    *MutationGate
	degradedFlag    atomic.Bool
	shardCache      *shardcache.Cache
	receipt         *receiptsvc.Handler
	incidentH       *incidentsvc.Handler
	proxyTrust      *ProxyTrust // §5 T45: trusted-proxy Forwarded / X-Forwarded-* validator

	readAfterWriteRetryTimeout  time.Duration
	readAfterWriteRetryInterval time.Duration

	eventQueueSize int

	eventWorker *eventWorker

	metricsGatherer prometheus.Gatherer
}

type ServerStorage struct {
	Ops     *storage.Operations
	Backend storage.Backend
}
