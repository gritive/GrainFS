package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/iam"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/alertssvc"
	"github.com/gritive/GrainFS/internal/server/iceberg"
	"github.com/gritive/GrainFS/internal/server/incidentsvc"
	"github.com/gritive/GrainFS/internal/server/receiptsvc"
	"github.com/gritive/GrainFS/internal/server/snapshotsvc"
	"github.com/gritive/GrainFS/internal/snapshot"
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

// PerVersionCutoverReadiness is the JSON response shape for
// GET /v1/cluster/verify-per-version-cutover.
type PerVersionCutoverReadiness struct {
	Complete    int      `json:"complete"`
	Gaps        int      `json:"gaps"`
	Stuck       int      `json:"stuck"`
	Unknown     int      `json:"unknown"`
	Excluded    int      `json:"excluded"`
	Ineligible  int      `json:"ineligible"`
	GapRefs     []string `json:"gap_refs,omitempty"`
	StuckRefs   []string `json:"stuck_refs,omitempty"`
	UnknownRefs []string `json:"unknown_refs,omitempty"`
}

// VerifyPerVersionCutoverFunc aggregates per-version quorum-meta coverage
// across this node's hosted-group buckets and returns a readiness tally.
// Injected by serveruntime (which holds *cluster.DistributedBackend).
// An optional bucket filter ("") means "all hosted buckets".
type VerifyPerVersionCutoverFunc func(ctx context.Context, bucket string) (PerVersionCutoverReadiness, error)

type ReadIndexer interface {
	ReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error
}

type auditSearcher interface {
	SearchS3(ctx context.Context, f audit.SearchFilter) ([]audit.SearchRow, error)
}

type RaftSnapshotter interface {
	TriggerRaftSnapshot(ctx context.Context) (raft.SnapshotResult, error)
	RaftSnapshotStatus() (raft.SnapshotStatus, error)
}

// PolicyAuthorizer is the policy seam that Layer 1 (S3 iamCheck) + all Iceberg
// authz paths funnel through. *s3auth.Authorizer satisfies it; in production it is
// wrapped by *pdp.Decorator so the external PDP is chained with deny-override on the
// S3/Iceberg data plane. Only Authorize is needed here (AuthorizePrincipal is a
// control-plane-only entry, not called on this field).
type PolicyAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend           storage.Backend
	ops               *storage.Operations
	readIndexer       ReadIndexer
	raftSnapshots     RaftSnapshotter
	dataDir           string
	snapshotKEK       snapshot.KEKSource
	snapshotClusterID [16]byte
	snapMgr           *snapshot.Manager
	scrubber          *scrubber.BackgroundScrubber
	verifier          *s3auth.CachingVerifier
	protocolCredAuth  *protocolCredentialAuth
	iamStore          *iam.Store
	iamAudit          *iam.AuditLogger
	authz             *s3auth.RequestAuthorizer
	policyAuthorizer  PolicyAuthorizer
	mutations         *MutationBroker

	hertz       *server.Hertz
	tlsListener *HotTLSListener // §5 T43: SIGHUP-driven cert reload
	hub         *Hub
	policyStore *CompiledPolicyStore

	lifecycle       *lifecycle.Service
	icebergCatalog  icebergcatalog.Catalog
	icebergDisabled bool
	auditEmitter    *audit.Emitter
	auditOutbox     *audit.Outbox
	auditSearcher   auditSearcher
	auditNodeID     string

	auditInternalAccessKey string
	auditInternalSecretKey string
	auditInternalVerifier  *s3auth.CachingVerifier

	cluster                   ClusterInfo
	membership                ClusterMembership
	joinCluster               JoinClusterFunc
	expandPlacement           ExpandPlacementFunc
	verifyPerVersionCutoverFn VerifyPerVersionCutoverFunc
	balancer                  BalancerInfo
	evStore                   *eventstore.Store
	alerts                    *alertssvc.State
	receiptAPI                *receipt.API
	incidentStore             incident.StateStore
	mutationGate              *MutationGate
	degradedFlag              atomic.Bool
	shardCache                *shardcache.Cache
	jwtKeys                   *iamjwt.KeySet
	iceberg                   *iceberg.Handler
	receipt                   *receiptsvc.Handler
	incidentH                 *incidentsvc.Handler
	snapshotH                 *snapshotsvc.Handler
	proxyTrust                *ProxyTrust // §5 T45: trusted-proxy Forwarded / X-Forwarded-* validator

	readAfterWriteRetryTimeout  time.Duration
	readAfterWriteRetryInterval time.Duration

	eventQueueSize int

	eventWorker *eventWorker

	metricsGatherer prometheus.Gatherer
}

type ServerStorage struct {
	Ops          *storage.Operations
	Backend      storage.Backend
	Snapshotable storage.Snapshotable
	DBProvider   storage.DBProvider
}
