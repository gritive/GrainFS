package server

import (
	"context"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/cache/blockcache"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
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
}

type ClusterMembership interface {
	RemoveVoter(ctx context.Context, id string) error
}

type JoinClusterFunc func(nodeID, raftAddr, peers, clusterKey string) error

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

// Server handles S3-compatible API requests using Hertz.
type Server struct {
	backend       storage.Backend
	ops           *storage.Operations
	readIndexer   ReadIndexer
	raftSnapshots RaftSnapshotter
	dataDir       string
	snapshotEnc   *encrypt.Encryptor
	snapMgr       *snapshot.Manager
	scrubber      *scrubber.BackgroundScrubber
	verifier      *s3auth.CachingVerifier
	iamStore      *iam.Store
	iamAudit      *iam.AuditLogger
	authz         *s3auth.RequestAuthorizer
	mutations     *MutationBroker
	iamProposer   iam.Proposer

	hertz       *server.Hertz
	hub         *Hub
	volMgr      *volume.Manager
	policyStore *CompiledPolicyStore

	lifecycle      *lifecycle.Service
	icebergCatalog icebergcatalog.Catalog
	auditEmitter   *audit.Emitter
	auditOutbox    *audit.Outbox
	auditSearcher  auditSearcher
	auditNodeID    string

	auditInternalAccessKey string
	auditInternalVerifier  *s3auth.CachingVerifier

	cluster       ClusterInfo
	membership    ClusterMembership
	joinCluster   JoinClusterFunc
	balancer      BalancerInfo
	evStore       *eventstore.Store
	alerts        *AlertsState
	receiptAPI    *receipt.API
	incidentStore incident.StateStore
	mutationGate  *MutationGate
	degradedFlag  atomic.Bool
	blockCache    *blockcache.Cache
	shardCache    *shardcache.Cache

	eventWorker *eventWorker
}

type ServerStorage struct {
	Ops           *storage.Operations
	Backend       storage.Backend
	VolumeBackend storage.Backend
	Snapshotable  storage.Snapshotable
	DBProvider    storage.DBProvider
}
