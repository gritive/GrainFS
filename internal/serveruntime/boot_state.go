package serveruntime

import (
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/gritive/GrainFS/internal/volume"
)

// bootState carries the rolling state of Run's boot sequence. Phase functions
// populate typed fields here; the rest of Run reads them. Cleanups registered
// via AddCleanup run LIFO at function exit, matching Go's defer semantics.
//
// Lifecycle:
//
//	state := newBootState(cfg)
//	defer state.Cleanup()
//	if err := bootValidateConfig(state); err != nil { return err }
//	if err := bootOpenMetaDB(state); err != nil { return err }
//	// state.db, state.nodeID, ... populated for downstream phases
type bootState struct {
	cfg      Config
	cleanups []func()
	cancel   context.CancelFunc // set by Run; triggers graceful shutdown

	// Resolved config (populated by bootValidateConfig).
	nodeID      string
	raftAddr    string
	peers       []string
	clusterMode bool
	// join-pending mode: set by bootValidateConfig when .join-pending file exists.
	joinMode bool
	joinAddr string
	metaDir  string
	raftDir  string
	bootID   string

	// Storage role tracking (populated incrementally by storage phases;
	// readers in run.go body use these for the boot decision summary).
	roleRegistry     badgerrole.Registry
	startupDecisions []badgerrole.Decision
	recoveryJournal  *badgerrole.JournalWriter

	// Open DBs. Each phase that opens one of these also
	// registers the matching teardown via AddCleanup.
	db          *badger.DB // bootOpenMetaDB
	sharedFSMDB *badger.DB // bootOpenSharedFSMDB — <dataDir>/shared-fsm/, per-node shared FSM-state DB (C2 P3)

	// Transport (populated by transport phases — bootQUICTransport,
	// bootPeerConnections, bootGroupRaftMux). transportPSK records the
	// resolved cluster key (disk > flag > ephemeral). raftAddr is updated
	// in-place by bootQUICTransport once Listen resolves a kernel-picked
	// port (operator passed 127.0.0.1:0).
	transportPSK  string
	quicTransport *transport.QUICTransport
	groupRaftMux  *raft.GroupRaftQUICMux

	// Meta-raft + DataGroup wiring (populated by raft phases —
	// bootMetaRaftWiring, bootDataGroupRouter, bootRotationAndAdminAPI,
	// bootMetaRaftStart). Phase ordering enforces "callbacks registered
	// BEFORE Start fires the apply loop" race-free guarantee:
	// bootDataGroupRouter and bootRotationAndAdminAPI register callbacks
	// against state.metaRaft.FSM(); bootMetaRaftStart then calls Start.
	metaRaft         *cluster.MetaRaft
	metaTransport    *cluster.MetaTransportQUIC
	dgMgr            *cluster.DataGroupManager
	clusterRouter    *cluster.Router
	rotationKeystore *transport.Keystore
	rotationWorker   *cluster.RotationWorker
	iamAdminAPI      *iam.AdminAPI
	iamProposer      *iam.MetaProposer
	nfsExportSvc     *nfsexport.ExportService

	// Storage runtime (populated by storage phases — bootShardService,
	// bootStreamRouter, bootOwnedGroupsAndEC). The data plane: shard
	// service, stream multiplexing on QUIC, distributed backend, per-group
	// raft instantiation, and EC config. effectiveEC is captured here so
	// downstream phases (PR 6: balancer, healreceipt) can re-read the
	// resolved EC profile without re-deriving from cluster size.
	// node is the data-plane Raft node exposed through the cluster.RaftNode
	// interface.
	node             cluster.RaftNode
	rpcTransport     *cluster.RaftQUICRPCTransport
	streamRouter     *transport.StreamRouter
	shardSvc         *cluster.ShardService
	distBackend      *cluster.DistributedBackend
	shardCache       *shardcache.Cache
	effectiveEC      cluster.ECConfig
	stopApply        chan struct{}
	rebalancer       *cluster.Rebalancer
	loadReporter     *cluster.LoadReporter
	loadReporterStor *cluster.NodeStatsStore

	// Services + shutdown (populated by services phases — PR 6 onwards).
	// bootSnapshotAndApplyLoop owns: fsm (the distBackend's FSM —
	// distBackend.FSMRef() — group-0 keyspace over the shared FSM-state DB),
	// cachedBackend (the post-pack LRU read cache; the wrapping chain
	// inner→outer is distBackend → packblob (optional) → cachedBackend →
	// WAL → pullthrough, and the final two wrappers are added downstream
	// until later phases claim them).
	//
	// As of M5 PR 29 the v1 raft.SnapshotManager is no longer wired —
	// raftv2 owns snapshot lifecycle internally.
	fsm           *cluster.FSM
	cachedBackend *storage.CachedBackend

	// PR-final services-extra phases. Each field's owning phase is annotated.

	// bootBalancerAndGossip
	balancerProposer *cluster.BalancerProposer
	gossipReceiver   *cluster.GossipReceiver

	// bootWALAndForwarders
	wal               *wal.WAL
	walDir            string
	forwardSender     *cluster.ForwardSender
	forwardReceiver   *cluster.ForwardReceiver
	metaForwardSender *cluster.MetaProposeForwardSender
	metaReadSender    *cluster.MetaCatalogReadSender
	clusterCoord      *cluster.ClusterCoordinator
	seedGroups        int

	// bootBackendWrap
	backend          storage.Backend
	recoveryReadOnly bool
	diskCollector    *cluster.DiskCollector

	// bootSrvOptsAndReceipt
	srvOpts          []server.Option
	clusterAlerts    *server.AlertsState
	receiptWiring    *HealReceiptWiring
	incidentRecorder *incident.Recorder
	lifecycleSvc     *lifecycle.Service
	migrationSvc     *migration.Service
	mutationGate     *server.MutationGate
	volMgr           *volume.Manager

	// bootHTTPServerAndAdmin
	srv        *server.Server
	tokenStore *dashboard.TokenStore
	adminDeps  *admin.Deps
	adminSrv   *admin.Server

	// bootRecoveryAndScrubber
	scrubDirector *scrubber.Director
	activeEmitter scrubber.Emitter
}

// newBootState returns an empty state bound to cfg. Caller is responsible for
// calling Cleanup (typically via defer) once.
func newBootState(cfg Config) *bootState {
	return &bootState{cfg: cfg}
}

// AddCleanup pushes fn onto the cleanup stack. Order matters: cleanups run in
// LIFO so this mirrors `defer` registration order. Nil fn is a no-op (we still
// record the slot so positional references in tests stay stable).
func (s *bootState) AddCleanup(fn func()) {
	s.cleanups = append(s.cleanups, fn)
}

// Cleanup drains the cleanup stack in LIFO order. Each cleanup is wrapped so a
// panic in one fn does not skip the rest of the stack — the panic is logged
// and swallowed. Safe to call multiple times: subsequent calls are no-ops
// because the slice is reset to nil after the first drain.
func (s *bootState) Cleanup() {
	if s == nil {
		return
	}
	cleanups := s.cleanups
	s.cleanups = nil
	for i := len(cleanups) - 1; i >= 0; i-- {
		fn := cleanups[i]
		if fn == nil {
			continue
		}
		runCleanup(i, fn)
	}
}

// runCleanup invokes fn under a recover so a panic does not stop the stack.
// Split out so the test for panic safety can assert log output without
// reaching into an inline closure.
func runCleanup(idx int, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().
				Int("cleanup_idx", idx).
				Interface("panic", r).
				Msg("boot cleanup panicked; continuing with remaining cleanups")
		}
	}()
	fn()
}
