package serveruntime

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
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

	// Resolved config (populated by bootValidateConfig).
	nodeID      string
	raftAddr    string
	peers       []string
	clusterMode bool
	metaDir     string
	raftDir     string

	// Storage role tracking (populated incrementally by storage phases;
	// readers in run.go body use these for the boot decision summary).
	roleRegistry     badgerrole.Registry
	startupDecisions []badgerrole.Decision

	// Open DBs and log stores. Each phase that opens one of these also
	// registers the matching teardown via AddCleanup.
	db              *badger.DB           // bootOpenMetaDB
	logStore        *raft.BadgerLogStore // bootOpenRaftLogStore
	sharedRaftLogDB *badger.DB           // bootOpenSharedRaftLogDB (optional)

	// storeOpts are the raft.BadgerLogStoreOption set used to open the
	// meta log store. Captured on bootState so per-data-group shared log
	// stores opened later (run.go around shared-log fan-out) reuse the
	// same option set without re-deriving it from cfg.
	storeOpts []raft.BadgerLogStoreOption

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

	// Storage runtime (populated by storage phases — bootShardService,
	// bootStreamRouter, bootOwnedGroupsAndEC). The data plane: shard
	// service, stream multiplexing on QUIC, distributed backend, per-group
	// raft instantiation, and EC config. effectiveEC is captured here so
	// downstream phases (PR 6: balancer, healreceipt) can re-read the
	// resolved EC profile without re-deriving from cluster size.
	node             *raft.Node
	rpcTransport     *raft.QUICRPCTransport
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
	// bootSnapshotAndApplyLoop owns: fsm (meta-FSM bound to state.db),
	// snapMgr (auto-snapshot every 10000 applied entries), cachedBackend
	// (the post-pack LRU read cache; the wrapping chain inner→outer is
	// distBackend → packblob (optional) → cachedBackend → WAL → pullthrough,
	// and the final two wrappers are added downstream until later phases
	// claim them).
	fsm           *cluster.FSM
	snapMgr       *raft.SnapshotManager
	cachedBackend *storage.CachedBackend
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
