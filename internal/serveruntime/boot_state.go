package serveruntime

import (
	"context"
	"crypto/tls"
	"io"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/server/alertssvc"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
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
	cancel   context.CancelFunc // set by Run; triggers graceful shutdown

	// Resolved config (populated by bootValidateConfig).
	nodeID      string
	raftAddr    string
	peers       []string
	clusterMode bool
	// inviteJoinMode: set by maybeInviteJoin (W9b) when this node boots from a
	// Zero-CA invite bundle (FreshJoin/Resume). It drives the joiner's raft gating
	// (raftPeers=nil, raftCfg.JoinMode, skip Bootstrap), forces isGenesisBoot
	// false (so the joiner uses NewEmptyDEKKeeper + strict cluster.id load against
	// the secrets staged from Phase-1). Phase-2 membership ACK rides the dedicated
	// join transport (the invite-join is now the ONLY cluster-join path).
	inviteJoinMode bool
	// inviteJoin carries the Phase-1 outcome (resolved bundle, leaderID, node
	// SPKI) so the post-boot Phase-2 ACK can redrive DialJoin without re-reading
	// the bundle env var. Set by maybeInviteJoin; consumed in
	// bootWALAndForwardersPart1.
	inviteJoin *inviteJoinState
	metaDir    string
	raftDir    string
	bootID     string
	// priorState is captured by bootValidateConfig BEFORE bootOpenMetaDB
	// runs. True if <dataDir>/meta or <dataDir>/raft already contained
	// data on entry — signals a restart of an existing node (versus a
	// fresh-cluster init on an empty data dir). Consumed by
	// wireDEKKeeper to refuse silent auto-regeneration of keys/0.key and
	// cluster.id when prior raft/meta state exists.
	priorState bool

	// Storage role tracking (populated incrementally by storage phases;
	// readers in run.go body use these for the boot decision summary).
	roleRegistry     badgerrole.Registry
	startupDecisions []badgerrole.Decision
	recoveryJournal  *badgerrole.JournalWriter

	// Open DBs. Each phase that opens one of these also
	// registers the matching teardown via AddCleanup.
	db          *badger.DB // bootOpenMetaDB
	sharedFSMDB *badger.DB // bootOpenSharedFSMDB — <dataDir>/shared-fsm/, per-node shared FSM-state DB (C2 P3)
	// sharedFSMStore wraps sharedFSMDB once as the cluster MetadataStore
	// (Phase 6.5 S3: composition root injects; backends are shared-mode and
	// never close it — serveruntime owns the raw DB lifecycle).
	sharedFSMStore cluster.MetadataStore

	// Transport (populated by transport phases — bootClusterTransport,
	// bootGroupRaftMux). transportPSK records the
	// resolved cluster key (disk > flag > ephemeral). raftAddr is updated
	// in-place by bootClusterTransport once Listen resolves a kernel-picked
	// port (operator passed 127.0.0.1:0).
	transportPSK     string
	clusterTransport transport.ClusterTransport
	groupRaftMux     *raft.GroupRaftMux

	// Meta-raft + DataGroup wiring (populated by raft phases —
	// bootMetaRaftWiring, bootDataGroupRouter, bootRotationAndAdminAPI,
	// bootMetaRaftStart). Phase ordering enforces "callbacks registered
	// BEFORE Start fires the apply loop" race-free guarantee:
	// bootDataGroupRouter and bootRotationAndAdminAPI register callbacks
	// against state.metaRaft.FSM(); bootMetaRaftStart then calls Start.
	metaRaft         *cluster.MetaRaft
	capabilityGate   *cluster.CapabilityGate
	metaTransport    *cluster.MetaRaftTransport
	dgMgr            *cluster.DataGroupManager
	clusterRouter    *cluster.Router
	rotationKeystore *transport.Keystore
	rotationWorker   *cluster.RotationWorker
	iamAdminAPI      *iam.AdminAPI
	iamProposer      *iam.MetaProposer
	iamPolicyStores  *IAMStores
	cfgStore         *config.Store
	// pdpTokenSource is the single PDP TokenSource / admin.PDPTokenManager
	// instance, created once via ensurePDPTokenSource (adminAuthorizer is
	// called twice + the Deps literal references it). Its live encryptor is
	// refreshed by wireIAMEncryptor on fresh boot and snapshot-restore swaps.
	pdpTokenSource   *pdpTokenSource
	dekKeeper        *encrypt.DEKKeeper
	rewrapController *encrypt.RewrapController // created in wireDEKKeeper (early); lanes registered post-backend in wireRewrapLanes
	// clusterID is the 16-byte cluster identity loaded in wireDEKKeeper and (on
	// restore) rebuildDEKKeeperFromRestore, threaded as the single source into
	// the data-plane DEKKeeperAdapters so the WRITE and READ (ShardService)
	// clusterID are identical. Empty until wireDEKKeeper runs.
	clusterID []byte
	// raftStoreKey is the node-local Badger encryption key for raft v2 log
	// stores. It is sealed at rest in keys.d/raft-store.key.enc under kekStore
	// and loaded before data/meta raft stores open.
	raftStoreKey       []byte
	raftStoreKeyKEKVer atomic.Uint32
	// perNodeSPKI is the SPKI of this node's persisted per-node transport
	// identity (keys.d/node.key.enc), populated by ensureNodeIdentity after the
	// KEK store is wired (spec §6 D-rev3 step 1). It is the steady identity for
	// EVERY member (genesis/normal-boot included), not just invite-joiners. The
	// node does NOT yet present this on the wire — accept-side foundation only.
	// Task 6 consumes perNodeSPKI for self-registration.
	perNodeSPKI [32]byte
	// perNodeKeyKEKGen is the KEK generation that currently seals
	// keys.d/node.key.enc after any boot-time re-seal. Self-registration
	// publishes it so KEK prune can avoid bricking restarted voters.
	perNodeKeyKEKGen uint32
	// perNodeCert is this node's per-node identity TLS certificate,
	// populated alongside perNodeSPKI (PR-2a §8d F5 fix). The Task-5
	// onPresentFlip callback closure captures BOTH cert and SPKI to call
	// the cluster transport.FlipPresent.
	perNodeCert tls.Certificate
	// kekStore is the cluster-wide KEK store loaded by wireDEKKeeper. Phase
	// A holds a single version (0). Later phases use it for rotation,
	// prune, and join keystore catch-up. Receivers reach the active version
	// + cluster_id via state.handshakeVerifier rather than reading the
	// store directly.
	kekStore *encrypt.KEKStore
	// handshakeVerifier gates cluster-join admission via HMAC-SHA256 challenge-
	// response. The SAME instance must be wired into both MetaJoinReceiver and
	// MetaChallengeReceiver so the issued-nonce map is shared. Set by
	// wireDEKKeeper; consumed by bootWALAndForwardersPart1. §7 T55 / B1.
	handshakeVerifier *encrypt.HandshakeVerifier
	// kekLeaseTracker counts in-flight KEK consumers per version. Phase B has no
	// runtime acquire sites — Phase D wires them (raft snapshot reader holding
	// K_old during decrypt + InstallSnapshot receiver). LeaseSnapshot RPC returns
	// 0 deterministically in Phase B, which is correct: prune-after-retire only
	// requires lease_count == 0, and there are no consumers to drive it nonzero.
	kekLeaseTracker *encrypt.KEKLeaseTracker
	// kekRotationLeader orchestrates leader-side KEK lifecycle (rotate / retire /
	// prune). Wired by bootKEKRotationLeader once the peer probe + raft config
	// reader implementations land. Nil during boot → admin endpoints return 503
	// "kek admin disabled" instead of a 5xx. Set via MetaRaft.SetKEKRotationLeader
	// so the leadership watcher cancels in-flight propose calls on step-down.
	kekRotationLeader *cluster.KEKRotationLeader

	// bannerWriter is the destination for the §5 T46 default bucket anonymous
	// access banner. Set to os.Stdout in production via newBootState; tests
	// that exercise bootPhase0Banner substitute a *bytes.Buffer.
	bannerWriter io.Writer

	// Storage runtime (populated by storage phases — bootShardService,
	// bootShardRoutes, bootOwnedGroupsAndEC). The data plane: shard
	// service, native shard routes on the cluster transport, distributed backend, per-group
	// raft instantiation, and EC config. effectiveEC is captured here so
	// downstream phases (PR 6: balancer, healreceipt) can re-read the
	// resolved EC profile without re-deriving from cluster size.
	// node is the data-plane Raft node exposed through the cluster.RaftNode
	// interface.
	node cluster.RaftNode
	//nolint:unused // assigned by boot_phases_storage_runtime_test.go to seed the RPC transport for tests.
	rpcTransport     *cluster.RaftRPCTransport
	shardSvc         *cluster.ShardService
	distBackend      *cluster.DistributedBackend
	packedBackend    *packblob.PackedBackend // single-node packed-blob fast path; nil in cluster / when packing off (DEK rewrap lane source)
	shardCache       *shardcache.Cache
	effectiveEC      cluster.ECConfig
	stopApply        chan struct{}
	rebalancer       *cluster.Rebalancer
	evacuator        *cluster.DataGroupEvacuator
	loadReporter     *cluster.LoadReporter
	loadReporterStor *gossip.NodeStatsStore

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
	balancerProposer    *cluster.BalancerProposer
	gossipReceiver      *gossip.GossipReceiver
	placementStatsStore *gossip.NodeStatsStore // nil when balancer disabled

	// bootWALAndForwardersPart1
	forwardSender     *cluster.ForwardSender
	forwardReceiver   *cluster.ForwardReceiver
	metaForwardSender *cluster.MetaProposeForwardSender
	clusterCoord      *cluster.ClusterCoordinator
	seedGroups        int
	// seedMu (Option B) serializes the leader-side deferred seed-on-quorum across
	// overlapping post-join hooks within one process. The "should I seed" decision
	// itself is derived (handleDeferredSeed), not stored, so it survives a leader
	// change; seedMu only guards the one-shot critical section on a single leader.
	seedMu sync.Mutex
	// joinListener is the Zero-CA join listener (leader side, W9) serving
	// the two-phase invite handler on cfg.JoinListenAddr with a persisted stable
	// cert. Nil in single-node mode. Closed via AddCleanup on shutdown; its
	// Addr()/SPKI() are exposed to W10 (invite-bundle advertisement) via
	// JoinListenerAddr()/JoinListenerSPKI().
	joinListener joinListener
	// coalesceCfg is the cluster-wide coalesce/cap configuration derived from
	// CLI flags. Stored here so that GroupBackends instantiated after
	// bootWALAndForwardersPart1 (including dynamically created shard groups) can
	// inherit the same cap as state.distBackend (group-0).
	coalesceCfg cluster.CoalesceConfig

	// bootBackendWrap
	backend          storage.Backend
	lifecycleBackend storage.Backend
	recoveryReadOnly bool
	diskCollector    *cluster.DiskCollector

	// bootSrvOptsAndReceipt
	srvOpts          []server.Option
	clusterAlerts    *alertssvc.State
	receiptWiring    *HealReceiptWiring
	incidentRecorder *incident.Recorder
	lifecycleSvc     *lifecycle.Service
	lifecycleStore   *lifecycle.Store
	migrationSvc     *migration.Service
	mutationGate     *server.MutationGate

	// bootMetaRaftWiring: meta policy-invalidation worker.
	// Registered as a post-commit hook on MetaFSM before MetaRaft.Start().
	// SetInvalidate is called in bootHTTPServerAndAdmin once the server is up.
	// Stop is registered via AddCleanup in bootMetaRaftWiring.
	metaPolicyInvalidationWorker *cluster.MetaPolicyInvalidationWorker

	// bootHTTPServerAndAdmin
	srv                     *server.Server
	tokenStore              *dashboard.TokenStore
	adminDeps               *admin.Deps
	adminSrv                *admin.Server
	protocolCredentialStore *protocred.Store
	protocolCredentials     admin.ProtocolCredentialService

	// bootRecoveryAndScrubber
	scrubDirector *scrubber.Director
	activeEmitter scrubber.Emitter
}

// newBootState returns an empty state bound to cfg. Caller is responsible for
// calling Cleanup (typically via defer) once.
//
// bannerWriter defaults to os.Stdout — set by Run() before phase dispatch
// rather than here so the field stays zero-valued in tests that do not opt in
// to banner emission. See bootPhase0Banner for the consumer.
func newBootState(cfg Config) *bootState {
	s := &bootState{cfg: cfg}
	if cfg.InviteJoin != nil {
		s.inviteJoinMode = true
		s.inviteJoin = cfg.InviteJoin
	}
	return s
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
