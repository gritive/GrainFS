package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/incident/badgerstore"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/packblob"
	"github.com/gritive/GrainFS/internal/storage/pullthrough"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/gritive/GrainFS/internal/volume"
)

// Run is the cluster-mode server entry point. cmd/grainfs/runServe builds a
// Config from cobra flags and pre-resolves auth/encryptor inputs, then calls
// Run. Run owns lifecycle: starts every component, blocks on ctx.Done(), and
// orchestrates graceful shutdown.
func Run(ctx context.Context, cfg Config) error {
	addr := cfg.Addr
	dataDir := cfg.DataDir
	nodeID := cfg.NodeID
	raftAddr := cfg.RaftAddr
	clusterKey := cfg.ClusterKey
	authOpts := cfg.AuthOpts
	encryptor := cfg.Encryptor
	peers := cfg.Peers
	joinMode := cfg.JoinMode
	raftAddrExplicit := cfg.RaftAddrExplicit

	if nodeID == "" {
		var err error
		nodeID, err = GenerateNodeID(dataDir)
		if err != nil {
			return fmt.Errorf("generate node ID: %w", err)
		}
		log.Info().Str("component", "server").Str("node_id", nodeID).Msg("auto-generated node ID")
	}

	// D6/D7: --cluster-key is required when running in actual cluster mode
	// (peers > 0 || join != ""). Solo runs through this same function but
	// does not require a cluster key — Run handles both modes.
	clusterMode := len(peers) > 0 || joinMode
	if clusterMode {
		if err := transport.ValidateClusterKey(clusterKey); err != nil {
			if errors.Is(err, transport.ErrEmptyClusterKey) {
				return fmt.Errorf("--cluster-key is required in cluster mode (generate with: openssl rand -hex 32)")
			}
			log.Warn().Err(err).Msg("--cluster-key is below recommended length")
		}
	}
	if joinMode {
		if len(peers) > 0 {
			return fmt.Errorf("--join cannot be used with --peers")
		}
		if raftAddr == "" {
			return fmt.Errorf("--raft-addr is required when --join is set")
		}
		peers = []string{cfg.JoinAddr}
	}

	// When no peers are configured, we boot a singleton Raft node on a
	// loopback port so a single-machine deployment still goes through the
	// unified storage path (versioning, scrubber, lifecycle, WAL all work).
	// Operators who later want to expand the cluster pick a concrete
	// --raft-addr and --peers list; the loopback default is only for the
	// "just start it" path.
	if raftAddr == "" {
		if len(peers) > 0 {
			return fmt.Errorf("--raft-addr is required when --peers is set")
		}
		// Singleton: let the kernel pick a free port so multiple instances
		// (dev, tests) coexist without collisions. No peer will ever reach it.
		raftAddr = "127.0.0.1:0"
	}

	metaDir := filepath.Join(dataDir, "meta")
	raftDir := filepath.Join(dataDir, "raft")
	roleRegistry := badgerrole.DefaultRegistry()
	startupDecisions := make([]badgerrole.Decision, 0, 8)
	recordStartupDecision := func(decision badgerrole.Decision) {
		startupDecisions = append(startupDecisions, decision)
	}

	// Auto-migrate BEFORE any filesystem or lock side effects. If Raft dir
	// doesn't exist but meta dir holds an existing local BadgerDB, convert
	// in place. Two things previously broke this branch on fresh cluster
	// starts with an empty dataDir:
	//   1. MkdirAll ran before this check, so os.Stat(metaDir) succeeded
	//      on a freshly-created empty dir and triggered a spurious
	//      migration.
	//   2. The migration opens the meta DB, but we had already opened it
	//      here, and BadgerDB takes an exclusive directory lock, so the
	//      migration aborted with "Another process is using this Badger
	//      database".
	// Moving the migration above both MkdirAll and badger.Open removes
	// both failure modes.
	if _, err := os.Stat(raftDir); os.IsNotExist(err) {
		if info, err := os.Stat(metaDir); err == nil && info.IsDir() {
			// A populated local meta dir has .sst / .vlog / MANIFEST files.
			// Distinguish "real data" from "empty dir someone pre-created"
			// by checking for any entries; empty → skip migration.
			if entries, err := os.ReadDir(metaDir); err == nil && len(entries) > 0 {
				log.Info().Str("component", "migrate").Msg("auto-migrating local metadata to cluster format")
				if err := cluster.MigrateLegacyMetaToCluster(dataDir, nodeID); err != nil {
					return fmt.Errorf("auto-migrate: %w", err)
				}
				log.Info().Str("component", "migrate").Msg("auto-migration complete")
			}
		}
	}

	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir at %s: %w\n  recovery: check that the parent directory exists and the user has write permission", metaDir, err)
	}
	dbOpts := badgerutil.SmallOptions(metaDir)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return fmt.Errorf("open metadata db at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK), see README#badger-troubleshooting", metaDir, err, metaDir)
	}
	defer db.Close()
	metaVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryMeta, db)
	defer resourcewatch.DeregisterDB(metaVlogEntry)
	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	recordStartupDecision(badgerrole.ProbeWritable(db, badgerrole.RoleMeta, "", metaDir))
	if startupDecisions[len(startupDecisions)-1].Status != badgerrole.DecisionOK {
		return server.PreflightBadger(db, metaDir, nil)
	}

	if cfg.RaftElectionTimeout > 0 && cfg.RaftHeartbeatInterval > 0 && cfg.RaftElectionTimeout < 3*cfg.RaftHeartbeatInterval {
		return fmt.Errorf("--raft-election-timeout (%s) must be >= 3 * --raft-heartbeat-interval (%s)", cfg.RaftElectionTimeout, cfg.RaftHeartbeatInterval)
	}
	if cfg.QUICMuxEnabled && cfg.QUICMuxFlushWindow > 0 && cfg.RaftHeartbeatInterval > 0 && cfg.QUICMuxFlushWindow >= cfg.RaftHeartbeatInterval {
		return fmt.Errorf("--quic-mux-flush (%s) must be << --raft-heartbeat-interval (%s)", cfg.QUICMuxFlushWindow, cfg.RaftHeartbeatInterval)
	}
	// Meta-raft heartbeat is fixed (not user-configurable) and shares the
	// same coalescer flush window. If the flush window were larger than
	// the meta heartbeat, meta hb dispatch could be delayed past the meta
	// election deadline. Cap conservatively at < half of the meta heartbeat.
	if cfg.QUICMuxEnabled && cfg.QUICMuxFlushWindow > 0 && cfg.QUICMuxFlushWindow*2 >= cluster.MetaRaftHeartbeatInterval {
		return fmt.Errorf("--quic-mux-flush (%s) must be << meta-raft heartbeat (%s); meta-raft uses a fixed 150ms heartbeat / 750ms election", cfg.QUICMuxFlushWindow, cluster.MetaRaftHeartbeatInterval)
	}

	var storeOpts []raft.BadgerLogStoreOption
	if cfg.BadgerManagedMode {
		storeOpts = append(storeOpts, raft.WithManagedMode())
	}
	logStore, err := raft.NewBadgerLogStore(raftDir, storeOpts...)
	if err != nil {
		return fmt.Errorf("open raft store at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK)", raftDir, err, raftDir)
	}
	defer logStore.Close()
	if !logStore.IsShared() && logStore.DB() != nil {
		raftLogVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, logStore.DB())
		defer resourcewatch.DeregisterDB(raftLogVlogEntry)
	}
	recordStartupDecision(badgerrole.Decision{
		Role:   badgerrole.RoleMetaRaftLog,
		Path:   raftDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})

	// C2 P0b prototype: optionally open one shared raft-log BadgerDB so all
	// data groups share a single instance instead of opening their own. Reduces
	// process-level BadgerDB instance count from (2N+1) → (2+1) for the log
	// half. FSM state DB consolidation deferred to full C2.
	var sharedRaftLogDB *badger.DB
	if cfg.SharedBadgerEnabled {
		// Refuse to silently abandon legacy per-group raft logs. Existing
		// deployments that started before P0b have raft state under
		// <dataDir>/groups/*/raft. Ignoring those and opening a fresh shared
		// DB would silently reset every group's term/votedFor/log — i.e.,
		// data loss. Fail with a clear migration message instead.
		groupsDir := filepath.Join(dataDir, "groups")
		if entries, _ := os.ReadDir(groupsDir); len(entries) > 0 {
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				legacyRaftDir := filepath.Join(groupsDir, e.Name(), "raft")
				if st, err := os.Stat(legacyRaftDir); err == nil && st.IsDir() {
					return fmt.Errorf("shared-badger=true incompatible with legacy per-group raft dir %s. "+
						"This deployment was started before C2 P0b. Use --shared-badger=false to keep "+
						"per-group raft logs, or wipe %s to start fresh on the new layout (DESTRUCTIVE — "+
						"only on test clusters or after a full backup)", legacyRaftDir, dataDir)
				}
			}
		}
		sharedDir := filepath.Join(dataDir, "shared-raft-log")
		if err := os.MkdirAll(sharedDir, 0o755); err != nil {
			return fmt.Errorf("mkdir shared raft-log dir: %w", err)
		}
		sharedRaftLogDB, err = badger.Open(badgerutil.RaftLogOptions(sharedDir, true))
		if err != nil {
			return fmt.Errorf("open shared raft-log badger at %s: %w", sharedDir, err)
		}
		defer sharedRaftLogDB.Close()
		sharedRaftLogVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, sharedRaftLogDB)
		defer resourcewatch.DeregisterDB(sharedRaftLogVlogEntry)
		recordStartupDecision(badgerrole.Decision{
			Role:   badgerrole.RoleSharedRaftLog,
			Path:   sharedDir,
			Status: badgerrole.DecisionOK,
			Action: badgerrole.RecoveryActionNone,
		})
		log.Info().Str("dir", sharedDir).Msg("shared raft-log DB enabled (C2 P0b prototype)")
	}

	// Start QUIC transport for inter-node communication. Resolution order
	// (rotation-spec D10):
	//   1. keys.d/current.key wins over --cluster-key flag if both differ
	//      (warn emitted; refuse-to-start path explicitly NOT used).
	//   2. Disk only: use disk silently.
	//   3. Flag only: use flag, mirror to keys.d/current.key on first boot.
	//   4. Both empty + cluster mode: refused upstream by ValidateClusterKey.
	//      Both empty + solo mode: generate ephemeral so zero-config holds.
	resolvedKey, warn, err := ResolveClusterKey(dataDir, clusterKey)
	if err != nil {
		return fmt.Errorf("resolve cluster key: %w", err)
	}
	if warn != "" {
		log.Warn().Msg(warn)
	}
	transportPSK := resolvedKey
	if transportPSK == "" {
		ephemeral, err := GenerateEphemeralClusterKey()
		if err != nil {
			return fmt.Errorf("init QUIC transport: %w", err)
		}
		transportPSK = ephemeral
	}
	quicTransport, err := transport.NewQUICTransport(transportPSK)
	if err != nil {
		return fmt.Errorf("init QUIC transport: %w", err)
	}
	// Forwarded S3 PUTs can fan out into EC shard body streams on the bucket
	// owner. Keep enough bulk capacity for that nested data path while meta and
	// raft traffic remain independently classed.
	quicTransport.SetTrafficLimits(transport.TrafficLimits{Bulk: 64})
	if err := quicTransport.Listen(ctx, raftAddr); err != nil {
		return fmt.Errorf("start QUIC transport on %s: %w\n  recovery: confirm UDP port is free (lsof -i UDP:%s), check firewall, or pass --raft-addr=127.0.0.1:0 to pick any free port", raftAddr, err, raftAddr)
	}
	defer quicTransport.Close()
	// Resolve `raftAddr` to its actual bound port. When the operator asked
	// for 127.0.0.1:0 (singleton default) QUIC picks a free UDP port; we
	// need that concrete address in allNodes so shard placement produces
	// dialable self entries.
	if local := quicTransport.LocalAddr(); local != "" {
		raftAddr = local
	}

	// Connect to all peers
	for _, peer := range peers {
		if err := quicTransport.Connect(ctx, peer); err != nil {
			log.Warn().Str("peer", peer).Err(err).Msg("failed to connect to peer (will retry lazily)")
		}
	}

	raftCfg := raft.DefaultConfig(nodeID, peers)
	raftCfg.ManagedMode = cfg.BadgerManagedMode
	raftCfg.LogGCInterval = cfg.RaftLogGCInterval
	node := raft.NewNode(raftCfg, logStore)
	if !joinMode {
		if err := node.Bootstrap(); err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
			return fmt.Errorf("raft bootstrap: %w", err)
		}
	}

	// Wire QUIC transport to Raft RPC layer
	rpcTransport := raft.NewQUICRPCTransport(quicTransport, node)
	rpcTransport.SetTransport()

	// GroupRaftQUICMux multiplexes per-group raft RPCs over StreamGroupRaft.
	// Created BEFORE NewMetaTransportQUICMux so the meta-raft transport can
	// auto-register its node on the mux at construction time. This closes
	// the codex P1 #3 startup race: if EnableMux ran before metaNode was
	// registered, all inbound meta calls would hit "mux: unknown group
	// __meta__" and meta election would stall.
	groupRaftMux := raft.NewGroupRaftQUICMux(quicTransport)
	if cfg.QUICMuxEnabled {
		groupRaftMux.EnableMux(cfg.QUICMuxPoolSize, cfg.QUICMuxFlushWindow)
		log.Info().
			Int("pool", cfg.QUICMuxPoolSize).
			Dur("flush", cfg.QUICMuxFlushWindow).
			Msg("group raft mux mode enabled (R+H Phase 2 prototype)")
	}

	// Meta-Raft: dedicated control-plane Raft group for cluster membership.
	metaRaft, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{
		NodeID:  nodeID,
		Peers:   peers,
		DataDir: dataDir,
	})
	if err != nil {
		return fmt.Errorf("init meta-raft: %w", err)
	}
	// Mux-aware constructor: auto-registers metaRaft.Node() on groupRaftMux
	// under the magic groupID "__meta__" so receiver-side mux dispatch is
	// wired before any meta heartbeat hits the wire.
	metaTransport := cluster.NewMetaTransportQUICMux(quicTransport, metaRaft.Node(), groupRaftMux)
	metaRaft.SetTransport(metaTransport)

	// PR-D: DataGroupManager + Router — created before metaRaft.Start() so the
	// OnBucketAssigned callback is registered before the apply loop starts (race-free).
	dgMgr := cluster.NewDataGroupManager()
	clusterRouter := cluster.NewRouter(dgMgr)
	clusterRouter.SetDefault("group-0")
	// SetOnBucketAssigned uses f.mu.Lock() internally; must be called before Start().
	metaRaft.FSM().SetOnBucketAssigned(func(bucket, groupID string) {
		clusterRouter.AssignBucket(bucket, groupID)
	})

	// 클러스터 키 회전 — RotationWorker가 FSM phase 변경에 반응하여 디스크
	// I/O와 transport identity swap을 수행 (D16 분리). 콜백은 metaRaft.Start
	// 전에 등록해야 첫 apply 이벤트를 놓치지 않는다 (race-free).
	rotationKeystore := transport.NewKeystore(dataDir)
	rotationWorker := cluster.NewRotationWorker(rotationKeystore, quicTransport, nodeID)
	metaRaft.FSM().SetOnRotationApplied(func(st cluster.RotationState) {
		_ = rotationWorker.OnPhaseChange(st)
	})
	// Seed rotation FSM steady state with active SPKI so RotateKeyBegin can be
	// validated against the current cluster key (D10).
	if _, activeSPKI, err := transport.DeriveClusterIdentity(transportPSK); err == nil {
		metaRaft.FSM().SetRotationSteady(activeSPKI)
	} else {
		log.Warn().Err(err).Msg("failed to seed rotation FSM steady state; rotation will be unavailable until next restart")
	}

	if !joinMode {
		if err := metaRaft.Bootstrap(); err != nil {
			return fmt.Errorf("meta-raft bootstrap: %w", err)
		}
	}
	if err := metaRaft.Start(ctx); err != nil {
		return fmt.Errorf("meta-raft start: %w", err)
	}
	// previous.key cleanup goroutine — deletes keys.d/previous.key after
	// RotationPreviousGrace expires. Runs on all nodes (FSM state is
	// identical via raft); each node deletes its own local file.
	metaRaft.StartPreviousKeyCleanup(ctx, rotationKeystore)
	if err := StartRotationSocket(ctx, dataDir, metaRaft); err != nil {
		log.Warn().Err(err).Msg("rotation socket failed to start; cluster rotate-key CLI will be unavailable")
	}
	defer metaRaft.Close()

	// Seed Router with any bucket assignments already persisted in FSM state.
	// Start() returns before replay finishes; onBucketAssigned fires live updates.
	clusterRouter.Sync(metaRaft.FSM().BucketAssignments())

	clusterSize := 1 + len(peers)
	// Seed data groups from cluster size only. Operators no longer choose this:
	// group count is placement headroom, not a durability policy.
	seedGroups := clusterSize * 4
	if seedGroups < 8 {
		seedGroups = 8
	}
	effectiveEC := cluster.AutoECConfigForClusterSize(clusterSize)
	if !effectiveEC.IsActive(clusterSize) {
		return fmt.Errorf("no effective EC profile for cluster size %d", clusterSize)
	}
	normalGroupVoters := effectiveEC.NumShards()
	if !joinMode {
		if err := WaitForMetaRaftLeader(ctx, metaRaft, 15*time.Second); err != nil {
			return err
		}
		addNodeCtx, addNodeCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := metaRaft.ProposeAddNode(addNodeCtx, cluster.MetaNodeEntry{ID: nodeID, Address: raftAddr, Role: 0}); err != nil {
			log.Debug().Err(err).Str("node_id", nodeID).Str("addr", raftAddr).Msg("seed node metadata propose failed (non-fatal)")
		}
		addNodeCancel()

		if err := SeedInitialShardGroups(ctx, metaRaft, nodeID, raftAddr, peers, seedGroups, normalGroupVoters); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		clusterRouter.SetRequireExplicitAssignments(true)
	}

	// Create ShardService for distributed data replication
	shardSvcOpts := []cluster.ShardServiceOption{cluster.WithEncryptor(encryptor)}
	if cfg.DirectIO {
		shardSvcOpts = append(shardSvcOpts, cluster.WithDirectIO())
		log.Info().Msg("direct I/O enabled for local shard writes (page cache bypass)")
	}
	if cfg.MeasureReadAmp {
		readamp.Enable()
		log.Info().Msg("read-amplification simulator enabled — see grainfs_readamp_* counters at /metrics")
	}
	shardSvcOpts = append(shardSvcOpts, cluster.WithNodeAddressBook(metaRaft.FSM()))
	shardSvc := cluster.NewShardService(dataDir, quicTransport, shardSvcOpts...)

	// Set up StreamRouter: Raft RPCs on Control stream, Shard RPCs on Data stream
	router := transport.NewStreamRouter()
	router.Handle(transport.StreamControl, rpcTransport.Handler())
	router.Handle(transport.StreamData, shardSvc.HandleRPC())
	quicTransport.SetStreamHandler(router.Dispatch)
	// Body handler must register on QUICTransport's internal router; the
	// catch-all (router.Dispatch) only sees per-message handlers and cannot
	// consume body streams. Pre-fix, every StreamShardWriteBody fell through
	// to the catch-all → returned nil → stream closed without response →
	// caller saw "decode response: read header: EOF" → N×replication produced
	// only the leader's local copy.
	quicTransport.HandleBody(transport.StreamShardWriteBody, shardSvc.HandleWriteBody())
	quicTransport.HandleRead(transport.StreamShardReadBody, shardSvc.HandleReadBody())

	node.Start()
	defer node.Stop()

	distBackend, err := cluster.NewDistributedBackend(dataDir, db, node)
	if err != nil {
		return fmt.Errorf("failed to initialize distributed storage: %w", err)
	}

	// Wire shard service for distributed fan-out replication
	allNodes := append([]string{raftAddr}, peers...)
	distBackend.SetShardService(shardSvc, allNodes)

	// EC shard cache (Phase 2 #3 follow-up). Construct it before any per-group
	// backend can be instantiated so group-1..N receive the same cache wiring
	// as the legacy group-0 backend.
	shardCache := shardcache.New(cfg.ShardCacheSize)
	distBackend.SetShardCache(shardCache)
	log.Info().Int64("bytes", cfg.ShardCacheSize).Msg("ec shard cache configured")

	// Live multi-raft sharding (v0.0.7.0): group-0 keeps using the shared
	// distBackend (legacy single-backend deployment is the group-0 instance);
	// groups 1..N-1 get their own per-group BadgerDB+raft via instantiateLocalGroup
	// when this node is a voter (see ownedGroups loop below).
	group0Backend := cluster.WrapDistributedBackend("group-0", distBackend)
	group0 := cluster.NewDataGroupWithBackend(
		"group-0",
		SeedShardGroupVoters(nodeID, raftAddr, peers, metaRaft.FSM().Nodes(), "group-0", 3),
		group0Backend,
	)
	dgMgr.Add(group0)
	distBackend.SetRouter(clusterRouter)
	distBackend.SetShardGroupSource(metaRaft.FSM())

	// PR-D: Rebalancer 배선 — LoadReporter가 meta-Raft FSM에 부하 스냅샷을 커밋하고
	// Rebalancer가 leader에서 주기적으로 평가해 RebalancePlan을 제안·실행한다.
	rebalancerCfg := cluster.DefaultRebalancerConfig()
	rebalancer := cluster.NewRebalancer(nodeID, metaRaft, dgMgr, rebalancerCfg)
	rebalancer.SetGroupRebalancer(
		cluster.NewDataGroupPlanExecutor(nodeID, dgMgr, metaRaft.FSM(), metaRaft),
	)
	metaRaft.FSM().SetOnRebalancePlan(func(plan *cluster.RebalancePlan) {
		if joinMode {
			return
		}
		execCtx, execCancel := context.WithTimeout(ctx, rebalancerCfg.PlanTimeout)
		go func() {
			defer execCancel()
			if err := rebalancer.ExecutePlan(execCtx, plan); err != nil {
				log.Error().Err(err).Str("plan_id", plan.PlanID).Msg("rebalancer: ExecutePlan failed")
			}
		}()
	})
	go rebalancer.Run(ctx)

	// EC config — instantiateLocalGroup needs it for per-group EC.

	// Live multi-raft sharding (v0.0.7.0): instantiate per-group raft.Node +
	// BadgerDB + GroupBackend for each group this node is a voter of. group-0
	// is already wired with the shared distBackend (legacy compat).
	//
	// Two paths cover all entries:
	//   1. Iterate ShardGroups() once for entries already in FSM.
	//   2. SetOnShardGroupAdded for entries replayed/applied later.
	// Both call into instantiateOwnedIfNeeded which is idempotent — duplicates
	// from concurrent paths are no-ops.
	ownedGroups := struct {
		mu           sync.Mutex
		wg           sync.WaitGroup
		m            map[string]*cluster.GroupBackend
		inFlight     map[string]bool // entry.ID currently being instantiated; prevents duplicate concurrent OpenSharedLogStore / badger.Open
		shuttingDown bool
	}{m: make(map[string]*cluster.GroupBackend), inFlight: make(map[string]bool)}

	// Shared stop channel for all apply loops (distBackend + per-group).
	// Must be initialized before any goroutine that passes it to RunApplyLoop.
	stopApply := make(chan struct{})

	// groupRaftMux was created earlier (before NewMetaTransportQUICMux) so
	// metaTransport could auto-register its node onto the mux. Each group
	// uses ForGroup(groupID) as its raft transport.

	instantiateOwnedIfNeeded := func(entry cluster.ShardGroupEntry) error {
		// group-0 is already wired with the shared distBackend.
		if entry.ID == "group-0" {
			return nil
		}
		EnsureShardGroupPlaceholder(dgMgr, entry)
		// Only instantiate for groups where we are a voter. Shard groups should
		// store node IDs; raftAddr remains a local alias for legacy/static
		// entries written before that invariant existed.
		groupNodeID, isVoter := cluster.NewShardGroupPeerSet(entry).MatchLocal(nodeID, raftAddr)
		if !isVoter {
			return nil
		}
		ownedGroups.mu.Lock()
		if ownedGroups.shuttingDown {
			ownedGroups.mu.Unlock()
			return nil
		}
		if _, ok := ownedGroups.m[entry.ID]; ok {
			ownedGroups.mu.Unlock()
			return nil // already instantiated
		}
		if ownedGroups.inFlight[entry.ID] {
			ownedGroups.mu.Unlock()
			return nil // another goroutine is currently bringing this group up
		}
		ownedGroups.inFlight[entry.ID] = true
		ownedGroups.mu.Unlock()
		// Make sure inFlight is cleared even if instantiation fails.
		defer func() {
			ownedGroups.mu.Lock()
			delete(ownedGroups.inFlight, entry.ID)
			ownedGroups.mu.Unlock()
		}()

		glc := cluster.GroupLifecycleConfig{
			NodeID:           groupNodeID,
			DataDir:          dataDir,
			ShardSvc:         shardSvc,
			Transport:        groupRaftMux.ForGroup(entry.ID),
			AddrBook:         metaRaft.FSM(),
			EC:               effectiveEC,
			ElectionTimeout:  cfg.RaftElectionTimeout,
			HeartbeatTimeout: cfg.RaftHeartbeatInterval,
		}
		if sharedRaftLogDB != nil {
			// Forward managed-mode and any future BadgerLogStoreOption to
			// the shared store so flags don't get silently dropped on the
			// shared path.
			ls, lerr := raft.OpenSharedLogStore(sharedRaftLogDB, entry.ID, storeOpts...)
			if lerr != nil {
				return fmt.Errorf("group %s: open shared log store: %w", entry.ID, lerr)
			}
			glc.LogStore = ls
		}
		gb, err := cluster.InstantiateLocalGroup(glc, entry)
		if err != nil {
			return fmt.Errorf("group %s: instantiate local group: %w", entry.ID, err)
		}
		gb.SetShardCache(shardCache)
		groupRaftMux.Register(entry.ID, gb.RaftNode())
		dgMgr.Add(cluster.NewDataGroupWithBackend(entry.ID, entry.PeerIDs, gb))
		go gb.RunApplyLoop(stopApply)
		ownedGroups.mu.Lock()
		ownedGroups.m[entry.ID] = gb
		ownedGroups.mu.Unlock()
		log.Info().Str("group_id", entry.ID).Strs("peers", entry.PeerIDs).Msg("instantiateLocalGroup ok")
		return nil
	}

	scheduleOwnedInstantiation := func(entry cluster.ShardGroupEntry) {
		ownedGroups.mu.Lock()
		if ownedGroups.shuttingDown {
			ownedGroups.mu.Unlock()
			return
		}
		ownedGroups.wg.Add(1)
		ownedGroups.mu.Unlock()
		go func() {
			defer ownedGroups.wg.Done()
			if err := instantiateOwnedIfNeeded(entry); err != nil {
				log.Error().Err(HandleRuntimeGroupInstantiationError(entry.ID, err)).Str("group_id", entry.ID).Msg("runtime data group instantiation failed")
			}
		}()
	}

	// Cold-start instantiation for entries already in FSM (restart path).
	// Run synchronously so Badger role failures feed the startup reducer before
	// the server accepts traffic.
	for _, entry := range metaRaft.FSM().ShardGroups() {
		if err := instantiateOwnedIfNeeded(entry); err != nil {
			recordStartupDecision(badgerrole.Decision{
				Role:    badgerrole.RoleGroupState,
				GroupID: entry.ID,
				Status:  badgerrole.DecisionOpenFailed,
				Action:  badgerrole.RecoveryActionStartReadOnly,
				Reason:  err.Error(),
				Err:     err,
			})
		}
	}
	// Runtime: handle entries replayed/applied after this point (fresh boot path).
	// Dispatch to goroutine so apply loop is not blocked by BadgerDB+raft.Node startup.
	metaRaft.FSM().SetOnShardGroupAdded(func(entry cluster.ShardGroupEntry) {
		scheduleOwnedInstantiation(entry)
	})

	// Shutdown hook: close all owned groups in parallel with 5s timeout each.
	defer func() {
		metaRaft.FSM().SetOnShardGroupAdded(nil)
		ownedGroups.mu.Lock()
		ownedGroups.shuttingDown = true
		ownedGroups.mu.Unlock()
		ownedGroups.wg.Wait()

		ownedGroups.mu.Lock()
		toClose := make([]*cluster.GroupBackend, 0, len(ownedGroups.m))
		for _, gb := range ownedGroups.m {
			toClose = append(toClose, gb)
		}
		ownedGroups.mu.Unlock()
		var wg sync.WaitGroup
		for _, gb := range toClose {
			wg.Add(1)
			go func(gb *cluster.GroupBackend) {
				defer wg.Done()
				if err := cluster.ShutdownLocalGroup(context.Background(), gb, 5*time.Second); err != nil {
					log.Warn().Err(err).Str("group_id", gb.ID()).Msg("shutdownLocalGroup")
				}
			}(gb)
		}
		wg.Wait()
	}()

	// LoadReporter: leader 전용 — NodeStatsStore에서 읽어 meta-Raft FSM에 부하 통계 커밋.
	loadReporterStore := cluster.NewNodeStatsStore(cluster.DefaultLoadReportInterval * 3)
	loadReporter := cluster.NewLoadReporter(nodeID, loadReporterStore, metaRaft, cluster.DefaultLoadReportInterval)
	go loadReporter.Run(ctx)

	distBackend.SetECConfig(effectiveEC)
	log.Info().
		Str("mode", "auto").
		Int("effective_k", effectiveEC.DataShards).
		Int("effective_m", effectiveEC.ParityShards).
		Bool("active", effectiveEC.IsActive(len(allNodes))).
		Int("cluster_size", len(allNodes)).Msg("cluster EC configured")

	// Set up snapshot manager: auto-snapshot every 10000 applied entries
	fsm := cluster.NewFSM(db)
	snapMgr := raft.NewSnapshotManager(logStore, fsm, raft.SnapshotConfig{Threshold: 10000})
	distBackend.SetSnapshotManager(snapMgr, node)

	// Restore from snapshot on startup
	snapIdx, err := snapMgr.Restore()
	if err != nil {
		log.Warn().Err(err).Msg("snapshot restore failed")
	} else if snapIdx > 0 {
		log.Info().Uint64("index", snapIdx).Msg("restored from snapshot")
	}

	// Wrapping chain (inner → outer): distBackend → packblob → cachedBackend →
	// WAL → pullthrough. Mirrors the pre-unification local path so operators
	// get identical semantics (small-object packing, LRU cache, PITR replay,
	// upstream pull-through) regardless of peer count.
	var inner storage.Backend = distBackend

	// Pack small objects into blob files when --pack-threshold is set.
	if cfg.PackThreshold > 0 {
		blobDir := filepath.Join(dataDir, "blobs")
		pb, err := packblob.NewPackedBackend(inner, blobDir, int64(cfg.PackThreshold))
		if err != nil {
			return fmt.Errorf("failed to initialize packed blob: %w", err)
		}
		inner = pb
		log.Info().Int("threshold", cfg.PackThreshold).Msg("packed blob storage enabled")
	}

	// Wrap with LRU read cache. Raft FSM-based invalidation ensures cache
	// consistency across nodes.
	cachedBackend := storage.NewCachedBackend(inner)

	distBackend.RegisterCacheInvalidator("s3-cache", cluster.CacheInvalidatorFunc(cachedBackend.InvalidateKey))

	go distBackend.RunApplyLoop(stopApply)

	// Start balancer if enabled (cluster mode only).
	var balancerProposer *cluster.BalancerProposer
	var gossipReceiver *cluster.GossipReceiver
	if cfg.BalancerEnabled {
		statsStore := cluster.NewNodeStatsStore(3 * cfg.BalancerGossipInterval)
		bopts := BalancerOptions{
			GossipInterval:      cfg.BalancerGossipInterval,
			WarmupTimeout:       cfg.BalancerWarmupTimeout,
			ImbalanceTriggerPct: cfg.BalancerImbalanceTriggerPct,
			ImbalanceStopPct:    cfg.BalancerImbalanceStopPct,
			MigrationRate:       cfg.BalancerMigrationRate,
			LeaderTenureMin:     cfg.BalancerLeaderTenureMin,
			CBThreshold:         cfg.BalancerCBThreshold,
			MigrationMaxRetries: cfg.BalancerMigrationMaxRetries,
			MigrationPendingTTL: cfg.BalancerMigrationPendingTTL,
		}
		var err error
		balancerProposer, gossipReceiver, err = StartBalancer(ctx, bopts, nodeID, dataDir, statsStore, node, peers, fsm, quicTransport, shardSvc, effectiveEC.NumShards())
		if err != nil {
			log.Warn().Err(err).Msg("balancer start failed")
		}
	}

	// Ensure a single GossipReceiver drains tr.Receive() whenever a feature
	// needs StreamReceipt gossip (heal-receipt's RoutingCache lives on this
	// path). Only one consumer is allowed because Receive() is a single
	// channel — competing readers would deliver each message to only one.
	// When balancer is off but heal-receipt is on, create a bare receiver;
	// its NodeStatsStore is unused in this path but required by the ctor.
	if gossipReceiver == nil && cfg.HealReceiptEnabled {
		standaloneStats := cluster.NewNodeStatsStore(3 * cfg.BalancerGossipInterval)
		gossipReceiver = cluster.NewGossipReceiver(quicTransport, standaloneStats)
		go gossipReceiver.Run(ctx)
		log.Info().Str("component", "gossip").Msg("gossip receiver started (receipt-only, balancer disabled)")
	}

	walDir := filepath.Join(dataDir, "wal")
	w, err := wal.Open(walDir)
	if err != nil {
		return fmt.Errorf("open WAL: %w", err)
	}
	defer w.Close()

	// v0.0.7.1 PR-D: Live multi-raft routing — ClusterCoordinator + ForwardSender/Receiver.
	// ClusterCoordinator implements storage.Backend and routes bucket-scoped ops to the
	// correct group leader via ForwardSender. 0x08 handler (ForwardReceiver) receives
	// forwarded calls on voter nodes and dispatches to local GroupBackend.
	forwardDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}
		reply, err := quicTransport.Call(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardStreamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}
		reply, err := quicTransport.CallWithBody(callCtx, peer, msg, body)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	forwardReadStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		msg := &transport.Message{Type: transport.StreamGroupForwardRead, Payload: payload}
		reply, body, err := quicTransport.CallRead(callCtx, peer, msg)
		if err != nil {
			return nil, nil, err
		}
		return reply.Payload, body, nil
	}

	forwardSender := cluster.NewForwardSender(forwardDialer).
		WithStreamDialer(forwardStreamDialer).
		WithReadStreamDialer(forwardReadStreamDialer).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})
	forwardReceiver := cluster.NewForwardReceiver(dgMgr)
	forwardReceiver.Register(shardSvc)

	metaForwardDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaProposeForward, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	metaForwardSender := cluster.NewMetaProposeForwardSender(metaForwardDialer)
	distBackend.SetBucketAssigner(cluster.NewForwardingBucketAssigner(metaRaft, func(ctx context.Context, command []byte) error {
		return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
	}))
	metaForwardReceiver := cluster.NewMetaProposeForwardReceiver(metaRaft)
	router.Handle(transport.StreamMetaProposeForward, metaForwardReceiver.Handle)
	metaJoinReceiver := cluster.NewMetaJoinReceiver(metaRaft)
	router.Handle(transport.StreamMetaJoin, metaJoinReceiver.Handle)
	metaReadDialer := func(peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamMetaCatalogRead, Payload: payload}
		reply, err := quicTransport.Call(ctx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	metaReadSender := cluster.NewMetaCatalogReadSender(metaReadDialer)

	clusterCoord := cluster.NewClusterCoordinator(
		distBackend,    // base for cluster-wide ops (CreateBucket, etc.)
		dgMgr,          // local owned groups (self-leader shortcut)
		clusterRouter,  // bucket → group lookup
		metaRaft.FSM(), // ShardGroupSource (PeerIDs, leader hints)
		nodeID,         // selfID for leader check
	).WithForwardSender(forwardSender).
		WithNodeAddressResolver(metaRaft.FSM()).
		WithSelfPeerAlias(raftAddr).
		WithECConfig(effectiveEC).
		WithObjectIndexProposer(cluster.NewForwardingObjectIndexProposer(metaRaft, func(ctx context.Context, command []byte) error {
			return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}))
	metaReadReceiver := cluster.NewMetaCatalogReadReceiver(cluster.NewMetaCatalog(metaRaft, clusterCoord, "s3://grainfs-tables/warehouse"))
	router.Handle(transport.StreamMetaCatalogRead, metaReadReceiver.Handle)
	if joinMode {
		if err := PerformMetaJoin(ctx, quicTransport, peers, nodeID, raftAddr); err != nil {
			return err
		}
		if err := WaitForShardGroupCount(ctx, metaRaft.FSM(), seedGroups, 30*time.Second); err != nil {
			return err
		}
		clusterRouter.Sync(metaRaft.FSM().BucketAssignments())
		clusterRouter.SetRequireExplicitAssignments(true)
	}

	// Use ClusterCoordinator as the primary backend for S3, NFSv4, NBD, then
	// wrap it with WAL so routed object mutations are captured for PITR.
	var backend storage.Backend = wal.NewBackend(clusterCoord, w)
	log.Info().Msg("v0.0.7.1 PR-D: ClusterCoordinator wired — live multi-raft routing enabled")

	// Wrap with pull-through cache if upstream is configured.
	if cfg.UpstreamEndpoint != "" {
		up, err := pullthrough.NewS3Upstream(cfg.UpstreamEndpoint, cfg.UpstreamAccessKey, cfg.UpstreamSecretKey)
		if err != nil {
			return fmt.Errorf("init upstream: %w", err)
		}
		backend = pullthrough.NewBackend(backend, up)
		log.Info().Str("upstream", cfg.UpstreamEndpoint).Msg("pull-through cache enabled")
	}
	startupResult := badgerrole.ReduceStartupDecisions(roleRegistry, startupDecisions)
	for _, decision := range startupResult.Decisions {
		log.Info().
			Str("role", string(decision.Role)).
			Str("group_id", decision.GroupID).
			Str("status", string(decision.Status)).
			Dur("probe_duration", decision.ProbeDuration).
			Msg("badger role startup probe")
	}
	startupReadOnly := startupResult.Mode == badgerrole.StartupModeReadOnly
	if startupResult.Mode == badgerrole.StartupModeBlocked {
		return fmt.Errorf("badger startup recovery blocked server start: %v", startupResult.BlockedReasons)
	}
	recoveryReadOnly := startupReadOnly
	if startupReadOnly {
		backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		log.Warn().Strs("reasons", startupResult.ReadOnlyReasons).Msg("badger startup recovery read-only gate enabled")
	}
	if marker, err := cluster.LoadRecoverClusterMarker(dataDir); err != nil {
		return fmt.Errorf("load recovery marker: %w", err)
	} else if marker != nil && !marker.Writable {
		recoveryReadOnly = true
		if !startupReadOnly {
			backend = storage.NewRecoveryWriteGate(backend, storage.ErrRecoveryWriteDisabled)
		}
		log.Warn().Str("marker", filepath.Join(dataDir, cluster.RecoverClusterMarkerPath)).Msg("recovered cluster write gate enabled")
	}

	// DiskCollector exposes grainfs_disk_used_pct metric. In multi-node mode
	// the balancer owns its own collector; in singleton mode nothing else
	// would emit disk stats. Register unconditionally — duplicate registration
	// is guarded inside NewDiskCollector.
	diskCollector := cluster.NewDiskCollector(nodeID, dataDir, nil, 30*time.Second)
	// Wiring of OnThreshold + Run() happens after clusterAlerts is built (below)
	// so the callback can dispatch critical webhooks on transitions.

	// Auto-create "default" bucket only for singleton startup. In cluster mode,
	// bucket creation is a cluster-wide metadata operation and must be driven by
	// an explicit client/API action, not repeated independently by every node.
	if ShouldCreateDefaultBucketOnStartup(peers, recoveryReadOnly) {
		if err := CreateDefaultBucketWithRetry(ctx, backend, 30*time.Second); err != nil {
			return fmt.Errorf("create default bucket: %w", err)
		}
	}

	// Start auto-snapshotter for object-level PITR snapshots (separate from
	// Raft snapshots above). Uses the WAL-wrapped backend so replay is
	// anchored to the object mutation log. Start only after startup bucket
	// metadata exists and the routed snapshot enumeration path is usable; data
	// groups are instantiated asynchronously during boot.
	if err := StartAutoSnapshotterWhenReady(ctx, dataDir, walDir, backend, cfg.SnapInterval, cfg.SnapRetain, 30*time.Second); err != nil {
		log.Warn().Err(err).Msg("auto-snapshot init failed")
	}

	log.Info().Str("component", "server").Str("version", cfg.Version).
		Str("node_id", nodeID).Str("raft_addr", raftAddr).Strs("peers", peers).
		Str("addr", addr).Str("data", dataDir).Msg("server started")

	// Startup config snapshot — debug-level log of every flag-derived runtime
	// value. Useful for diffing against a known-good config when an operator
	// is comparing two installs or troubleshooting drift after a restart.
	LogStartupConfigSnapshot(cfg.FlagsSnapshot, addr, dataDir, nodeID, raftAddr, peers)

	clusterAlerts := server.NewAlertsState(cfg.AlertWebhook, alerts.Options{Secret: cfg.AlertSecret}, alerts.DegradedConfig{})

	// Wire predictive disk warnings into the collector now that clusterAlerts
	// exists. Thresholds are taken as fractions on the flag (more natural for
	// operators) but DiskCollector works in percent.
	diskCollector.SetThresholds(cfg.DiskWarnFrac*100, cfg.DiskCritFrac*100)
	diskCollector.SetOnThreshold(func(level cluster.DiskThresholdLevel, pct float64, availBytes uint64) {
		// Webhook send may block on retries — dispatch in a goroutine so the
		// collect loop is never delayed.
		switch level {
		case cluster.DiskLevelCritical:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage CRITICAL")
			go func() {
				_ = clusterAlerts.Send(alerts.Alert{
					Type:     "disk_critical",
					Severity: alerts.SeverityCritical,
					Resource: nodeID,
					Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
				})
			}()
		case cluster.DiskLevelWarn:
			log.Warn().Float64("pct", pct).Uint64("avail_bytes", availBytes).Msg("disk usage warning")
			go func() {
				_ = clusterAlerts.Send(alerts.Alert{
					Type:     "disk_warn",
					Severity: alerts.SeverityWarning,
					Resource: nodeID,
					Message:  fmt.Sprintf("disk used %.1f%% (avail %d bytes) on %s", pct, availBytes, dataDir),
				})
			}()
		case cluster.DiskLevelOK:
			log.Info().Float64("pct", pct).Msg("disk usage recovered to normal")
		}
	})
	go diskCollector.Run(ctx)
	srvOpts := []server.Option{
		// cluster status / remove-peer must reflect *meta-raft* membership —
		// that is the cluster-wide membership ledger that `serve --join`
		// updates via performMetaJoin. The node initialised at line 872 is a
		// legacy per-process raft instance whose Peers() never grows on join.
		server.WithClusterInfo(NewRaftClusterInfo(metaRaft.Node(), peers, distBackend, metaRaft.FSM())),
		server.WithClusterMembership(NewRaftMembership(metaRaft.Node())),
		server.WithEventStore(eventstore.New(db)),
		server.WithAlerts(clusterAlerts),
		server.WithDataDir(dataDir),
	}
	if len(peers) == 0 && !raftAddrExplicit && !joinMode {
		legacyStore := icebergcatalog.NewStore(db, "s3://grainfs-tables/warehouse")
		metaCatalog := cluster.NewMetaCatalog(metaRaft, backend, "s3://grainfs-tables/warehouse")
		if err := MigrateLegacySingletonIcebergCatalog(ctx, legacyStore, metaCatalog, backend); err != nil {
			return fmt.Errorf("migrate singleton Iceberg catalog: %w", err)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(metaCatalog))
	} else {
		metaForward := func(ctx context.Context, command []byte) error {
			return metaForwardSender.Send(ctx, MetaProposalTargets(metaRaft.Node().LeaderID(), peers), command)
		}
		metaReadTargets := func() []string {
			return MetaProposalTargets(metaRaft.Node().LeaderID(), peers)
		}
		srvOpts = append(srvOpts, server.WithIcebergCatalog(cluster.NewMetaCatalogWithForwarders(metaRaft, backend, "s3://grainfs-tables/warehouse", metaForward, metaReadSender, metaReadTargets)))
	}
	// Propagate S3 auth from --access-key / --secret-key. Previously this
	// was local-only; cluster mode silently ran without auth regardless of
	// the flags.
	srvOpts = append(srvOpts, authOpts...)
	if balancerProposer != nil {
		srvOpts = append(srvOpts, server.WithBalancerInfo(NewBalancerInfoAdapter(balancerProposer)))
	}

	// Phase 16 Week 5 Slice 2 — HealReceipt API + gossip + broadcast fallback.
	rcptPSK := cfg.HealReceiptPSK
	if rcptPSK == "" {
		rcptPSK = clusterKey
	}
	rcptOpts := ReceiptOptions{
		Enabled:        cfg.HealReceiptEnabled,
		PSK:            rcptPSK,
		Retention:      cfg.HealReceiptRetention,
		GossipInterval: cfg.HealReceiptGossipInterval,
		WindowSize:     cfg.HealReceiptWindow,
	}
	newSrvOpts, receiptWiring, err := SetupClusterReceipt(
		ctx, rcptOpts, dataDir, nodeID, peers,
		quicTransport, router, gossipReceiver, srvOpts,
	)
	if err != nil {
		return fmt.Errorf("heal-receipt wiring: %w", err)
	}
	srvOpts = newSrvOpts
	defer receiptWiring.Close()

	var incidentRecorder *incident.Recorder
	incidentDB, incidentDecision, err := badgerrole.OpenRole(roleRegistry, badgerrole.RoleIncidentState, badgerrole.PathContext{DataDir: dataDir})
	if err != nil {
		if feature, ok := OptionalRoleDisabled(roleRegistry, incidentDecision); ok {
			LogOptionalRoleDisabled(badgerrole.RoleIncidentState, feature, err)
		} else {
			return fmt.Errorf("open incident db: %w", err)
		}
	} else {
		defer incidentDB.Close()
		incidentVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryIncident, incidentDB)
		defer resourcewatch.DeregisterDB(incidentVlogEntry)
		incidentStore := badgerstore.New(incidentDB)
		incidentRecorder = incident.NewRecorder(incidentStore, incident.NewReducer())
		distBackend.SetIncidentRecorder(incidentRecorder)
		srvOpts = append(srvOpts, server.WithIncidentStore(incidentStore))
		guardDeps := resourceguard.Deps{
			NodeID:   nodeID,
			Alerts:   clusterAlerts,
			Recorder: incidentRecorder,
		}
		if cfg.FDWatchEnabled {
			resourceguard.StartFD(ctx, cfg.FDOpts, guardDeps)
		}
		if cfg.GoroutineWatchEnabled {
			resourceguard.StartGoroutine(ctx, cfg.GoroutineOpts, guardDeps)
		}
		if cfg.VlogWatchEnabled {
			resourceguard.StartVlog(ctx, cfg.VlogResourceGuardOpts, guardDeps)
		}
	}
	clusterIncidentRecorder, scrubberIncidentRecorder := IncidentRecorderInterfaces(incidentRecorder)

	// Slice 4 of refactor/unify-storage-paths: cluster-mode lifecycle.
	// Construct the manager before srv.New so the S3 PutBucketLifecycle API
	// can reuse the same config store the worker scans. The worker itself
	// runs leader-only — see LifecycleManager.Run.
	var lifecycleMgr *cluster.LifecycleManager
	if cfg.LifecycleInterval > 0 {
		lifecycleMgr = cluster.NewLifecycleManager(distBackend, cfg.LifecycleInterval)
		srvOpts = append(srvOpts, server.WithLifecycleStore(lifecycleMgr.Store()))
	}

	volMgr, blockCache, dedupDB, err := BuildVolumeManager(VolumeManagerOptions{DedupEnabled: cfg.DedupEnabled, BlockCacheSize: cfg.BlockCacheSize}, dataDir, backend)
	if err != nil {
		return fmt.Errorf("volume manager: %w", err)
	}
	if dedupDB != nil {
		defer dedupDB.Close()
		dedupVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryDedup, dedupDB)
		defer resourcewatch.DeregisterDB(dedupVlogEntry)
	}
	srvOpts = append(srvOpts, server.WithVolumeManager(volMgr), server.WithBlockCache(blockCache), server.WithShardCache(shardCache))
	if !joinMode {
		srvOpts = append(srvOpts, server.WithReadIndexer(distBackend))
	}
	srvOpts = append(srvOpts, server.WithRaftSnapshotter(distBackend))

	distBackend.RegisterReadIndexHandler()
	distBackend.RegisterProposeForwardHandler()

	mutationGate := server.NewMutationGate(nil)
	if recoveryReadOnly {
		mutationGate.SetBlocked(storage.ErrRecoveryWriteDisabled)
	}
	srvOpts = append(srvOpts, server.WithMutationGate(mutationGate))

	srv := server.New(addr, backend, srvOpts...)

	// --- Admin / dashboard wiring (Volume CLI Phase B) ---
	// Open the dashboard auth token (creates <data>/dashboard.token mode 0600
	// on first run). Install the middleware on /ui/* and register /ui/api/*
	// admin routes BEFORE the data-plane Hertz starts serving.
	tokenStore, err := dashboard.Open(filepath.Join(dataDir, "dashboard.token"))
	if err != nil {
		return fmt.Errorf("dashboard token: %w", err)
	}
	adminDeps := &admin.Deps{
		Manager:    srv.VolumeManager(),
		Token:      tokenStore,
		PublicURL:  cfg.PublicURL,
		NodeID:     nodeID,
		PeerHealth: NewPeerHealthAdapter(distBackend),
		VlogBreakdown: NewVlogBreakdownAdapter(VlogBreakdownOptions{
			Enabled:       cfg.VlogWatchEnabled,
			DataDir:       dataDir,
			WarnRatio:     cfg.VlogWarnRatio,
			CriticalRatio: cfg.VlogCriticalRatio,
		}),
	}
	dataHertz := srv.HertzEngine()
	dataHertz.Use(server.DashboardTokenMiddleware(tokenStore))
	admin.RegisterUI(dataHertz, adminDeps)

	// Open the admin Unix socket. Operator commands (`grainfs volume *`, `grainfs
	// dashboard`) reach this socket; permissions are governed by the file mode
	// (0660) and optional --admin-group chown.
	adminSocket := cfg.AdminSocket
	if adminSocket == "" {
		adminSocket = filepath.Join(dataDir, "admin.sock")
	}
	adminSrv, err := admin.Start(admin.Config{
		SocketPath:  adminSocket,
		Group:       cfg.AdminGroup,
		Deps:        adminDeps,
		ExtraRoutes: srv.RegisterClusterAdminUDS,
	})
	if err != nil {
		return fmt.Errorf("admin server: %w", err)
	}
	log.Info().Str("path", adminSocket).
		Str("hint", fmt.Sprintf("--endpoint %q", adminSocket)).
		Msg("admin endpoint")
	// Best-effort fallback: ensures the socket is unlinked even if the explicit
	// shutdown path is bypassed (e.g. early return). The explicit shutdown
	// sequence below stops admin BEFORE the data plane drains so operator
	// commands cannot land on a half-shutdown server (spec A5).
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Stop(stopCtx)
	}()

	// receiptWiring.keyStore may be nil when heal-receipt is disabled — in that
	// case NewReceiptTrackingEmitter degrades to a pass-through (signing unhealthy).
	var activeEmitter scrubber.Emitter = srv.HealEmitter()
	if receiptWiring != nil && receiptWiring.Store() != nil {
		rte := server.NewReceiptTrackingEmitter(srv.HealEmitter(), receiptWiring.Store(), receiptWiring.KeyStore())
		defer rte.Close()
		activeEmitter = rte
	}

	// Phase 16 Week 3: cluster mode also needs startup recovery for the
	// node's local data dir (per-node multipart parts + .tmp leftovers).
	if rec, err := server.RunStartupRecovery(ctx, dataDir, activeEmitter); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn().Err(err).Msg("startup recovery failed")
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		log.Info().
			Int("orphan_tmp", rec.OrphanTmpRemoved).
			Int("orphan_multipart", rec.OrphanMultipartRemoved).
			Int("errors", len(rec.Errors)).Msg("startup recovery summary")
	}

	// Cluster-mode scrubber with ShardOwner filtering.
	// Each node only verifies shards assigned to it in the placement vector,
	// avoiding redundant cross-node I/O. RepairShard is idempotent so
	// concurrent repair from multiple nodes is safe.
	//
	// ShardPlacementMonitor detects locally-missing shards between full
	// scrub cycles; its onMissing callback calls RepairShardLocal which
	// resolves the latest version and pulls survivor shards from peers.
	// --- Object-layer replication scrub shared infrastructure.
	// Wired unconditionally so the admin-trigger Director works even when
	// periodic scrub is disabled (--scrub-interval=0). Periodic scrub +
	// placement monitor still require a positive interval.
	//
	// All three plumbings (walk, opener, repair) route through the local
	// data-group that owns the bucket — single-node serve still sits inside
	// a multi-raft group structure, so the volume bucket's files live under
	// {dataDir}/groups/<gid>/ rather than the bare distBackend root.
	groupBackendForBucket := func(bucket string) *cluster.DistributedBackend {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil || dg.Backend() == nil {
			return nil
		}
		return dg.Backend().DistributedBackend
	}
	opener := scrubber.LocalOpener(func(bucket, key string) (io.ReadCloser, error) {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return nil, fmt.Errorf("scrub opener: no local group for %s", bucket)
		}
		return gb.OpenLocalReplica(bucket, key)
	})
	repairer := scrubber.ReplicaRepairer(ReplicaRepairerFunc(func(rctx context.Context, bucket, key string) error {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return fmt.Errorf("scrub repair: no local group for %s", bucket)
		}
		return gb.RepairReplica(rctx, bucket, key)
	}))
	replSource := scrubber.NewReplicationObjectSource("replication", volume.VolumeBucketName, volume.MetaPrefix, backend)
	replVerifier := scrubber.NewReplicationVerifier(opener, repairer)

	// Director owns CLI-triggered sessions + (later) cluster-broadcast
	// trigger. Independent of periodic scrub interval — operators must be
	// able to run `grainfs volume scrub <name>` even with --scrub-interval=0.
	director := scrubber.NewDirector(scrubber.DirectorOpts{
		Incident:  scrubberIncidentRecorder,
		QueueSize: 64,
		NodeID:    nodeID,
	})
	director.Register("replication", replSource, replVerifier)

	// PR4: EC scrub source via per-bucket group resolver. The resolver maps
	// bucket → DataGroup → GroupBackend (which embeds *DistributedBackend so
	// it implements scrubber.Scrubbable). When the bucket lives on a peer-
	// owned group, the resolver returns (nil, false) and Iter closes an
	// empty channel; the FSM-replicated trigger ensures the owning peer's
	// Director runs the actual scrub.
	ecResolver := func(bucket string) (scrubber.Scrubbable, bool) {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil {
			return nil, false
		}
		gb := dg.Backend()
		if gb == nil {
			return nil, false
		}
		return gb, true
	}
	ecSource := scrubber.NewECScrubSource(ecResolver, nodeID)
	ecScrubVerifier := scrubber.NewShardVerifier(distBackend)
	ecScrubLimiter := rate.NewLimiter(rate.Limit(100), 100)
	ecVerifier := scrubber.NewECScrubVerifier(distBackend, ecScrubVerifier, ecScrubLimiter, activeEmitter, nodeID, ecSource)
	director.Register("ec", ecSource, ecVerifier)

	// PR4: cluster-wide scrub trigger via meta-raft. Each node's MetaFSM
	// fires onScrubTrigger when MetaScrubTriggerCmd applies; Director.ApplyFromFSM
	// creates a session for the same SessionID and runs the source with the
	// resolver above.
	if metaRaft != nil {
		metaRaft.FSM().SetOnScrubTrigger(func(entry scrubber.ScrubTriggerEntry) {
			director.ApplyFromFSM(entry)
		})
	}
	forwardReceiver.WithScrubSessionLookup(director)

	director.Start(ctx)
	adminDeps.Director = director
	adminDeps.ScrubProposer = NewScrubProposerAdapter(metaRaft, director, nodeID)
	adminDeps.ScrubAggregator = NewScrubAggregatorAdapter(clusterCoord)

	if cfg.ScrubInterval > 0 {
		sc := scrubber.New(distBackend, cfg.ScrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.RegisterSource("replication", replSource, replVerifier)
		sc.Start(ctx)

		placementMonitors := NewPlacementMonitorRegistry()
		startPlacementMonitor := func(monitorCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			if clusterIncidentRecorder != nil {
				gb.SetIncidentRecorder(clusterIncidentRecorder)
			}
			placementMonitor := cluster.NewShardPlacementMonitor(gb.FSMRef(), gb, shardSvc, gb.NodeID(), cfg.ScrubInterval)
			splitShardKey := func(shardKey string) (string, string) {
				objectKey, versionID := shardKey, ""
				if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
					objectKey, versionID = shardKey[:i], shardKey[i+1:]
				}
				return objectKey, versionID
			}
			placementMonitor.SetOnMissing(func(bucket, shardKey string, shardIdx int) {
				// shardKey from placement resolution is objectKey+"/"+versionID.
				// Split on the last "/" so RepairShard can skip LookupLatestVersion.
				objectKey, versionID := splitShardKey(shardKey)
				correlationID := uuid.Must(uuid.NewV7()).String()
				receiptID := "rcpt-" + correlationID
				repairReq := cluster.IncidentRepairRequest{
					Bucket:        bucket,
					Key:           objectKey,
					VersionID:     versionID,
					ShardIdx:      shardIdx,
					Recorder:      clusterIncidentRecorder,
					CorrelationID: correlationID,
				}
				if err := gb.RepairShardLocalWithIncident(monitorCtx, repairReq); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor repair failed")
				} else if receiptWiring != nil && receiptWiring.Store() != nil && receiptWiring.KeyStore() != nil {
					r := &receipt.HealReceipt{
						ReceiptID:     receiptID,
						Timestamp:     time.Now().UTC(),
						Object:        receipt.ObjectRef{Bucket: bucket, Key: objectKey, VersionID: versionID},
						ShardsLost:    []int32{int32(shardIdx)},
						ShardsRebuilt: []int32{int32(shardIdx)},
						EventIDs:      []string{correlationID},
						CorrelationID: correlationID,
					}
					if err := receipt.Sign(r, receiptWiring.KeyStore()); err != nil {
						log.Warn().Str("correlation_id", correlationID).Err(err).Msg("placement monitor receipt sign failed")
					} else if err := receiptWiring.Store().Put(r); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt store failed")
					} else if err := receiptWiring.Store().Flush(); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt flush failed")
					} else if err := gb.RecordRepairReceiptSigned(context.Background(), repairReq, receiptID); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor incident proof update failed")
					}
				}
			})
			placementMonitor.SetOnCorrupt(func(bucket, shardKey string, shardIdx int, readErr error) {
				objectKey, versionID := splitShardKey(shardKey)
				if err := gb.QuarantineCorruptShardLocal(bucket, objectKey, versionID, shardIdx, readErr.Error()); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor quarantine failed")
				}
			})
			go placementMonitor.Start(monitorCtx)
		}
		refreshPlacementMonitors := func() {
			placementMonitors.Refresh(ctx, dgMgr.All(), startPlacementMonitor)
		}
		refreshPlacementMonitors()
		go func() {
			ticker := time.NewTicker(cfg.ScrubInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					refreshPlacementMonitors()
				}
			}
		}()
		log.Info().Dur("interval", cfg.ScrubInterval).Msg("cluster scrubber started")
	}

	// Background EC resharding is group-local: each locally-owned DataGroup has
	// its own Raft leader and object metadata DB. Followers skip each pass in
	// ReshardManager, but we still start one manager per local group so
	// leadership changes are picked up without serve-level rewiring.
	if cfg.ReshardInterval > 0 {
		reshardManagers := NewReshardManagerRegistry()
		startReshardManager := func(managerCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			leader := gb.RaftNode()
			if leader == nil {
				log.Warn().Str("group", dg.ID()).Msg("reshard manager skipped: group has no raft node")
				return
			}
			go cluster.NewReshardManager(gb, leader, cfg.ReshardInterval).Start(managerCtx)
		}
		refreshReshardManagers := func() {
			reshardManagers.Refresh(ctx, dgMgr.All(), startReshardManager)
		}
		refreshReshardManagers()
		go func() {
			ticker := time.NewTicker(cfg.ReshardInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					refreshReshardManagers()
				}
			}
		}()
		log.Info().Dur("interval", cfg.ReshardInterval).Msg("cluster reshard manager started")
	}

	// Start the leader-aware worker loop. Only the Raft leader runs the
	// worker; followers skip the scan so we don't waste IO on proposals that
	// would be rejected anyway. LifecycleManager polls node.State() and
	// starts/stops the worker on leadership transitions.
	if lifecycleMgr != nil {
		go lifecycleMgr.Run(ctx)
		log.Info().Dur("interval", cfg.LifecycleInterval).Msg("cluster lifecycle manager started")
	}

	// Start the degraded mode monitor — checks live node count vs EC threshold
	// every 30 s. The first check fires immediately so the server knows its
	// state before serving any requests.
	degradedMon := cluster.NewDegradedMonitor(distBackend, clusterAlerts.Tracker(), cfg.DegradedInterval).
		WithQuorumCheck(node, clusterAlerts)
	go degradedMon.Run(ctx)

	go func() {
		if err := srv.Run(); err != nil {
			log.Error().Err(err).Str("addr", addr).
				Msg("http server error — confirm TCP port is free (lsof -i TCP:" + addr + "), or pass --port=0 to pick a free port")
		}
	}()

	// Post-Phase-18 local-path merge: universal node services (NFS/NFSv4/NBD)
	// are now wired in cluster mode too, not just local. Formerly local-only
	// because runCluster never called the NFS/NBD wiring. Scrubber/lifecycle
	// remain local-specific pending ECBackend→cluster integration (A.2).
	nodeSvc := StartNodeServices(ctx, backend, volMgr, cfg.NFS4Port, cfg.NBDPort, cfg.NBDVolumeSize, distBackend)
	defer nodeSvc.Close()

	<-ctx.Done()
	log.Info().Str("component", "server").Msg("graceful shutdown started")

	// 1. Stop admin Unix socket FIRST so operator commands fail fast with
	// "connection refused" instead of landing on a server that's draining.
	// Spec A5: admin-first, data-plane-second.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := adminSrv.Stop(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("admin server shutdown error")
	}

	// 2. Drain in-flight HTTP requests on the data plane.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("http server shutdown error")
	}

	// 3. Transfer Raft leadership before stopping
	if err := node.TransferLeadership(); err != nil {
		log.Debug().Err(err).Msg("leadership transfer skipped")
	} else {
		log.Info().Str("component", "raft").Msg("leadership transferred")
	}

	// 4. Stop Raft apply loop
	close(stopApply)

	log.Info().Str("component", "server").Msg("server stopped")
	return nil
}
