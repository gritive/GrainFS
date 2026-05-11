package serveruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/transport"
)

// bootValidateConfig validates flag combinations, resolves nodeID, computes
// clusterMode, defaults raftAddr for solo mode, and stages metaDir/raftDir
// paths. No I/O on DB or network; the only side effect is GenerateNodeID
// writing the node-id file when nodeID is empty.
func bootValidateConfig(state *bootState) error {
	cfg := state.cfg
	state.nodeID = cfg.NodeID
	state.raftAddr = cfg.RaftAddr
	state.peers = cfg.Peers

	if state.nodeID == "" {
		var err error
		state.nodeID, err = GenerateNodeID(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("generate node ID: %w", err)
		}
		log.Info().Str("component", "server").Str("node_id", state.nodeID).Msg("auto-generated node ID")
	}

	// D6/D7: --cluster-key is required when running in actual cluster mode
	// (peers > 0 || join != ""). Solo runs through this same function but
	// does not require a cluster key — Run handles both modes.
	state.clusterMode = len(state.peers) > 0 || cfg.JoinMode
	if state.clusterMode {
		if err := transport.ValidateClusterKey(cfg.ClusterKey); err != nil {
			if errors.Is(err, transport.ErrEmptyClusterKey) {
				return fmt.Errorf("--cluster-key is required in cluster mode (generate with: openssl rand -hex 32)")
			}
			log.Warn().Err(err).Msg("--cluster-key is below recommended length")
		}
	}
	if cfg.JoinMode {
		if len(state.peers) > 0 {
			return fmt.Errorf("--join cannot be used with --peers")
		}
		if state.raftAddr == "" {
			return fmt.Errorf("--raft-addr is required when --join is set")
		}
		state.peers = []string{cfg.JoinAddr}
	}

	// When no peers are configured, we boot a singleton Raft node on a
	// loopback port so a single-machine deployment still goes through the
	// unified storage path (versioning, scrubber, lifecycle, WAL all work).
	// Operators who later want to expand the cluster pick a concrete
	// --raft-addr and --peers list; the loopback default is only for the
	// "just start it" path.
	if state.raftAddr == "" {
		if len(state.peers) > 0 {
			return fmt.Errorf("--raft-addr is required when --peers is set")
		}
		// Singleton: let the kernel pick a free port so multiple instances
		// (dev, tests) coexist without collisions. No peer will ever reach it.
		state.raftAddr = "127.0.0.1:0"
	}

	state.metaDir = filepath.Join(cfg.DataDir, "meta")
	state.raftDir = filepath.Join(cfg.DataDir, "raft")
	state.roleRegistry = badgerrole.DefaultRegistry()
	state.startupDecisions = make([]badgerrole.Decision, 0, 8)

	return nil
}

// bootAutoMigrate runs cluster.MigrateLegacyMetaToCluster when the layout
// indicates a pre-cluster local meta DB is present and the raft directory
// has not yet been created. Reads disk only; on a fresh dataDir or already-
// migrated layout it is a no-op.
//
// Must run BEFORE any filesystem or lock side effects (MkdirAll on metaDir,
// badger.Open). Two failure modes that drove this ordering:
//  1. MkdirAll ran before this check, so os.Stat(metaDir) succeeded on a
//     freshly-created empty dir and triggered a spurious migration.
//  2. The migration opens the meta DB, but if Run had already opened it,
//     BadgerDB's exclusive directory lock would abort the migration with
//     "Another process is using this Badger database".
func bootAutoMigrate(state *bootState) error {
	if _, err := os.Stat(state.raftDir); !os.IsNotExist(err) {
		return nil
	}
	info, err := os.Stat(state.metaDir)
	if err != nil || !info.IsDir() {
		return nil
	}
	// A populated local meta dir has .sst / .vlog / MANIFEST files.
	// Distinguish "real data" from "empty dir someone pre-created" by
	// checking for any entries; empty → skip migration.
	entries, err := os.ReadDir(state.metaDir)
	if err != nil || len(entries) == 0 {
		return nil
	}
	log.Info().Str("component", "migrate").Msg("auto-migrating local metadata to cluster format")
	if err := cluster.MigrateLegacyMetaToCluster(state.cfg.DataDir, state.nodeID); err != nil {
		return fmt.Errorf("auto-migrate: %w", err)
	}
	log.Info().Str("component", "migrate").Msg("auto-migration complete")
	return nil
}

// bootOpenMetaDB ensures metaDir exists, opens the meta BadgerDB, registers
// it with resourcewatch, and runs the writability preflight. Registers two
// cleanups: db.Close (LIFO outermost) and resourcewatch.DeregisterDB. On
// preflight rejection returns the PreflightBadger error so cmd/serve can
// surface a structured operator message.
func bootOpenMetaDB(state *bootState) error {
	if err := os.MkdirAll(state.metaDir, 0o755); err != nil {
		return fmt.Errorf("create meta dir at %s: %w\n  recovery: check that the parent directory exists and the user has write permission", state.metaDir, err)
	}
	db, err := badger.Open(badgerutil.SmallOptions(state.metaDir))
	if err != nil {
		return fmt.Errorf("open metadata db at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK), see README#badger-troubleshooting", state.metaDir, err, state.metaDir)
	}
	state.db = db
	state.AddCleanup(func() { db.Close() })
	metaVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryMeta, db)
	state.AddCleanup(func() { resourcewatch.DeregisterDB(metaVlogEntry) })

	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	decision := badgerrole.ProbeWritable(db, badgerrole.RoleMeta, "", state.metaDir)
	state.startupDecisions = append(state.startupDecisions, decision)
	if decision.Status != badgerrole.DecisionOK {
		return server.PreflightBadger(db, state.metaDir, nil)
	}
	return nil
}

// bootValidateTimings cross-validates raft timing flags. Pure config check;
// runs after MetaDB only because the original ordering was that way (early
// MetaDB failure surfaces a clearer recovery message than a flag rejection).
func bootValidateTimings(state *bootState) error {
	cfg := state.cfg
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
	return nil
}

// bootOpenRaftLogStore opens the raft log BadgerDB. Registers logStore.Close
// as a cleanup. Also registers a resourcewatch entry for the underlying
// raft-log Badger when the store owns it (vs the shared variant where the
// shared DB phase owns the registration).
func bootOpenRaftLogStore(state *bootState) error {
	cfg := state.cfg
	if cfg.BadgerManagedMode {
		state.storeOpts = append(state.storeOpts, raft.WithManagedMode())
	}
	logStore, err := raft.NewBadgerLogStore(state.raftDir, state.storeOpts...)
	if err != nil {
		return fmt.Errorf("open raft store at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK)", state.raftDir, err, state.raftDir)
	}
	state.logStore = logStore
	state.AddCleanup(func() { logStore.Close() })
	if !logStore.IsShared() && logStore.DB() != nil {
		raftLogVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, logStore.DB())
		state.AddCleanup(func() { resourcewatch.DeregisterDB(raftLogVlogEntry) })
	}
	state.startupDecisions = append(state.startupDecisions, badgerrole.Decision{
		Role:   badgerrole.RoleMetaRaftLog,
		Path:   state.raftDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})
	return nil
}

// bootOpenSharedRaftLogDB opens the per-node shared raft-log BadgerDB at
// <dataDir>/shared-raft-log/. Every data group's Raft log is a key-prefixed
// view of this one DB (C2 P0b — see docs/architecture/badger-consolidation.md).
// Registers the DB's Close as a cleanup and a resourcewatch entry.
func bootOpenSharedRaftLogDB(state *bootState) error {
	cfg := state.cfg
	sharedDir := filepath.Join(cfg.DataDir, "shared-raft-log")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		return fmt.Errorf("mkdir shared raft-log dir: %w", err)
	}
	sharedDB, err := badger.Open(badgerutil.RaftLogOptions(sharedDir, true))
	if err != nil {
		return fmt.Errorf("open shared raft-log badger at %s: %w", sharedDir, err)
	}
	state.sharedRaftLogDB = sharedDB
	state.AddCleanup(func() { sharedDB.Close() })
	sharedVlog := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedRaftLog, sharedDB)
	state.AddCleanup(func() { resourcewatch.DeregisterDB(sharedVlog) })
	state.startupDecisions = append(state.startupDecisions, badgerrole.Decision{
		Role:   badgerrole.RoleSharedRaftLog,
		Path:   sharedDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})
	log.Info().Str("dir", sharedDir).Msg("shared raft-log DB opened")
	return nil
}

// bootOpenSharedFSMDB opens the per-node shared FSM-state BadgerDB at
// <dataDir>/shared-fsm/. Each data group's metadata state is a key-prefixed
// view of this one DB (C2 P3 — see docs/architecture/badger-consolidation.md).
// Any legacy per-group <dataDir>/groups/*/badger/ dir from a pre-P3 deployment
// is IGNORED — pre-1.0, no migration. Registers the DB's Close as a cleanup
// and a resourcewatch entry.
func bootOpenSharedFSMDB(state *bootState) error {
	cfg := state.cfg
	sharedDir := filepath.Join(cfg.DataDir, "shared-fsm")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		return fmt.Errorf("mkdir shared FSM-state dir: %w", err)
	}
	sharedDB, err := badger.Open(badgerutil.SmallOptions(sharedDir))
	if err != nil {
		return fmt.Errorf("open shared FSM-state badger at %s: %w", sharedDir, err)
	}
	state.sharedFSMDB = sharedDB
	state.AddCleanup(func() { sharedDB.Close() })
	sharedVlog := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedFSM, sharedDB)
	state.AddCleanup(func() { resourcewatch.DeregisterDB(sharedVlog) })
	state.startupDecisions = append(state.startupDecisions, badgerrole.Decision{
		Role:   badgerrole.RoleSharedFSM,
		Path:   sharedDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})
	log.Info().Str("dir", sharedDir).Msg("shared FSM-state DB opened")
	return nil
}
