package serveruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/transport"
)

// JoinPendingFile is the sentinel written by the UDS join handler and read at
// startup to perform a pending cluster join. Its presence means the solo Raft
// state must be wiped so the node can bootstrap cleanly in join mode.
// Exported so tests in cmd/grainfs can write it without duplicating the path.
const JoinPendingFile = ".join-pending"

// wipeSoloRaftState renames meta_raft/, raft/, and shared-raft-log/ to
// *.pre-join-backup/ so the join-mode boot starts with no existing Raft state.
// Backups are removed by Run after a successful join. On failure the caller
// should not proceed with join mode.
func wipeSoloRaftState(dataDir string) error {
	for _, dir := range []string{"meta_raft", "raft", "shared-raft-log"} {
		src := filepath.Join(dataDir, dir)
		dst := src + ".pre-join-backup"
		if _, err := os.Stat(src); os.IsNotExist(err) {
			continue // nothing to back up
		}
		if _, err := os.Stat(dst); err == nil {
			// Backup from a previous failed join attempt exists; overwrite it
			// only now that we have a fresh src to replace it with.
			log.Warn().Str("backup", dst).Msg("overwriting pre-join backup from prior attempt")
			_ = os.RemoveAll(dst)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("backup %s: %w", dir, err)
		}
	}
	return nil
}

// bootValidateConfig validates flag combinations, resolves nodeID, computes
// clusterMode (always true — --cluster-key required in all modes), defaults
// raftAddr for solo mode, and stages metaDir/raftDir paths. No I/O on DB or
// network; the only side effect is GenerateNodeID writing the node-id file
// when nodeID is empty.
func bootValidateConfig(state *bootState) error {
	cfg := state.cfg
	state.nodeID = cfg.NodeID
	state.raftAddr = cfg.RaftAddr

	if state.nodeID == "" {
		var err error
		state.nodeID, err = GenerateNodeID(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("generate node ID: %w", err)
		}
		log.Info().Str("component", "server").Str("node_id", state.nodeID).Msg("auto-generated node ID")
	}

	// File-based join detection: written by `grainfs join` UDS handler.
	// Takes precedence over any other bootstrap logic.
	pendingFile := filepath.Join(cfg.DataDir, JoinPendingFile)
	if rawPeer, err := os.ReadFile(pendingFile); err == nil {
		peerAddr := strings.TrimSpace(string(rawPeer))
		if peerAddr != "" {
			if err := wipeSoloRaftState(cfg.DataDir); err != nil {
				return fmt.Errorf("wipe solo state for join: %w", err)
			}
			state.joinAddr = peerAddr
			state.joinMode = true
			state.peers = []string{peerAddr}
			log.Info().Str("peer", peerAddr).Msg("join-pending: entering join mode")
		}
	}

	// clusterMode is always true: --cluster-key is required in all modes.
	state.clusterMode = true
	if err := transport.ValidateClusterKey(cfg.ClusterKey); err != nil {
		if errors.Is(err, transport.ErrEmptyClusterKey) {
			return fmt.Errorf("--cluster-key is required (generate with: openssl rand -hex 32)")
		}
		log.Warn().Err(err).Msg("--cluster-key is below recommended length")
	}

	// Solo mode: let the kernel pick a free loopback port so multiple instances
	// (dev, tests) coexist without collisions.
	if state.raftAddr == "" {
		if state.joinMode {
			return fmt.Errorf("--raft-addr is required in join mode")
		}
		state.raftAddr = "127.0.0.1:0"
	}

	state.metaDir = cfg.MetaDir
	if state.metaDir == "" {
		state.metaDir = filepath.Join(cfg.DataDir, "meta")
	}
	state.raftDir = filepath.Join(cfg.DataDir, "raft")
	// Capture prior-state signal BEFORE any DB phase (bootOpenMetaDB)
	// has a chance to populate <dataDir>/meta. True if either meta or
	// raft dir is non-empty on entry → restart of an existing node.
	// Consumed by wireDEKKeeper to refuse silent auto-regeneration.
	state.priorState = dirHasContent(state.metaDir) || dirHasContent(state.raftDir)
	state.bootID = uuid.NewString()
	state.roleRegistry = badgerrole.DefaultRegistry()
	state.startupDecisions = make([]badgerrole.Decision, 0, 8)
	state.recoveryJournal = badgerrole.NewJournalWriter(badgerrole.JournalOptions{
		DataDir:       cfg.DataDir,
		NodeID:        state.nodeID,
		BootID:        state.bootID,
		BinaryVersion: cfg.Version,
	})

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
		recordBadgerStartupDecision(state, badgerrole.Decision{
			Role:   badgerrole.RoleMeta,
			Path:   state.metaDir,
			Status: badgerrole.DecisionOpenFailed,
			Action: badgerrole.RecoveryActionBlockStart,
			Reason: err.Error(),
			Err:    err,
		})
		return fmt.Errorf("create meta dir at %s: %w\n  recovery: check that the parent directory exists and the user has write permission", state.metaDir, err)
	}
	db, err := badger.Open(badgerutil.SmallOptions(state.metaDir))
	if err != nil {
		recordBadgerStartupDecision(state, badgerrole.Decision{
			Role:   badgerrole.RoleMeta,
			Path:   state.metaDir,
			Status: badgerrole.DecisionOpenFailed,
			Action: badgerrole.RecoveryActionBlockStart,
			Reason: err.Error(),
			Err:    err,
		})
		return fmt.Errorf("open metadata db at %s: %w\n  recovery: check disk free space, confirm no other grainfs process holds the lock (lsof %s/LOCK), see README#badger-troubleshooting", state.metaDir, err, state.metaDir)
	}
	state.db = db
	state.AddCleanup(func() { db.Close() })
	metaVlogEntry := resourcewatch.RegisterDB(resourcewatch.DBCategoryMeta, db)
	state.AddCleanup(func() { resourcewatch.DeregisterDB(metaVlogEntry) })

	// Phase 16 Week 3: cluster mode preflight. Same reasoning as local.
	decision := badgerrole.ProbeWritable(db, badgerrole.RoleMeta, "", state.metaDir)
	recordBadgerStartupDecision(state, decision)
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
		recordBadgerStartupDecision(state, badgerrole.Decision{
			Role:   badgerrole.RoleSharedFSM,
			Path:   sharedDir,
			Status: badgerrole.DecisionOpenFailed,
			Action: badgerrole.RecoveryActionBlockStart,
			Reason: err.Error(),
			Err:    err,
		})
		return fmt.Errorf("mkdir shared FSM-state dir: %w", err)
	}
	sharedDB, err := badger.Open(badgerutil.SmallOptions(sharedDir))
	if err != nil {
		recordBadgerStartupDecision(state, badgerrole.Decision{
			Role:   badgerrole.RoleSharedFSM,
			Path:   sharedDir,
			Status: badgerrole.DecisionOpenFailed,
			Action: badgerrole.RecoveryActionBlockStart,
			Reason: err.Error(),
			Err:    err,
		})
		return fmt.Errorf("open shared FSM-state badger at %s: %w", sharedDir, err)
	}
	state.sharedFSMDB = sharedDB
	state.AddCleanup(func() { sharedDB.Close() })
	sharedVlog := resourcewatch.RegisterDB(resourcewatch.DBCategorySharedFSM, sharedDB)
	state.AddCleanup(func() { resourcewatch.DeregisterDB(sharedVlog) })
	recordBadgerStartupDecision(state, badgerrole.Decision{
		Role:   badgerrole.RoleSharedFSM,
		Path:   sharedDir,
		Status: badgerrole.DecisionOK,
		Action: badgerrole.RecoveryActionNone,
	})
	log.Info().Str("dir", sharedDir).Msg("shared FSM-state DB opened")
	return nil
}

// dirHasContent reports whether dir exists and contains at least one entry.
// Returns false on any error (missing dir, permission denied, etc.) — used
// as a signal that the directory holds data from a prior boot, so a
// permission error conservatively means "no prior state" (the boot would
// fail downstream on the real open anyway).
func dirHasContent(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	return len(entries) > 0
}
