package serveruntime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/server"
)

// Config is the flat snapshot of every flag and pre-resolved input that Run
// needs. Run itself reads no cobra state; callers materialize Config via
// optionsToConfig (or RunFromOptions, which wraps it).
type Config struct {
	// Build/runtime identity
	Version string

	// Network / identity
	Addr             string
	DataDir          string
	DataDirs         []string
	MetaDir          string
	NodeID           string
	RaftAddr         string
	RaftAddrExplicit bool
	ClusterKey       string
	// JoinListenAddr is the bind address for the Zero-CA QUIC join listener
	// (two-phase invite handler, leader side). Empty derives a kernel-picked
	// port on the raft-addr host; the resolved address is advertised in the
	// invite bundle (W10).
	JoinListenAddr string

	// Pre-built per Q9 of the cmd-thin grill
	AuthOpts  []server.Option
	Encryptor *encrypt.Encryptor
	// RawEncryptionKey is the raw bytes of the static encryption.key, captured
	// alongside Encryptor by LoadOrCreateEncryptionKeyWithRaw. Threaded onto
	// bootState (via cfg) so the zero-CA bootstrap-secret provider can seal the
	// encryption key to an invite-joining node. Nil when encryption is unwired.
	RawEncryptionKey []byte

	// InviteJoin carries the Zero-CA invite-join Phase-1 outcome (W9b) when this
	// node booted from an invite bundle (FreshJoin/Resume). Nil on every other
	// boot. Threaded onto bootState by newBootState so the raft gating, the
	// isGenesisBoot decision, and the post-boot Phase-2 ACK can see it.
	InviteJoin *inviteJoinState

	// IAM (Phase 2): store + applier for cluster IAM state. Both nil in
	// fully unwired (test/legacy) configurations; cmd/grainfs/serve.go
	// always provides them in production.
	IAMStore   *iam.Store
	IAMApplier *iam.Applier

	// Raft tuning
	RaftLogGCInterval     time.Duration
	RaftHeartbeatInterval time.Duration
	RaftElectionTimeout   time.Duration
	QUICMuxEnabled        bool
	QUICMuxPoolSize       int
	QUICMuxFlushWindow    time.Duration

	// Storage / EC
	DirectIO           bool
	MeasureReadAmp     bool
	ShardCacheSize     int64
	PackThreshold      int
	ShardPackThreshold int

	// Heal receipts
	HealReceiptEnabled        bool
	HealReceiptPSK            string
	HealReceiptRetention      time.Duration
	HealReceiptGossipInterval time.Duration
	HealReceiptWindow         int

	// AppendObject forward buffer pool
	AppendForwardBufferTotalBytes    int64
	AppendForwardBufferMaxPerRequest int64

	// AppendObject size cap
	AppendSizeCapBytes int64

	// Lifecycle / cache
	LifecycleInterval time.Duration
	MigrationInterval time.Duration
	BlockCacheSize    int64

	// Dashboard / vlog
	PublicURL         string
	VlogWarnRatio     float64
	VlogCriticalRatio float64

	// Admin socket
	AdminSocket string
	AdminGroup  string

	// Scrub / reshard / degraded
	ScrubInterval            time.Duration
	ReshardInterval          time.Duration
	DataGroupRefreshInterval time.Duration
	DegradedInterval         time.Duration
	// ScrubOrphanAge is the minimum filesystem mtime age before an orphan raw
	// segment is eligible for sweep. Default 5m.
	ScrubOrphanAge time.Duration
	// SegmentGCRetention is the grace period after a raw segment blob becomes
	// unreferenced before the scrubber may physically delete it. Protects
	// in-flight reads and recent-PITR margin. 0 disables retention gating
	// (age-gate only). Default 24h (matches snapshot retention 1h x 24).
	SegmentGCRetention time.Duration

	// Audit Iceberg log lake
	AuditIceberg        bool
	AuditCommitInterval time.Duration

	// Node services
	NFS4Port           int
	NFSWriteBufferDir  string
	NFSWriteBufferIdle time.Duration
	NBDPort            int
	P9Bind             string
	P9Port             int

	// Resource guards (pre-resolved from cobra; Run body reads only these)
	FDWatchEnabled        bool
	FDOpts                resourceguard.FDOptions
	GoroutineWatchEnabled bool
	GoroutineOpts         resourceguard.GoroutineOptions
	VlogWatchEnabled      bool
	VlogResourceGuardOpts resourceguard.VlogOptions

	// Startup snapshot map (built once via cmd.Flags().VisitAll with secrets redacted)
	FlagsSnapshot map[string]string
}

// LogStartupConfigSnapshot writes the structured startup-config debug log and
// the on-disk .last-config.json snapshot. It accepts a pre-redacted snapshot
// map (built upstream by cmd/grainfs/collectFlagsSnapshot) plus the
// resolved-at-runtime fields (addr/dataDir/nodeID/raftAddr) that aren't
// cobra-derived.
func LogStartupConfigSnapshot(
	flagsSnap map[string]string,
	addr, dataDir, nodeID, raftAddr string,
) {
	snapshot := make(map[string]any, len(flagsSnap)+4)
	snapshot["addr"] = addr
	snapshot["data_dir"] = dataDir
	snapshot["node_id"] = nodeID
	snapshot["raft_addr"] = raftAddr
	for k, v := range flagsSnap {
		snapshot[k] = v
	}

	log.Debug().Interface("flags", snapshot).Msg("startup config snapshot")

	snapPath := filepath.Join(dataDir, ".last-config.json")
	if prev, err := os.ReadFile(snapPath); err == nil {
		var prevMap map[string]any
		if err := json.Unmarshal(prev, &prevMap); err == nil {
			diff := diffSnapshots(prevMap, snapshot)
			if len(diff) > 0 {
				log.Info().Interface("changed", diff).Msg("config changed since last startup")
			}
		}
	}

	if data, err := json.MarshalIndent(snapshot, "", "  "); err == nil {
		if err := os.WriteFile(snapPath, data, 0o600); err != nil {
			log.Debug().Err(err).Str("path", snapPath).Msg("could not persist startup config snapshot")
		}
	}
}

func diffSnapshots(prev, curr map[string]any) map[string]map[string]any {
	out := make(map[string]map[string]any)
	keys := make(map[string]struct{}, len(prev)+len(curr))
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range curr {
		keys[k] = struct{}{}
	}
	sortedKeys := make([]string, 0, len(keys))
	for k := range keys {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		pv, pok := prev[k]
		cv, cok := curr[k]
		if !pok || !cok || fmt.Sprintf("%v", pv) != fmt.Sprintf("%v", cv) {
			out[k] = map[string]any{"prev": pv, "curr": cv}
		}
	}
	return out
}
