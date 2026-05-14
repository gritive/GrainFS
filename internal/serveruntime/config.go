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
// needs. The cmd/grainfs/buildClusterConfig captures this from cobra so Run
// itself reads no cobra state.
type Config struct {
	// Build/runtime identity
	Version string

	// Network / identity
	Addr             string
	DataDir          string
	NodeID           string
	RaftAddr         string
	RaftAddrExplicit bool
	ClusterKey       string

	// Pre-built per Q9 of the cmd-thin grill
	AuthOpts  []server.Option
	Encryptor *encrypt.Encryptor

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
	DirectIO       bool
	MeasureReadAmp bool
	ShardCacheSize int64
	PackThreshold  int

	// Heal receipts
	HealReceiptEnabled        bool
	HealReceiptPSK            string
	HealReceiptRetention      time.Duration
	HealReceiptGossipInterval time.Duration
	HealReceiptWindow         int

	// Lifecycle / dedup / cache
	LifecycleInterval time.Duration
	MigrationInterval time.Duration
	DedupEnabled      bool
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
	RingReshardInterval      time.Duration
	DataGroupRefreshInterval time.Duration
	DegradedInterval         time.Duration

	// Node services
	NFS4Port int
	NBDPort  int
	P9Port   int

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
