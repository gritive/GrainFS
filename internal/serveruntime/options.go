package serveruntime

import (
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/resourceguard"
)

// ServeOptions is the cobra-free input to RunFromOptions. Every cmd flag
// maps to exactly one field. The struct is flat (no embedded *cobra.Command,
// no embedded io.Reader/io.Writer beyond explicit Stdout/Stderr) so tests
// can construct it with a plain struct literal.
type ServeOptions struct {
	// Version is the build version string. cmd passes `version` global.
	Version string

	// --- Listen + addressing ---
	DataDir     string   // --data (raw string)
	DataDirs    []string // --data (parsed multi-paths)
	MetaDir     string   // --meta-dir
	Port        int      // --port
	AdminSocket string   // --admin-socket
	AdminGroup  string   // --admin-group
	PublicURL   string   // --public-url

	// --- Cluster identity ---
	NodeID            string // --node-id
	RaftAddr          string // --raft-addr
	JoinListenAddr    string // --join-listen-addr
	ClusterKey        string // --cluster-key
	EncryptionKeyFile string // --encryption-key-file

	// --- Cluster transport tuning ---
	AppendForwardBufferTotalBytes    int64         // --cluster-append-forward-buffer-total-bytes
	AppendForwardBufferMaxPerRequest int64         // --cluster-append-forward-buffer-max-per-request
	AppendSizeCapBytes               int64         // --append-size-cap-bytes
	QUICMuxPoolSize                  int           // --quic-mux-pool
	QUICMuxFlushWindow               time.Duration // --quic-mux-flush

	// --- Storage knobs ---
	PackThreshold      int   // --pack-threshold
	ShardPackThreshold int   // --shard-pack-threshold
	DirectIO           bool  // --direct-io
	MeasureReadAmp     bool  // --measure-read-amp
	BlockCacheSize     int64 // --block-cache-size
	ShardCacheSize     int64 // --shard-cache-size

	// --- Protocols ---
	NFS4Port           int           // --nfs4-port
	NFSWriteBufferDir  string        // --nfs-write-buffer-dir
	NFSWriteBufferIdle time.Duration // --nfs-write-buffer-idle
	NBDPort            int           // --nbd-port
	P9Bind             string        // --9p-bind
	P9Port             int           // --9p-port

	// --- Intervals ---
	ScrubInterval            time.Duration // --scrub-interval
	ScrubOrphanAge           time.Duration // --scrub-orphan-age
	SegmentGCRetention       time.Duration // --segment-gc-retention
	ReshardInterval          time.Duration // --reshard-interval
	DataGroupRefreshInterval time.Duration // --datagroup-refresh-interval
	DegradedInterval         time.Duration // --degraded-check-interval
	LifecycleInterval        time.Duration // --lifecycle-interval
	RaftLogGCInterval        time.Duration // --raft-log-gc-interval
	RaftHeartbeatInterval    time.Duration // --raft-heartbeat-interval
	RaftElectionTimeout      time.Duration // --raft-election-timeout

	// --- Heal Receipt ---
	HealReceiptEnabled        bool          // --heal-receipt-enabled
	HealReceiptPSK            string        // --heal-receipt-psk
	HealReceiptRetention      time.Duration // --heal-receipt-retention
	HealReceiptGossipInterval time.Duration // --heal-receipt-gossip-interval
	HealReceiptWindow         int           // --heal-receipt-window

	// --- Audit ---
	AuditIceberg        bool          // --audit-iceberg
	AuditCommitInterval time.Duration // --audit-commit-interval

	// --- Observability ---
	OTelEndpoint   string  // --otel-endpoint
	OTelSampleRate float64 // --otel-sample-rate
	PprofPort      int     // --pprof-port

	// --- Resource guards (already cobra-free structs) ---
	FDWatchEnabled        bool                    // --fd-watch-enabled
	FDOpts                resourceguard.FDOptions // --fd-watch-interval / threshold / eta / recovery / classification-cap
	GoroutineWatchEnabled bool                    // --goroutine-watch-enabled
	GoroutineOpts         resourceguard.GoroutineOptions
	VlogWatchEnabled      bool // --vlog-watch-enabled
	VlogOpts              resourceguard.VlogOptions

	// --- Misc / hidden / deprecated ---
	BadgerValueThreshold int64 // --badger-value-threshold (hidden, test-only)
	StrictVlogRegistry   bool  // --strict-vlog-registry
	VlogSmokeDefer       time.Duration

	// --- Snapshot for structured logs ---
	// Captured at cmd layer where cobra is available; passed through.
	FlagsSnapshot map[string]string

	// --- Test seams ---
	// Stdout/Stderr can be overridden; default to os.Stdout/os.Stderr inside
	// RunFromOptions if nil.
	Stdout io.Writer
	Stderr io.Writer
}
