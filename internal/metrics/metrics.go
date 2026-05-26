package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts HTTP requests by method and status.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"method", "status"})

	// HTTPRequestDuration measures HTTP request duration by method.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	// ECEncodeDuration measures erasure coding encode duration.
	ECEncodeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_ec_encode_duration_seconds",
		Help:    "Erasure coding encode duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// ECDecodeDuration measures erasure coding decode duration.
	ECDecodeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_ec_decode_duration_seconds",
		Help:    "Erasure coding decode duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// ObjectPutStageDuration measures fixed-stage PUT latency in the storage
	// path. Labels are deliberately low-cardinality: path and stage only.
	ObjectPutStageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_object_put_stage_duration_seconds",
		Help:    "Duration of fixed stages in the object PUT path.",
		Buckets: prometheus.DefBuckets,
	}, []string{"path", "stage"})

	// ChunkFanoutBreadth measures how many distinct placement groups a
	// successful chunked PUT touched.
	ChunkFanoutBreadth = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_chunk_fanout_breadth",
		Help:    "Number of distinct placement groups touched by a chunked PUT.",
		Buckets: []float64{1, 2, 3, 4, 6, 8, 12, 16},
	})

	// StorageBytesTotal tracks total bytes stored.
	StorageBytesTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_storage_bytes_total",
		Help: "Total bytes stored.",
	})

	// BucketsTotal tracks total number of buckets.
	BucketsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_buckets_total",
		Help: "Total number of buckets.",
	})

	// ObjectsTotal tracks total number of objects.
	ObjectsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_objects_total",
		Help: "Total number of objects.",
	})

	// EventQueueDropsTotal counts events dropped because the bounded event
	// queue was full. Non-zero values indicate BadgerDB write latency is
	// exceeding event production rate; investigate before event log gaps
	// become user-visible.
	EventQueueDropsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_event_queue_drops_total",
		Help: "Total number of events dropped because the in-flight event queue was full.",
	})

	// CacheInvalidationTotal counts cache invalidation operations.
	CacheInvalidationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_cache_invalidation_total",
		Help: "Total number of cache invalidation operations.",
	}, []string{"bucket", "protocol"}) // protocol: vfs, nfs, cached_backend

	// CacheInvalidationDuration measures cache invalidation operation duration.
	CacheInvalidationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_cache_invalidation_duration_seconds",
		Help:    "Cache invalidation operation duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	// CacheStatHits counts cache hits in stat cache.
	CacheStatHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cache_stat_hits_total",
		Help: "Total number of cache hits in stat cache.",
	})

	// CacheStatMisses counts cache misses in stat cache.
	CacheStatMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cache_stat_misses_total",
		Help: "Total number of cache misses in stat cache.",
	})

	// DeletedMarkersTotal tracks current number of deleted file markers.
	DeletedMarkersTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_deleted_markers_total",
		Help: "Current number of deleted file markers in memory.",
	})

	// RegistrySize tracks current number of registered cache invalidators.
	RegistrySize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cache_registry_size",
		Help: "Current number of registered cache invalidators.",
	})

	// NFSv4BufferPoolGets tracks total buffer pool get operations by buffer size.
	NFSv4BufferPoolGets = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_nfsv4_buffer_pool_gets_total",
		Help: "Total number of buffer pool get operations.",
	}, []string{"size"})

	// NFSv4BufferPoolMisses tracks buffer pool misses (fallback allocations).
	NFSv4BufferPoolMisses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_nfsv4_buffer_pool_misses_total",
		Help: "Total number of buffer pool misses (temporary allocations).",
	}, []string{"size"})

	// NFSv4BufferSizeInUse tracks current buffer size in use by pool type.
	NFSv4BufferSizeInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_nfsv4_buffer_size_bytes",
		Help: "Current buffer size in use by pool type.",
	}, []string{"size"})

	// SplitBrainSuspected is 1 when split brain is detected in the cluster, 0 otherwise.
	SplitBrainSuspected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_split_brain_suspected",
		Help: "1 if split brain is suspected (multiple leaders or large term divergence), 0 otherwise.",
	})

	// DataWALStartupRepairDiscovered counts metadata-only data WAL shard repair
	// candidates discovered and queued during startup replay, per record before
	// deduplication. Only counted when a repair sink is configured.
	DataWALStartupRepairDiscovered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_discovered_total",
		Help: "Metadata-only data WAL shard repair candidates discovered and queued during startup replay (only counted when a repair sink is configured); counted per WAL record before deduplication.",
	}, []string{"reason"})

	// ScrubShardErrorsTotal counts shard errors (missing + corrupt) detected during scrubbing.
	ScrubShardErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_shard_errors_total",
		Help: "Total shard errors detected during scrubbing.",
	})

	// ScrubRepairedTotal counts objects successfully repaired by the scrubber.
	ScrubRepairedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_repaired_total",
		Help: "Total objects repaired by the scrubber.",
	})

	// ECDegradedTotal counts EC objects that could not be repaired (too many shards lost).
	ECDegradedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_ec_degraded_total",
		Help: "Total EC objects that could not be repaired.",
	})

	// ScrubObjectsCheckedTotal counts objects checked by the scrubber.
	ScrubObjectsCheckedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_objects_checked_total",
		Help: "Total objects checked by the scrubber.",
	})

	// ScrubSkippedOverCapTotal counts objects skipped because repair cap was reached.
	ScrubSkippedOverCapTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_skipped_over_cap_total",
		Help: "Total objects skipped because max_repairs_per_cycle was reached.",
	})

	// ScrubPlainMigratedTotal counts plain objects re-encoded to EC by the scrubber.
	ScrubPlainMigratedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_plain_migrated_total",
		Help: "Total plain objects re-encoded to EC by the scrubber.",
	})

	// ScrubPlainMigrateErrorTotal counts plain→EC migration errors.
	ScrubPlainMigrateErrorTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_plain_migrate_error_total",
		Help: "Total errors during plain→EC migration by the scrubber.",
	})

	// ScrubMigrationSkippedOverCapTotal counts plain objects skipped because migration cap was reached.
	ScrubMigrationSkippedOverCapTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_migration_skipped_over_cap_total",
		Help: "Total plain objects skipped because max_migrations_per_cycle was reached.",
	})

	// ECScrubUnverifiedShardsTotal counts EC shards that were readable but
	// could not be integrity-verified because they lack a CRC oracle.
	ECScrubUnverifiedShardsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_ec_scrub_unverified_shards_total",
		Help: "Total EC shards skipped by scrub because no integrity oracle was available.",
	}, []string{"reason"})

	// DiskUsedPct tracks local disk usage percentage per node.
	DiskUsedPct = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_disk_used_pct",
		Help: "Local disk usage percentage (0–100) as seen by each node.",
	}, []string{"node_id"})

	// FDOpen tracks current open file descriptors per node.
	FDOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_fd_open",
		Help: "Current number of open file descriptors for this process.",
	}, []string{"node_id"})

	// FDLimit tracks the current RLIMIT_NOFILE soft limit per node.
	FDLimit = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_fd_limit",
		Help: "Current process file descriptor soft limit.",
	}, []string{"node_id"})

	// FDUsedRatio tracks open file descriptors divided by the FD soft limit.
	FDUsedRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_fd_used_ratio",
		Help: "Current open file descriptor usage ratio, from 0 to 1.",
	}, []string{"node_id"})

	// FDETASeconds estimates time until an FD threshold is reached.
	FDETASeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_fd_eta_seconds",
		Help: "Estimated seconds until the named file descriptor threshold is reached; -1 means no positive trend.",
	}, []string{"node_id", "threshold"})

	// FDOpenByCategory tracks best-effort open FD classification per node.
	FDOpenByCategory = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_fd_open_by_category",
		Help: "Best-effort current open file descriptors by bounded category.",
	}, []string{"node_id", "category"})

	// GoroutineCount tracks runtime.NumGoroutine() per node.
	GoroutineCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_goroutine_count",
		Help: "Current number of running goroutines on this node.",
	}, []string{"node_id"})

	// GoroutineLimit tracks the configured goroutine critical threshold per node.
	GoroutineLimit = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_goroutine_limit",
		Help: "Configured critical goroutine count threshold (--goroutine-critical).",
	}, []string{"node_id"})

	// GoroutineUsedRatio tracks goroutine count divided by the critical limit.
	GoroutineUsedRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_goroutine_used_ratio",
		Help: "Current goroutine usage ratio (count / critical-limit), from 0 to 1+.",
	}, []string{"node_id"})

	// GoroutineETASeconds estimates time until a goroutine threshold is reached.
	GoroutineETASeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_goroutine_eta_seconds",
		Help: "Estimated seconds until the named goroutine threshold is reached; -1 means no positive trend.",
	}, []string{"node_id", "threshold"})

	// VlogBytes tracks aggregate vlog bytes across all registered BadgerDB instances.
	VlogBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_vlog_bytes",
		Help: "Aggregate BadgerDB vlog bytes across all registered DB instances.",
	}, []string{"node_id"})

	// VlogBytesByCategory exposes per-category vlog breakdown.
	VlogBytesByCategory = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_vlog_bytes_by_category",
		Help: "BadgerDB vlog bytes grouped by category (meta, group-raft, incident, ...).",
	}, []string{"node_id", "category"})

	// VlogLimitBytes is the configured ratio denominator (Bavail + vlog).
	VlogLimitBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_vlog_limit_bytes",
		Help: "vlog ratio denominator: Bavail (statfs) + current vlog bytes.",
	}, []string{"node_id"})

	// VlogUsedRatio is the watcher's primary fire signal (vlog / limit).
	VlogUsedRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_vlog_used_ratio",
		Help: "Current vlog usage ratio; warn fires above --vlog-warn-ratio.",
	}, []string{"node_id"})

	// VlogETASeconds estimates time until a vlog threshold is reached.
	VlogETASeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_vlog_eta_seconds",
		Help: "Estimated seconds until the named vlog threshold is reached; -1 means no positive trend.",
	}, []string{"node_id", "threshold"})

	// BadgerGCRunsTotal counts vlog GC tick invocations per DB category.
	BadgerGCRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_badger_gc_runs_total",
		Help: "Total RunValueLogGC invocations per category.",
	}, []string{"node_id", "category"})

	// BadgerGCFailuresTotal counts vlog GC tick failures (excludes ErrNoRewrite).
	BadgerGCFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_badger_gc_failures_total",
		Help: "Total RunValueLogGC failures per category (excludes ErrNoRewrite).",
	}, []string{"node_id", "category"})

	// BadgerGCConsecutiveFailures exposes the live retry counter per category.
	BadgerGCConsecutiveFailures = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_badger_gc_consecutive_failures",
		Help: "Current consecutive RunValueLogGC failures per category; resets on ErrNoRewrite.",
	}, []string{"node_id", "category"})

	// Balancer metrics.

	BalancerGossipTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_gossip_total",
		Help: "Total gossip broadcasts sent by this node.",
	})

	BalancerGossipErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_gossip_errors_total",
		Help: "Total gossip send failures.",
	})

	BalancerMigrationsProposedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_migrations_proposed_total",
		Help: "Total CmdMigrateShard proposals submitted to Raft.",
	})

	BalancerMigrationsDoneTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_migrations_done_total",
		Help: "Total migrations completed successfully.",
	})

	BalancerMigrationsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_migrations_failed_total",
		Help: "Total migrations that failed.",
	})

	BalancerImbalancePct = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_balancer_imbalance_pct",
		Help: "Current disk imbalance across cluster nodes (max - min used %).",
	})

	BalancerPendingTasks = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_balancer_pending_tasks",
		Help: "Number of pending-migration entries persisted in BadgerDB.",
	})

	BalancerLeaderTransfersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_leader_transfers_total",
		Help: "Total load-based Raft leader transfers initiated.",
	})

	BalancerShardWriteErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_shard_write_errors_total",
		Help: "Total shard write errors during migration copy phase.",
	})

	BalancerShardCopyDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_balancer_shard_copy_duration_seconds",
		Help:    "Duration of shard copy operations during migration.",
		Buckets: prometheus.DefBuckets,
	})

	BalancerGracePeriodActiveTicks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_grace_period_active_ticks_total",
		Help: "Total balancer tick evaluations where at least one peer was within the join grace period (imbalance trigger relaxed by 1.5×).",
	})

	BalancerCBOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_balancer_cb_open",
		Help: "1 when circuit breaker for a destination node is open (disk full), 0 otherwise.",
	}, []string{"node_id"})

	BalancerCBAllOpenTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_cb_all_open_total",
		Help: "Total ticks where all destination nodes had open circuit breakers (no proposal possible).",
	})

	BalancerShardWriteRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_balancer_shard_write_retries_total",
		Help: "Total shard write retries per node and shard index.",
	}, []string{"node_id", "shard_idx"})

	BalancerMigrationPendingTTLExpiredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_balancer_migration_pending_ttl_expired_total",
		Help: "Total pending migrations cancelled due to TTL expiry.",
	})

	// Orphan shard sweep metrics.

	OrphanShardsFoundTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_shards_found_total",
		Help: "Total orphan shard directories found during scrubbing.",
	})

	OrphanShardsDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_shards_deleted_total",
		Help: "Total orphan shard directories deleted by the scrubber.",
	})

	OrphanSweepCappedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_sweep_capped_total",
		Help: "Total orphan shards deferred because maxOrphansPerCycle was reached.",
	})

	OrphanSegmentsFoundTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_segments_found_total",
		Help: "Total orphan raw segment files newly tombstoned during scrubbing.",
	})

	OrphanSegmentsDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_segments_deleted_total",
		Help: "Total orphan raw segment files deleted by the scrubber.",
	})

	OrphanSegmentSweepCappedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_segment_sweep_capped_total",
		Help: "Total orphan segments deferred due to per-cycle cap.",
	})

	OrphanSegmentWalkErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_segment_walk_errors_total",
		Help: "Total filesystem errors during orphan segment walk (readdir/stat).",
	})

	OrphanSegmentDeleteErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_scrub_orphan_segment_delete_errors_total",
		Help: "Total delete failures during orphan segment sweep (excludes ENOENT).",
	})

	// Phase 16 — Self-healing metrics.

	HealEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_heal_events_total",
		Help: "Total HealEvents emitted, partitioned by phase and outcome.",
	}, []string{"phase", "outcome"})

	HealShardsRepairedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_heal_shards_repaired_total",
		Help: "Total EC shards reconstructed and rewritten by the scrubber.",
	})

	HealDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grainfs_heal_duration_ms",
		Help:    "Per-phase healing latency in milliseconds.",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
	}, []string{"phase"})

	HealStreamDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_heal_stream_dropped_events_total",
		Help: "HealEvents dropped because an SSE subscriber's buffer was full.",
	})

	// Phase 16 Week 4 — Degraded mode + alert delivery metrics.

	Degraded = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_degraded",
		Help: "1 when the cluster is currently in degraded mode (after hysteresis), 0 otherwise.",
	})

	AlertDeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_alert_delivery_attempts_total",
		Help: "Webhook delivery attempts, partitioned by outcome.",
	}, []string{"outcome"}) // outcome = success | failed | dedup_suppressed

	AlertDeliveryFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_alert_delivery_failed_total",
		Help: "Webhook deliveries that exhausted all retries.",
	})

	WebhookSignatureDecryptFailureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_webhook_signature_decrypt_failure_total",
		Help: "Webhook signing-secret decrypt failures, partitioned by alert dispatcher kind and error class. Indicates a stale wrapped-secret after cluster rotate-key; alerts continue to deliver unsigned.",
	}, []string{"alert_kind", "err_class"})

	// AlertDispatchDroppedTotal counts alerts dropped without delivery,
	// partitioned by alert_kind and reason (inbox_full|not_started|stopped).
	AlertDispatchDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_alerts_dispatcher_dropped_total",
		Help: "Count of alerts dropped without delivery, by reason (inbox_full|not_started|stopped).",
	}, []string{"alert_kind", "reason"})

	// Raft snapshot operator metrics.
	RaftSnapshotTriggerTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_raft_snapshot_trigger_total",
		Help: "Total operator-triggered Raft snapshots, partitioned by outcome.",
	}, []string{"outcome"})

	RaftSnapshotLastIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_raft_snapshot_last_index",
		Help: "Raft log index of the last successfully operator-triggered snapshot.",
	})

	RaftSnapshotLastSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_raft_snapshot_last_size_bytes",
		Help: "Size in bytes of the last successfully operator-triggered Raft snapshot.",
	})

	// PeerUnhealthy is 1 while a peer sits in PeerHealth's cooldown window
	// (recent transport or EC shard write failure), 0 otherwise. Operators alert
	// on `grainfs_peer_unhealthy > 0` to catch EC stripe degradation before
	// it affects durability.
	PeerUnhealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_peer_unhealthy",
		Help: "1 if a peer is currently in PeerHealth cooldown, 0 otherwise.",
	}, []string{"peer"})

	// FsmKeyspaceLeakTotal counts FSM-state keys observed without the expected
	// group prefix at a scoped strip-site. A non-zero value means a
	// prefix-scoping bug — the process also panics on the offending op.
	FsmKeyspaceLeakTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_fsm_keyspace_leak_total",
		Help: "FSM-state keys observed without the expected group prefix at a scoped strip-site (indicates a prefix-scoping bug; the op also panics).",
	})

	// Object tagging metrics.

	// ObjectTaggingRequests counts object tagging API requests by op and result.
	ObjectTaggingRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_object_tagging_requests_total",
		Help: "Object tagging API requests by op and result.",
	}, []string{"op", "result"})

	// ObjectTaggingValidationErrors counts validation rejections in the tagging surface.
	ObjectTaggingValidationErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_object_tagging_validation_errors_total",
		Help: "Validation rejections in the tagging surface.",
	}, []string{"reason"})

	// ObjectTagsPerObject observes tag count per PUT (header or body).
	ObjectTagsPerObject = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_object_tags_per_object",
		Help:    "Tag count per PUT (header or body).",
		Buckets: []float64{0, 1, 2, 3, 5, 10},
	})

	// Data WAL startup repair candidates, attempts, successes, failures, and skips.

	DataWALStartupRepairCandidates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_candidates_total",
		Help: "Distinct data WAL shard repair candidates queued for the startup worker after (bucket, shardKey, shardIdx) deduplication.",
	}, []string{"reason"})

	DataWALStartupRepairAttempts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_attempts_total",
		Help: "Startup data WAL EC shard repair attempts.",
	})

	DataWALStartupRepairSuccesses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_successes_total",
		Help: "Successful startup data WAL EC shard repairs.",
	})

	// DataWALStartupRepairFailures reason label values:
	//   context_canceled       — repair context was canceled
	//   insufficient_survivors — too few EC shards readable for reconstruction
	//   repair_failed          — other repair error
	//   panic                  — worker goroutine panicked
	DataWALStartupRepairFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_failures_total",
		Help: "Failed startup data WAL EC shard repairs.",
	}, []string{"reason"})

	// DataWALStartupRepairSkips reason label values:
	//   no_group              — bucket has no data group assignment
	//   no_backend            — data group has no distributed backend
	//   unsupported_shardkey  — segment or coalesced shard key form (placement in segment metadata, not yet supported)
	//   invalid_shard_key     — shard key is empty or shard index is negative
	//   placement_corrupt     — LookupObjectPlacement returned an error or inconsistent node count
	//   not_local_owner       — this node does not own the shard
	//   stale                 — placement record has no nodes (object version deleted)
	DataWALStartupRepairSkips = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_datawal_startup_repair_skips_total",
		Help: "Startup data WAL EC shard repair candidates skipped before repair.",
	}, []string{"reason"})
)
