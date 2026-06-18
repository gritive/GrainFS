package server

const (
	routePathS3Root   = "/"
	routePathS3Bucket = "/:bucket"
	routePathS3Object = "/:bucket/*key"

	routePathMetrics = "/metrics"
	routePathUI      = "/ui"
	routePathUIRoot  = "/ui/"

	routePrefixAPI           = "/api/"
	routePrefixAdmin         = "/admin/"
	routePrefixIceberg       = "/iceberg/"
	routePrefixIcebergAIStor = "/_iceberg/"

	// routePathOAuthTokenSuffix is the iceberg OAuth2 token endpoint suffix,
	// appended to each iceberg route prefix. The iceberg package holds its own
	// copy for route registration; core uses this for the route-surface manifest.
	routePathOAuthTokenSuffix = "v1/oauth/tokens"
	routePrefixUI             = "/ui/"

	routePathEvents            = "/api/events"
	routePathHealEventsStream  = "/api/events/heal/stream"
	routePathEventLog          = "/api/eventlog"
	routePathIncidents         = "/api/incidents"
	routePrefixIncidents       = "/api/incidents/"
	routePathClusterStatus     = "/api/cluster/status"
	routePathClusterPlacement  = "/api/cluster/placement"
	routePathClusterJoin       = "/api/cluster/join"
	routePathClusterRemovePeer = "/api/cluster/remove-peer"
	routePathClusterBalancer   = "/api/cluster/balancer/status"
	routePathLifecycleStatus   = "/api/cluster/lifecycle/status"
	// Test seams for e2e: drive the in-process lifecycle worker
	// deterministically across the HTTP boundary (the e2e test process
	// cannot reach the in-process Service handle of the binary it spawned).
	// SigV4-authenticated like the rest of /api/. Documented in TODOS.
	routePathLifecycleTestRunCycle = "/api/cluster/lifecycle/test/run-cycle"
	routePathLifecycleTestSetNow   = "/api/cluster/lifecycle/test/set-now"
	routePathAuditHealth           = "/api/audit/health"
	routePathAuditS3               = "/api/audit/s3"
	routePathCacheStatus           = "/api/cache/status"
	routePathConfig                = "/api/admin/config"
	routePathAlertsStatus          = "/api/admin/alerts/status"
	routePathAlertsResend          = "/api/admin/alerts/resend"
	routePathReceipts              = "/api/receipts"
	routePathReceiptByID           = "/api/receipts/:id"

	routePathAdminHealthBadger = "/admin/health/badger"
	routePathAdminHealthRaft   = "/admin/health/raft"
	routePathAdminHealthScrub  = "/admin/health/scrub"
	routePathAdminRaftSnapshot = "/admin/raft/snapshot"
	routePathAdminSnapshots    = "/admin/snapshots"
	routePathAdminDebug        = "/admin/debug"
	routePathAdminDebugVFSStat = "/vfs/stat"

	routePathSnapshotSeq        = "/:seq"
	routePathSnapshotSeqRestore = "/:seq/restore"

	routePrefixAdminUDSCluster               = "/v1/cluster"
	routePathAdminUDSClusterStatus           = "/status"
	routePathAdminUDSPlacement               = "/placement"
	routePathAdminUDSRemovePeer              = "/remove-peer"
	routePathAdminUDSEventLog                = "/eventlog"
	routePathAdminUDSTransferLeader          = "/transfer-leader"
	routePathAdminUDSHealth                  = "/health"
	routePathAdminUDSBalancerStatus          = "/balancer/status"
	routePathAdminUDSCapabilities            = "/capabilities"
	routePathAdminUDSExpandPlacement         = "/expand-placement"
	routePathAdminUDSVerifyPerVersionCutover = "/verify-per-version-cutover"
)
