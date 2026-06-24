package admin

const (
	routePrefixAdmin = "/v1"
	routePrefixUI    = "/ui/api"

	routePathClusterPeers          = "/cluster/peers"
	routePathResourceVlogBreakdown = "/resource/vlog/breakdown"

	routePathStorageBuckets = "/storage/buckets"

	routePathScrub           = "/scrub"
	routePathScrubJobs       = "/scrub/jobs"
	routePathScrubJob        = "/scrub/jobs/:id"
	routePathDashboardToken  = "/dashboard/token"
	routePathDashboardRotate = "/dashboard/token/rotate"

	routePathCredentials      = "/credentials"
	routePathCredential       = "/credentials/:id"
	routePathCredentialRotate = "/credentials/:id/rotate"

	routePathBuckets          = "/buckets"
	routePathBucket           = "/buckets/:name"
	routePathBucketPolicy     = "/buckets/:name/policy"
	routePathBucketVersioning = "/buckets/:name/versioning"

	routePathConfig      = "/config"
	routePathConfigByKey = "/config/:key"

	routePathIAMSA        = "/iam/sa"
	routePathIAMSAByID    = "/iam/sa/:id"
	routePathIAMSAKey     = "/iam/sa/:id/key"
	routePathIAMSAKeyByAK = "/iam/sa/:id/key/:ak"

	routePathIAMPolicy         = "/iam/policy"
	routePathIAMPolicySimulate = "/iam/policy/simulate"
	routePathIAMPolicyByName   = "/iam/policy/:name"
	routePathIAMPolicyAttachSA = "/iam/policy/:name/attach/sa/:said"

	// Group routes: member and policy sub-paths listed before bare :name to
	// ensure more-specific routes are registered first.
	routePathIAMGroupByName       = "/iam/group/:name"
	routePathIAMGroupMember       = "/iam/group/:name/member/:said"
	routePathIAMGroupPolicyAttach = "/iam/group/:name/policy/:policy"

	// PDP bearer-token: set (POST, token in body) / clear (DELETE) / show (GET).
	routePathIAMPDPToken  = "/iam/pdp/token"
	routePathIAMPDPStatus = "/iam/pdp/status"

	routePathAuditQuery        = "/audit/query"
	routePathAuditRecentDenies = "/audit/recent-denies"
	routePathAuditBySA         = "/audit/by-sa/:said"
	routePathAuditByRequestID  = "/audit/by-request-id/:rid"

	routePathStatus = "/status"

	routePathUpstreams        = "/upstreams"
	routePathBucketUpstream   = "/buckets/:bucket/upstream"
	routePathMigrationCutover = "/migration/cutover"
)
