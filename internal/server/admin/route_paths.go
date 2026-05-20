package admin

const (
	routePrefixAdmin = "/v1"
	routePrefixUI    = "/ui/api"

	routePathClusterPeers          = "/cluster/peers"
	routePathResourceVlogBreakdown = "/resource/vlog/breakdown"

	routePathStorageProtocols  = "/storage/protocols"
	routePathStorageBuckets    = "/storage/buckets"
	routePathStorageNfsExports = "/storage/nfs/exports"
	routePathStorageNfsExport  = "/storage/nfs/exports/:name"
	routePathStorageNfsDebug   = "/storage/nfs/exports/:name/debug"

	routePathVolumes           = "/volumes"
	routePathVolume            = "/volumes/:name"
	routePathVolumeStat        = "/volumes/:name/stat"
	routePathVolumeResize      = "/volumes/:name/resize"
	routePathVolumeRecalculate = "/volumes/:name/recalculate"
	routePathVolumeClone       = "/volumes/clone"
	routePathVolumeWriteAt     = "/volumes/:name/write-at"
	routePathVolumeReadAt      = "/volumes/:name/read-at"

	routePathVolumeSnapshots        = "/volumes/:name/snapshots"
	routePathVolumeSnapshot         = "/volumes/:name/snapshots/:snap"
	routePathVolumeSnapshotRollback = "/volumes/:name/snapshots/:snap/rollback"
	routePathVolumeScrub            = "/volumes/:name/scrub"

	routePathScrub           = "/scrub"
	routePathScrubJobs       = "/scrub/jobs"
	routePathScrubJob        = "/scrub/jobs/:id"
	routePathDashboardToken  = "/dashboard/token"
	routePathDashboardRotate = "/dashboard/token/rotate"

	routePathBuckets          = "/buckets"
	routePathBucket           = "/buckets/:name"
	routePathBucketPolicy     = "/buckets/:name/policy"
	routePathBucketVersioning = "/buckets/:name/versioning"

	routePathNfsExports = "/nfs/exports"
	routePathNfsDebug   = "/nfs/exports/:name/debug"
	routePathNfsExport  = "/nfs/exports/:name"

	routePathConfig      = "/config"
	routePathConfigByKey = "/config/:key"

	routePathIAMSA        = "/iam/sa"
	routePathIAMSAByID    = "/iam/sa/:id"
	routePathIAMSAKey     = "/iam/sa/:id/key"
	routePathIAMSAKeyByAK = "/iam/sa/:id/key/:ak"
	routePathIAMGrant     = "/iam/grant"

	routePathUpstreams        = "/upstreams"
	routePathBucketUpstream   = "/buckets/:bucket/upstream"
	routePathMigrationCutover = "/migration/cutover"
)
