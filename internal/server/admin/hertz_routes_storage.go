package admin

func registerStorageUI(g router, d *Deps) {
	g.GET(routePathStorageProtocols, wrapZero(d, AdminStorageProtocols))
	if routeFeatureRoutesVisible(d, routeFeatureBuckets) {
		g.GET(routePathStorageBuckets, wrapZero(d, AdminListStorageBuckets))
		g.POST(routePathStorageBuckets, wrapBody[CreateBucketAdminReq, BucketInfo](d, AdminCreateStorageBucket))
	}
	if routeFeatureRoutesVisible(d, routeFeatureNfsExports) {
		g.GET(routePathStorageNfsExports, wrapZero(d, AdminNfsExportList))
		g.GET(routePathStorageNfsExport, wrapName(d, AdminNfsExportGet))
	}
}

func registerBucket(g router, d *Deps) {
	if !routeFeatureRoutesVisible(d, routeFeatureBuckets) {
		return
	}
	g.POST(routePathBuckets, wrapBody[CreateBucketAdminReq, BucketInfo](d, AdminCreateBucket))
	g.GET(routePathBuckets, wrapZero(d, AdminListBuckets))
	g.GET(routePathBucket, wrapName(d, AdminGetBucket))
	g.DELETE(routePathBucket, bucketDeleteHandler(d))
	g.GET(routePathBucketPolicy, bucketGetPolicyHandler(d))
	g.PUT(routePathBucketPolicy, bucketSetPolicyHandler(d))
	g.DELETE(routePathBucketPolicy, bucketDeletePolicyHandler(d))
	g.GET(routePathBucketVersioning, wrapName(d, AdminGetBucketVersioning))
	g.PUT(routePathBucketVersioning, bucketSetVersioningHandler(d))
}

func registerNfsExports(g router, d *Deps) {
	if !routeFeatureRoutesVisible(d, routeFeatureNfsExports) {
		return
	}
	g.POST(routePathNfsExports, wrapBody[NfsExportUpsertReq, NfsExportInfo](d, AdminNfsExportUpsert))
	g.GET(routePathNfsExports, wrapZero(d, AdminNfsExportList))
	g.GET(routePathNfsDebug, wrapName(d, AdminNfsExportDebug))
	g.GET(routePathNfsExport, wrapName(d, AdminNfsExportGet))
	g.DELETE(routePathNfsExport, nfsExportDeleteHandler(d))
	g.PATCH(routePathNfsExport, nfsExportPatchHandler(d))
}
