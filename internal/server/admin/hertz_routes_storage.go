package admin

func registerStorageUI(g router, d *Deps) {
	g.GET(routePathStorageProtocols, wrapZero(d, AdminStorageProtocols))
	if routeFeatureRoutesVisible(d, routeFeatureBuckets) {
		g.GET(routePathStorageBuckets, wrapZero(d, AdminListStorageBuckets))
		g.POST(routePathStorageBuckets, wrapBody[CreateBucketAdminReq, BucketInfo](d, AdminCreateStorageBucket))
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
	actor := adminActorMiddleware(d)
	g.GET(routePathBucketPolicy, actor, bucketGetPolicyHandler(d))
	g.PUT(routePathBucketPolicy, actor, bucketSetPolicyHandler(d))
	g.DELETE(routePathBucketPolicy, actor, bucketDeletePolicyHandler(d))
	g.GET(routePathBucketVersioning, wrapName(d, AdminGetBucketVersioning))
	g.PUT(routePathBucketVersioning, bucketSetVersioningHandler(d))
}
