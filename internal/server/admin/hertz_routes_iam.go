package admin

import "github.com/gritive/GrainFS/internal/iam"

func registerIAM(g router, d *Deps) {
	if !routeFeatureRoutesVisible(d, routeFeatureIAM) {
		return
	}
	// SA
	g.POST(routePathIAMSA, wrapBody[iam.SACreateRequest, iam.SACreateResponse](d, CreateSA))
	g.GET(routePathIAMSA, wrapZero(d, ListSA))
	g.GET(routePathIAMSAByID, iamGetSAHandler(d))
	g.DELETE(routePathIAMSAByID, iamDeleteSAHandler(d))
	g.PUT(routePathIAMGrant, wrapBodyNoOut204[iam.GrantPutRequest](d, PutGrant))
	g.DELETE(routePathIAMGrant, wrapBodyNoOut204[iam.GrantDeleteRequest](d, DeleteGrant))
	// Key
	g.POST(routePathIAMSAKey, iamCreateKeyHandler(d))
	g.DELETE(routePathIAMSAKeyByAK, iamRevokeKeyHandler(d))
	// Bucket upstream (PUT upsert -> 204). Routes under /upstreams (not
	// /buckets/upstream) to avoid Hertz static-beats-param collision with
	// GET /buckets/:name used by AdminGetBucket.
	g.PUT(routePathUpstreams, wrapBodyNoOut204[iam.BucketUpstreamPutRequest](d, PutBucketUpstream))
	g.GET(routePathUpstreams, wrapZero(d, ListBucketUpstreams))
	g.GET(routePathBucketUpstream, iamGetBucketUpstreamHandler(d))
	g.DELETE(routePathBucketUpstream, iamDeleteBucketUpstreamHandler(d))
	g.POST(routePathMigrationCutover, iamBucketUpstreamCutoverHandler(d))
}
