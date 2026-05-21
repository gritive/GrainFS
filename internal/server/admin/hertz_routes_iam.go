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
	// Key
	g.POST(routePathIAMSAKey, iamCreateKeyHandler(d))
	g.DELETE(routePathIAMSAKeyByAK, iamRevokeKeyHandler(d))
	// Policy — simulate must be registered before :name to avoid param capture
	g.POST(routePathIAMPolicySimulate, iamPolicySimulateHandler(d))
	g.PUT(routePathIAMPolicyByName, iamPolicyPutHandler(d))
	g.GET(routePathIAMPolicyByName, iamPolicyGetHandler(d))
	g.DELETE(routePathIAMPolicyByName, iamPolicyDeleteHandler(d))
	g.GET(routePathIAMPolicy, iamPolicyListHandler(d))
	g.PUT(routePathIAMPolicyAttachSA, iamPolicyAttachSAHandler(d))
	g.DELETE(routePathIAMPolicyAttachSA, iamPolicyDetachSAHandler(d))
	// Group (create/delete/member/policy-attach)
	// More-specific sub-paths (:name/member/:said, :name/policy/:policy) must be
	// registered before the bare :name handler to prevent param capture.
	g.PUT(routePathIAMGroupMember, iamGroupMemberPutHandler(d))
	g.DELETE(routePathIAMGroupMember, iamGroupMemberDeleteHandler(d))
	g.PUT(routePathIAMGroupPolicyAttach, iamGroupPolicyAttachHandler(d))
	g.DELETE(routePathIAMGroupPolicyAttach, iamGroupPolicyDetachHandler(d))
	g.PUT(routePathIAMGroupByName, iamGroupPutHandler(d))
	g.DELETE(routePathIAMGroupByName, iamGroupDeleteHandler(d))
	// MountSA (create/delete/policy-attach/detach + list/get)
	// Policy sub-path (:name/policy/:policy) must be registered before bare :name.
	g.PUT(routePathIAMMountSAPolicyAttach, iamMountSAPolicyAttachHandler(d))
	g.DELETE(routePathIAMMountSAPolicyAttach, iamMountSAPolicyDetachHandler(d))
	g.POST(routePathIAMMountSA, iamMountSAPostHandler(d))
	g.GET(routePathIAMMountSA, iamMountSAListHandler(d))
	g.GET(routePathIAMMountSAByName, iamMountSAGetHandler(d))
	g.DELETE(routePathIAMMountSAByName, iamMountSADeleteHandler(d))
	// Bucket upstream (PUT upsert -> 204). Routes under /upstreams (not
	// /buckets/upstream) to avoid Hertz static-beats-param collision with
	// GET /buckets/:name used by AdminGetBucket.
	g.PUT(routePathUpstreams, wrapBodyNoOut204[iam.BucketUpstreamPutRequest](d, PutBucketUpstream))
	g.GET(routePathUpstreams, wrapZero(d, ListBucketUpstreams))
	g.GET(routePathBucketUpstream, iamGetBucketUpstreamHandler(d))
	g.DELETE(routePathBucketUpstream, iamDeleteBucketUpstreamHandler(d))
	g.POST(routePathMigrationCutover, iamBucketUpstreamCutoverHandler(d))
}

// registerIAMUI mounts the dashboard-safe subset of IAM routes: SA CRUD, key
// management, and bucket-upstream management (including migration cutover).
// Policy and group admin routes are intentionally omitted — they grant powers
// (attach Resource:* policies, create groups, modify SA membership) that are
// root-equivalent. The CLI Resource:* warning lives in the binary; the wire
// shape carries no such guard. Dashboard-token holders get the SA / Key /
// BucketUpstream surface only.
func registerIAMUI(g router, d *Deps) {
	if !routeFeatureRoutesVisible(d, routeFeatureIAM) {
		return
	}
	// SA
	g.POST(routePathIAMSA, wrapBody[iam.SACreateRequest, iam.SACreateResponse](d, CreateSA))
	g.GET(routePathIAMSA, wrapZero(d, ListSA))
	g.GET(routePathIAMSAByID, iamGetSAHandler(d))
	g.DELETE(routePathIAMSAByID, iamDeleteSAHandler(d))
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
