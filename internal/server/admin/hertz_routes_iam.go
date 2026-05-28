package admin

import "github.com/gritive/GrainFS/internal/iam"

func registerIAM(g router, d *Deps) {
	if !routeFeatureRoutesVisible(d, routeFeatureIAM) {
		return
	}
	actor := adminActorMiddleware(d)
	saListAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMServiceAccountList",
		resource: iamSAResource,
	})
	saReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMServiceAccountRead",
		resource: iamSAResource,
	})
	policySimulateAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicySimulate",
		resource: iamPolicyResource,
	})
	policyReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyRead",
		resource: iamPolicyResource,
	})
	policyListAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyList",
		resource: iamPolicyResource,
	})
	policyWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyWrite",
		resource: iamPolicyResource,
		guard:    denyPolicyIfSelfEffective,
	})
	policyDeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyDelete",
		resource: iamPolicyResource,
		guard:    denyPolicyIfSelfEffective,
	})
	policyAttachSAAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyAttach",
		resource: iamPolicyAttachSAResource,
		guard:    denyDirectSelfPolicyAttach,
	})
	policyDetachSAAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMPolicyDetach",
		resource: iamPolicyAttachSAResource,
		guard:    denyDirectSelfPolicyAttach,
	})
	groupWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupWrite",
		resource: iamGroupResource,
		guard:    denyGroupIfSelfEffective,
	})
	groupDeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupDelete",
		resource: iamGroupResource,
		guard:    denyGroupIfSelfEffective,
	})
	groupMemberWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupMemberWrite",
		resource: iamGroupResource,
		guard:    denyDirectSelfGroupMember,
	})
	groupMemberDeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupMemberDelete",
		resource: iamGroupResource,
		guard:    denyDirectSelfGroupMember,
	})
	groupPolicyAttachAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupPolicyAttach",
		resource: iamGroupPolicyResource,
		guard:    denyGroupIfSelfEffective,
	})
	groupPolicyDetachAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMGroupPolicyDetach",
		resource: iamGroupPolicyResource,
		guard:    denyGroupIfSelfEffective,
	})
	mountSAListAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSAList",
		resource: iamMountSAResource,
	})
	mountSAReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSARead",
		resource: iamMountSAResource,
	})
	mountSAWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSAWrite",
		resource: iamMountSAResource,
	})
	mountSADeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSADelete",
		resource: iamMountSAResource,
	})
	mountSAPolicyAttachAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSAPolicyAttach",
		resource: iamMountSAPolicyResource,
	})
	mountSAPolicyDetachAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMMountSAPolicyDetach",
		resource: iamMountSAPolicyResource,
	})
	upstreamListAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMBucketUpstreamList",
		resource: iamBucketUpstreamResource,
	})
	upstreamReadAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMBucketUpstreamRead",
		resource: iamBucketUpstreamResource,
	})
	upstreamWriteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMBucketUpstreamWrite",
		resource: iamBucketUpstreamResource,
	})
	upstreamDeleteAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMBucketUpstreamDelete",
		resource: iamBucketUpstreamResource,
	})
	upstreamCutoverAuthz := adminRouteAuthzMiddleware(d, adminRouteAuthzSpec{
		action:   "grainfs:IAMBucketUpstreamCutover",
		resource: iamBucketUpstreamCutoverResource,
	})
	// SA
	g.POST(routePathIAMSA, wrapBody[iam.SACreateRequest, iam.SACreateResponse](d, CreateSA))
	g.GET(routePathIAMSA, actor, saListAuthz, wrapZero(d, ListSA))
	g.GET(routePathIAMSAByID, actor, saReadAuthz, iamGetSAHandler(d))
	g.DELETE(routePathIAMSAByID, iamDeleteSAHandler(d))
	// Key
	g.POST(routePathIAMSAKey, iamCreateKeyHandler(d))
	g.DELETE(routePathIAMSAKeyByAK, iamRevokeKeyHandler(d))
	// Policy — simulate must be registered before :name to avoid param capture
	g.POST(routePathIAMPolicySimulate, actor, policySimulateAuthz, iamPolicySimulateHandler(d))
	g.PUT(routePathIAMPolicyByName, actor, policyWriteAuthz, iamPolicyPutHandler(d))
	g.GET(routePathIAMPolicyByName, actor, policyReadAuthz, iamPolicyGetHandler(d))
	g.DELETE(routePathIAMPolicyByName, actor, policyDeleteAuthz, iamPolicyDeleteHandler(d))
	g.GET(routePathIAMPolicy, actor, policyListAuthz, iamPolicyListHandler(d))
	g.PUT(routePathIAMPolicyAttachSA, actor, policyAttachSAAuthz, iamPolicyAttachSAHandler(d))
	g.DELETE(routePathIAMPolicyAttachSA, actor, policyDetachSAAuthz, iamPolicyDetachSAHandler(d))
	// Group (create/delete/member/policy-attach)
	// More-specific sub-paths (:name/member/:said, :name/policy/:policy) must be
	// registered before the bare :name handler to prevent param capture.
	g.PUT(routePathIAMGroupMember, actor, groupMemberWriteAuthz, iamGroupMemberPutHandler(d))
	g.DELETE(routePathIAMGroupMember, actor, groupMemberDeleteAuthz, iamGroupMemberDeleteHandler(d))
	g.PUT(routePathIAMGroupPolicyAttach, actor, groupPolicyAttachAuthz, iamGroupPolicyAttachHandler(d))
	g.DELETE(routePathIAMGroupPolicyAttach, actor, groupPolicyDetachAuthz, iamGroupPolicyDetachHandler(d))
	g.PUT(routePathIAMGroupByName, actor, groupWriteAuthz, iamGroupPutHandler(d))
	g.DELETE(routePathIAMGroupByName, actor, groupDeleteAuthz, iamGroupDeleteHandler(d))
	// MountSA (create/delete/policy-attach/detach + list/get)
	// Policy sub-path (:name/policy/:policy) must be registered before bare :name.
	g.PUT(routePathIAMMountSAPolicyAttach, actor, mountSAPolicyAttachAuthz, iamMountSAPolicyAttachHandler(d))
	g.DELETE(routePathIAMMountSAPolicyAttach, actor, mountSAPolicyDetachAuthz, iamMountSAPolicyDetachHandler(d))
	g.POST(routePathIAMMountSA, actor, mountSAWriteAuthz, iamMountSAPostHandler(d))
	g.GET(routePathIAMMountSA, actor, mountSAListAuthz, iamMountSAListHandler(d))
	g.GET(routePathIAMMountSAByName, actor, mountSAReadAuthz, iamMountSAGetHandler(d))
	g.DELETE(routePathIAMMountSAByName, actor, mountSADeleteAuthz, iamMountSADeleteHandler(d))
	// Bucket upstream (PUT upsert -> 204). Routes under /upstreams (not
	// /buckets/upstream) to avoid Hertz static-beats-param collision with
	// GET /buckets/:name used by AdminGetBucket.
	g.PUT(routePathUpstreams, actor, upstreamWriteAuthz, wrapBodyNoOut204[iam.BucketUpstreamPutRequest](d, PutBucketUpstream))
	g.GET(routePathUpstreams, actor, upstreamListAuthz, wrapZero(d, ListBucketUpstreams))
	g.GET(routePathBucketUpstream, actor, upstreamReadAuthz, iamGetBucketUpstreamHandler(d))
	g.DELETE(routePathBucketUpstream, actor, upstreamDeleteAuthz, iamDeleteBucketUpstreamHandler(d))
	g.POST(routePathMigrationCutover, actor, upstreamCutoverAuthz, iamBucketUpstreamCutoverHandler(d))
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
