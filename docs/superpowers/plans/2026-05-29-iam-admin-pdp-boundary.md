# IAM Admin PDP Boundary Follow-Up Plan

## Goal

Finish the remaining OIDC federated IAM admin follow-up by extending the
existing bearer actor route-authorization boundary to the remaining explicit
admin routes called out in `TODOS.md`: mount-SA, bucket-upstream, config, and
dashboard-token routes.

## Scope

This PR uses the existing central `adminRouteAuthzMiddleware` registry. It does
not introduce an external PDP adapter yet.

In scope:

- Mount-SA admin routes under `/v1/iam/mount-sa`.
- Bucket-upstream admin routes under `/v1/upstreams`, `/v1/buckets/:bucket/upstream`,
  and `/v1/migration/cutover`.
- Config admin routes under `/v1/config`.
- Dashboard-token admin routes under `/v1/dashboard/token`.
- IAM policy parser/matcher resource shapes for the new admin route resources.
- Operator documentation and `TODOS.md` cleanup.

Out of scope:

- External PDP adapter implementation.
- UI route adoption. Dashboard-token routes must remain absent from `/ui/api`.
- NFS, audit, status, volume, scrub, cluster, and iceberg config route adoption.

## Route Authz Contract

Bearer actors only:

- Requests without bearer tokens keep existing admin UDS behavior.
- Malformed bearer tokens fail before handlers run.
- Missing `AdminAuthz` fails closed for bearer requests.

Actions/resources:

- `grainfs:IAMMountSAList` on `iam/mount-sa/*`
- `grainfs:IAMMountSARead` on `iam/mount-sa/:name`
- `grainfs:IAMMountSAWrite` on `iam/mount-sa/:name`
- `grainfs:IAMMountSADelete` on `iam/mount-sa/:name`
- `grainfs:IAMMountSAPolicyAttach` on `iam/mount-sa/:name/policy/:policy`
- `grainfs:IAMMountSAPolicyDetach` on `iam/mount-sa/:name/policy/:policy`
- `grainfs:IAMBucketUpstreamList` on `iam/upstream/*`
- `grainfs:IAMBucketUpstreamRead` on `iam/upstream/:bucket`
- `grainfs:IAMBucketUpstreamWrite` on `iam/upstream/:bucket`
- `grainfs:IAMBucketUpstreamDelete` on `iam/upstream/:bucket`
- `grainfs:IAMBucketUpstreamCutover` on `iam/upstream/:bucket/cutover`
- `grainfs:AdminConfigList` on `admin/config/*`
- `grainfs:AdminConfigRead` on `admin/config/:key`
- `grainfs:AdminConfigWrite` on `admin/config/:key`
- `grainfs:AdminConfigDelete` on `admin/config/:key`
- `grainfs:AdminDashboardTokenRead` on `admin/dashboard/token`
- `grainfs:AdminDashboardTokenRotate` on `admin/dashboard/token/rotate`

Routes whose resource identifier lives only in the JSON body must derive the
concrete resource from `c.Request.Body()` before the handler runs, without
consuming or mutating the body bytes the handler will later decode. This applies
to mount-SA create, bucket-upstream PUT, and bucket-upstream cutover. Tests must
prove the authz layer records the concrete resource and the handler still
receives the same body.

`grainfs:Admin*` must follow the same explicit-admin-action rule as
`grainfs:IAM*`, `grainfs:Credential*`, and `grainfs:BucketPolicy*`: broad
`Action: "*"` and `Action: "grainfs:*"` must not grant these routes. Policies
must name `grainfs:Admin*` or a narrower `grainfs:Admin...` action.

## Tasks

1. Extend policy action/resource parsing and matching.
   - Verify `admin/config/*`, `admin/dashboard/token`, `iam/mount-sa/*`,
     `iam/mount-sa/*/policy/*`, `iam/upstream/*`, and
     `iam/upstream/*/cutover` resources parse.
   - Verify `admin/dashboard/token/rotate` parses.
   - Verify `grainfs:Admin*` actions deny `Action: "*"`, deny
     `Action: "grainfs:*"`, and allow `Action: "grainfs:Admin*"`.

2. Add resource helpers and route authz specs.
   - Keep no-bearer fallback.
   - Use `adminActorMiddleware` before `adminRouteAuthzMiddleware`.
   - Preserve existing route registration order.

3. Add route tests.
   - Bearer allow records action/resource and reaches handler for every newly
     adopted route/verb: mount-SA list/read/create/delete/policy attach/policy
     detach, upstream list/read/put/delete/cutover, config list/read/write/delete,
     dashboard token read/rotate.
   - Bearer deny stops before handler mutation for representative mutation
     routes in each newly adopted group.
   - Valid bearer with missing `AdminAuthz` fails closed before handler mutation
     for each newly adopted route family.
   - No-bearer requests keep existing UDS behavior.
   - Malformed bearer rejects before fallback.
   - Dashboard-token routes remain absent from `/ui/api`.

4. Update docs and TODOs.
   - Document new action/resource shapes.
   - Remove the completed TODO item from `TODOS.md`.
   - State that external PDP remains a future option, not part of this slice.

## Verification

- `go test ./internal/iam/policy ./internal/server/admin -count=1`
- `git diff --check`
- `rg -n "t\\.Fatalf|t\\.Fatal\\(|t\\.Errorf|t\\.Error\\("` on touched tests.
