# OIDC Federated IAM

## What This Enables

OIDC principals can be evaluated by the same policy engine as service accounts.
Selected admin operations use a typed actor when one is present in request
context, so group policies attached to mapped OIDC groups can authorize
credential create, read, list, rotate, revoke, bucket-policy decisions, and
IAM/config/dashboard admin decisions on opted-in routes.

## Operator Flow

1. Configure each issuer with a unique `group_prefix`.
2. Ensure tokens carry the configured subject, audience, and groups claim.
3. Attach policies to mapped group names such as
   `oidc:example:storage-admins`.
4. Simulate the expected actor and action before granting production access.
5. Send opted-in admin requests with `Authorization: Bearer <token>`.
6. Audit or debug denied admin operations by checking the policy reason and
   mapped group names. Never persist or paste raw JWTs into audit notes.

## Admin Protocol Credential Flow

Bearer-token actor adoption is route-scoped to `/v1/credentials*`.

- Requests without a bearer token keep the existing admin UDS behavior and fall
  back to the target service account for protocol credential authorization.
- Requests with a valid bearer token authenticate the OIDC actor first, then
  evaluate `grainfs:CredentialCreate`, `grainfs:CredentialList`,
  `grainfs:CredentialRead`, `grainfs:CredentialRotate`, or
  `grainfs:CredentialRevoke` against the mapped OIDC principal and groups.
- Invalid bearer tokens return `401` before the protocol credential handler can
  create, rotate, revoke, or reveal credential metadata.
- Bearer-token handling is not global admin authentication. Other admin UDS
  routes continue to use their existing peercred/dashboard-token boundaries.
- Empty credential list responses still require a `grainfs:CredentialList`
  decision for bearer actors; denied actors get `403` even when no rows match.

Allowed example:

```text
actor.kind=oidc
actor.id=oidc:<issuer-hash>:<subject-hash>
actor.groups=["oidc:example:storage-admins"]
action=grainfs:CredentialCreate
resource=protocol-credential/nbd/volume/devdisk
decision=Allow
matched_policy=credential-admin
```

Denied example:

```text
actor.kind=oidc
actor.id=oidc:<issuer-hash>:<subject-hash>
actor.groups=[]
action=grainfs:CredentialRotate
resource=protocol-credential/nbd/volume/devdisk
decision=Deny
reason=implicit Deny
```

## Bucket Policy Admin Flow

Bearer-token actor adoption also covers bucket policy admin routes:

- `GET /v1/buckets/:name/policy` evaluates `grainfs:BucketPolicyRead`.
- `PUT /v1/buckets/:name/policy` evaluates `grainfs:BucketPolicyWrite`.
- `DELETE /v1/buckets/:name/policy` evaluates `grainfs:BucketPolicyDelete`.

The resource is the bucket ARN, for example `arn:aws:s3:::logs`. Requests
without a bearer token keep the existing admin UDS behavior. Requests with a
bearer token fail closed if the actor is unauthenticated, the admin authorizer
is unavailable, or the mapped OIDC principal and groups do not have an explicit
allow.

Admin action allows must name the credential, bucket-policy, IAM, or generic
admin action family explicitly. Broad data-access policies that use
`Action: "*"` or `Action: "grainfs:*"` do not grant `grainfs:Credential*`,
`grainfs:BucketPolicy*`, `grainfs:IAM*`, or `grainfs:Admin*` privileges.

Example policy for a mapped OIDC group:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "grainfs:BucketPolicyRead",
        "grainfs:BucketPolicyWrite",
        "grainfs:BucketPolicyDelete"
      ],
      "Resource": "arn:aws:s3:::logs"
    }
  ]
}
```

Bearer actor allow and deny decisions emit structured `admin_authz` log rows
with normalized principal kind/id, action, resource, decision, policy match, and
reason. Raw JWTs are never logged.

## IAM Admin Flow

Bearer-token actor adoption covers IAM admin inspection routes:

- `GET /v1/iam/sa` evaluates `grainfs:IAMServiceAccountList` on `iam/sa/*`.
- `GET /v1/iam/sa/:id` evaluates `grainfs:IAMServiceAccountRead` on
  `iam/sa/:id`.
- `GET /v1/iam/policy` evaluates `grainfs:IAMPolicyList` on `iam/policy/*`.
- `GET /v1/iam/policy/:name` evaluates `grainfs:IAMPolicyRead` on
  `iam/policy/:name`.
- `POST /v1/iam/policy/simulate` evaluates `grainfs:IAMPolicySimulate` on
  `iam/policy/*`.

It also covers IAM policy and group mutation routes:

- `PUT /v1/iam/policy/:name` evaluates `grainfs:IAMPolicyWrite` on
  `iam/policy/:name`.
- `DELETE /v1/iam/policy/:name` evaluates `grainfs:IAMPolicyDelete` on
  `iam/policy/:name`.
- `PUT /v1/iam/policy/:name/attach/sa/:said` evaluates
  `grainfs:IAMPolicyAttach` on `iam/policy/:name/attach/sa/:said`.
- `DELETE /v1/iam/policy/:name/attach/sa/:said` evaluates
  `grainfs:IAMPolicyDetach` on `iam/policy/:name/attach/sa/:said`.
- `PUT /v1/iam/group/:name` evaluates `grainfs:IAMGroupWrite` on
  `iam/group/:name`.
- `DELETE /v1/iam/group/:name` evaluates `grainfs:IAMGroupDelete` on
  `iam/group/:name`.
- `PUT /v1/iam/group/:name/member/:said` evaluates
  `grainfs:IAMGroupMemberWrite` on `iam/group/:name`.
- `DELETE /v1/iam/group/:name/member/:said` evaluates
  `grainfs:IAMGroupMemberDelete` on `iam/group/:name`.
- `PUT /v1/iam/group/:name/policy/:policy` evaluates
  `grainfs:IAMGroupPolicyAttach` on `iam/group/:name/policy/:policy`.
- `DELETE /v1/iam/group/:name/policy/:policy` evaluates
  `grainfs:IAMGroupPolicyDetach` on `iam/group/:name/policy/:policy`.

It also covers mount-SA and bucket-upstream admin routes:

- `GET /v1/iam/mount-sa` evaluates `grainfs:IAMMountSAList` on
  `iam/mount-sa/*`.
- `GET /v1/iam/mount-sa/:name` evaluates `grainfs:IAMMountSARead` on
  `iam/mount-sa/:name`.
- `POST /v1/iam/mount-sa` evaluates `grainfs:IAMMountSAWrite` on
  `iam/mount-sa/:name`, with `:name` read from the JSON body.
- `DELETE /v1/iam/mount-sa/:name` evaluates `grainfs:IAMMountSADelete` on
  `iam/mount-sa/:name`.
- `PUT /v1/iam/mount-sa/:name/policy/:policy` evaluates
  `grainfs:IAMMountSAPolicyAttach` on
  `iam/mount-sa/:name/policy/:policy`.
- `DELETE /v1/iam/mount-sa/:name/policy/:policy` evaluates
  `grainfs:IAMMountSAPolicyDetach` on
  `iam/mount-sa/:name/policy/:policy`.
- `GET /v1/upstreams` evaluates `grainfs:IAMBucketUpstreamList` on
  `iam/upstream/*`.
- `GET /v1/buckets/:bucket/upstream` evaluates
  `grainfs:IAMBucketUpstreamRead` on `iam/upstream/:bucket`.
- `PUT /v1/upstreams` evaluates `grainfs:IAMBucketUpstreamWrite` on
  `iam/upstream/:bucket`, with `:bucket` read from the JSON body.
- `DELETE /v1/buckets/:bucket/upstream` evaluates
  `grainfs:IAMBucketUpstreamDelete` on `iam/upstream/:bucket`.
- `POST /v1/migration/cutover` evaluates
  `grainfs:IAMBucketUpstreamCutover` on `iam/upstream/:bucket/cutover`, with
  `:bucket` read from the JSON body.

Config and dashboard-token admin routes use the generic admin action namespace:

- `GET /v1/config` evaluates `grainfs:AdminConfigList` on
  `admin/config/*`.
- `GET /v1/config/:key` evaluates `grainfs:AdminConfigRead` on
  `admin/config/:key`.
- `PUT /v1/config/:key` evaluates `grainfs:AdminConfigWrite` on
  `admin/config/:key`.
- `DELETE /v1/config/:key` evaluates `grainfs:AdminConfigDelete` on
  `admin/config/:key`.
- `GET /v1/dashboard/token` evaluates `grainfs:AdminDashboardTokenRead` on
  `admin/dashboard/token`.
- `POST /v1/dashboard/token/rotate` evaluates
  `grainfs:AdminDashboardTokenRotate` on `admin/dashboard/token/rotate`.

Dashboard-token routes remain admin-UDS only and are not mounted under
`/ui/api`.

Requests without a bearer token keep the existing admin UDS behavior. Requests
with a bearer token fail closed if the actor is unauthenticated, the admin
authorizer is unavailable, or policy evaluation denies the route action.

For bearer actors, IAM policy and group mutation routes also run a
self-effective-policy guard after route authorization and before the handler.
The guard rejects direct policy attach/detach to the caller, mutations to a
policy already in the caller's effective policy set, direct group membership
changes for the caller, and policy attach/detach or delete operations for a
group that is present in the caller's effective group set.

Example policy for a mapped OIDC group:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "grainfs:IAMServiceAccountList",
        "grainfs:IAMServiceAccountRead",
        "grainfs:IAMPolicyList",
        "grainfs:IAMPolicyRead",
        "grainfs:IAMPolicySimulate",
        "grainfs:IAMPolicyWrite",
        "grainfs:IAMPolicyDelete",
        "grainfs:IAMPolicyAttach",
        "grainfs:IAMPolicyDetach",
        "grainfs:IAMGroupWrite",
        "grainfs:IAMGroupDelete",
        "grainfs:IAMGroupMemberWrite",
        "grainfs:IAMGroupMemberDelete",
        "grainfs:IAMGroupPolicyAttach",
        "grainfs:IAMGroupPolicyDetach",
        "grainfs:IAMMountSA*",
        "grainfs:IAMBucketUpstream*",
        "grainfs:AdminConfig*",
        "grainfs:AdminDashboardToken*"
      ],
      "Resource": [
        "iam/sa/*",
        "iam/policy/*",
        "iam/policy/*/attach/sa/*",
        "iam/group/*",
        "iam/group/*/policy/*",
        "iam/mount-sa/*",
        "iam/mount-sa/*/policy/*",
        "iam/upstream/*",
        "iam/upstream/*/cutover",
        "admin/config/*",
        "admin/dashboard/token",
        "admin/dashboard/token/rotate"
      ]
    }
  ]
}
```

## Failure Modes

- Issuer, audience, or key mismatch: token authentication fails before
  authorization.
- Missing or malformed groups claim: the actor has no mapped group policies.
- Policy attached to the target service account only: federated actors are
  denied unless their direct principal ID or mapped groups also carry policy.
- Authorizer not configured: bearer actor credential, bucket policy, IAM, config,
  and dashboard-token admin operations fail closed.
- Self-effective-policy guard not configured: bearer actor IAM mutation routes
  fail closed.
- OIDC issuers not configured: bearer-token credential requests fail with `401`;
  requests without bearer tokens continue through the existing admin UDS path.
- Unsupported actor principal kind: authorization denies with a resolver error.

## External PDP Adapter

GrainFS can chain an external Policy Decision Point (PDP) after its own IAM
evaluation for IAM admin and protocol-credential operations. It is **disabled by
default**; when enabled, a request is allowed only if **both** GrainFS IAM and the
PDP allow it (deny-override) — the PDP can further restrict, never grant.

Enable it with the `iam.pdp` config key (single JSON document):

```json
{
  "enabled": true,
  "endpoint": "unix:///run/grainfs/pdp.sock",
  "timeout": "2s",
  "failure_policy": "closed"
}
```

- `endpoint`: a **local Unix socket only** (`unix:///path`). GrainFS POSTs
  `/authorize` over the socket with a JSON `{principal, action, resource, protocol,
  context}` body and expects `{"decision":"allow|deny","reason":"..."}`. Trust is
  local (same model as the admin UDS) — there is no bearer token in this release.
- `timeout`: per-request deadline (`>0`, `≤10s`). Read fresh each request, so a
  config change takes effect without restart.
- `failure_policy`:
  - `closed` (default): if the PDP is unreachable/erroring, the request is
    **denied** (`pdp_unavailable`). A PDP outage therefore blocks every
    GrainFS-allowed admin/credential operation — the secure default.
  - `open`: on PDP failure, fall back to the GrainFS-only decision and **allow**
    (audited as `pdp_skipped_fail_open`). Availability over enforcement — a
    conscious operator choice.

Coverage: protocol-credential operations are PDP-gated on every request; admin
routes are PDP-gated only for bearer/OIDC actor requests (a local peercred CLI
call over the admin UDS is governed by socket trust, not the PDP).

Observe: `grainfs_iam_pdp_requests_total{decision,error_type,failure_policy}` and
`grainfs_iam_pdp_request_duration_seconds`. Every PDP outcome (including a
fail-open skip) is recorded in the `iam.pdp` audit log line.

### Decision cache + grace

To avoid a PDP round-trip on every request and to ride out brief PDP outages, add
an optional `cache` block to `iam.pdp`:

```json
{
  "enabled": true,
  "endpoint": "unix:///run/grainfs/pdp.sock",
  "timeout": "2s",
  "failure_policy": "closed",
  "cache": {
    "ttl_allow": "60s",
    "ttl_deny": "30s",
    "max_entries": 4096,
    "grace_ttl": "5m"
  }
}
```

- Caching is active when `ttl_allow` or `ttl_deny` is `> 0` (absent `cache` block ⇒
  no caching). A cached decision is reused (no PDP call) until its TTL expires;
  allow and deny have separate TTLs. The cache is a bounded LRU (`max_entries`).
- A definitive PDP allow/deny is cached; a PDP **failure** is never cached.
- The cache is keyed on the full request (principal + action + resource), so two
  requests share a cached decision only if every input the PDP sees is identical.
- The cache is cleared automatically whenever `iam.pdp` changes (endpoint,
  failure_policy, or cache params), so a config change never serves a stale
  decision from the previous PDP.
- **`grace_ttl` (optional, `0` = disabled):** when the PDP call fails (timeout /
  unreachable / error) and a cached decision exists whose age is within
  `grace_ttl`, GrainFS serves that **last-good** decision instead of applying the
  failure policy. This is what lets `failure_policy: closed` survive a brief PDP
  blip — cached allows keep working, cached denies stay denied. If no cached entry
  is within `grace_ttl`, the failure policy applies as usual.
- **Revocation-lag tradeoff (read before enabling grace):** during a PDP outage, a
  cached **allow** can keep granting access for up to `grace_ttl` after the
  external PDP would have revoked it. Normally (PDP reachable) revocations
  propagate within `ttl_allow`. `grace_ttl: 0` (the default) eliminates this — a
  PDP outage then fails closed immediately. Set `grace_ttl` to the availability
  window you are willing to trade for that lag.

Cache observability: `grainfs_iam_pdp_cache_total{result,decision}`
(`result` = `hit` | `miss` | `grace`) and `grainfs_iam_pdp_cache_entries` (gauge).
A `grace` outcome means a cached decision was served during a PDP outage — watch
it alongside `grainfs_iam_pdp_requests_total{decision="error"}` to spot PDP
unavailability. Cache **hits are not re-audited** in the `iam.pdp` log (the
decision was audited when first computed); misses and grace-serves are.

Not yet supported: remote (`https`) PDP endpoints, bearer tokens to the PDP, mTLS,
and S3/Iceberg data-plane enforcement.

## Current Boundary

HTTP bearer-token actors are wired for protocol credential, bucket policy, IAM,
mount-SA, bucket-upstream, config, and dashboard-token admin routes. An optional
External PDP adapter (local Unix socket, disabled by default) chains after GrainFS
IAM for admin and protocol-credential operations; remote transport and broader
coverage remain future slices.
