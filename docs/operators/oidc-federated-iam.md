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

It also covers bucket-upstream admin routes:

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
  "endpoint": "https://pdp.internal.example:8443",
  "timeout": "2s",
  "failure_policy": "closed",
  "tls": { "ca_pem": "-----BEGIN CERTIFICATE-----\n...", "min_version": "1.2" },
  "ssrf": { "allow_private": false }
}
```

- `endpoint`: **`https://` (remote) or `http://` (local sidecar only)**. GrainFS
  POSTs `/authorize` with a JSON `{principal, action, resource, protocol, context}`
  body and expects `{"decision":"allow|deny","reason":"..."}`. The endpoint must be
  `scheme://host[:port]` with no path/query/fragment/userinfo.
  - **`https://`**: the remote/production form. Requires TLS server verification
    (below) and is the only scheme that may carry a bearer token.
  - **`http://`**: allowed **only to a loopback host** (a co-located sidecar, e.g.
    `http://127.0.0.1:8181`). A bearer token or `tls` block on an `http://`
    endpoint is rejected at config time (no plaintext secret on the wire).
  - The legacy `unix://` transport has been removed; use `http://127.0.0.1:…` for a
    local sidecar.
- **Bearer token** (https only): set it out-of-band, never in this JSON. It is
  sealed at rest under the cluster DEK (same path as IAM credentials, replicated to
  every node) and delivered via the admin socket:
  ```
  grainfs iam pdp set-token --token-file /run/secrets/pdp-token   # seals + stores
  grainfs iam pdp show                                            # status only; prints a fingerprint, never the token
  grainfs iam pdp clear-token                                     # removes it
  ```
  The token is attached as `Authorization: Bearer <token>` on every PDP call and is
  never logged, audited, or shown. Rotating it (`set-token` again) transparently
  rebuilds the PDP client and clears the decision cache.
- **`tls`** (https only): `ca_pem` pins a CA by **inlining the PEM content** (not a
  file path) so every cluster node verifies identically — a per-node file path would
  diverge. Omit `ca_pem` to use the host's system roots (see the parity caveat
  below). `min_version` floors the TLS version (`1.2` default, `1.3` allowed; lower
  is rejected). `InsecureSkipVerify` is never available — there is no TLS-downgrade knob.
- **`ssrf`**: GrainFS applies an **egress filter at dial time** on the resolved IP
  (rebinding-proof): an `https` endpoint that resolves to loopback, link-local
  (incl. the cloud metadata IP `169.254.169.254`), private (RFC1918 / IPv6 ULA),
  CGNAT, or other special-use ranges is **blocked**. `allow_private: true` (default
  `false`) relaxes the loopback/private/CGNAT classes for an internal-network PDP —
  **note that this disables SSRF protection for those ranges**; link-local,
  metadata, and multicast stay blocked regardless. HTTP proxies (`HTTP(S)_PROXY`)
  are ignored for PDP calls so they cannot bypass the filter.
- `timeout`: per-request deadline (`>0`, `≤10s`). Read fresh each request, so a
  config change takes effect without restart.
- `failure_policy`:
  - `closed` (default): if the PDP is unreachable/erroring, the request is
    **denied** (`pdp_unavailable`). A PDP outage therefore blocks every
    GrainFS-allowed admin/credential operation — the secure default.
  - `open`: on PDP failure, fall back to the GrainFS-only decision and **allow**
    (audited as `pdp_skipped_fail_open`). Availability over enforcement — a
    conscious operator choice.
  - **SSRF-blocked is exempt from `failure_policy`:** if a PDP dial is rejected by
    the egress filter (`error_type=ssrf_blocked`), the request is **always denied**,
    even under `failure_policy: open`. An egress block is a security event, not an
    ordinary outage — `open` must not turn a poisoned-DNS/misconfigured endpoint into
    a silent total PDP bypass. Such events are logged at WARN and counted.

> **Cluster parity caveats.** TLS verification with `ca_pem` is identical on every
> node (the PEM replicates via Raft). With `ca_pem` omitted, "system roots" are
> **node-local** — nodes with different OS trust stores could disagree on the same
> PDP cert; pin `ca_pem` for strict cluster parity. TLS validity also depends on
> each node's clock (assume NTP). **NAT64/DNS64 residual:** in such environments a
> public-looking IPv6 can synthesize a route to private IPv4 that dial-time IP
> classification cannot detect; rely on operator egress controls there.

Coverage: protocol-credential operations are PDP-gated on every request; admin
routes are PDP-gated only for bearer/OIDC actor requests (a local peercred CLI
call over the admin UDS is governed by socket trust, not the PDP).

Observe: `grainfs_iam_pdp_requests_total{scope,decision,error_type,failure_policy}` and
`grainfs_iam_pdp_request_duration_seconds{scope}`. `error_type` now includes
`ssrf_blocked` (dial rejected by the egress filter) and `tls` (handshake failure)
alongside `timeout`/`transport`/`status`/`decode`/`invalid_decision`. Every PDP
outcome (including a fail-open skip and a hard SSRF deny) is recorded in the
`iam.pdp` audit log line.

All four `grainfs_iam_pdp_*` metrics now carry a **`scope`** label identifying the
authorizer instance that emitted them (`admin` and `protocol_credential` today).
`grainfs_iam_pdp_request_duration_seconds` and `grainfs_iam_pdp_cache_entries`
changed from unlabeled to per-`scope` series — **any dashboard or alert bound to
the old unlabeled series must add the `scope` label** (e.g. `sum without(scope)
(...)` to aggregate back). The `scope` label also fixed a gauge that previously
clobbered across authorizer instances, so `grainfs_iam_pdp_cache_entries` is now
accurate per scope.

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

Cache observability: `grainfs_iam_pdp_cache_total{scope,result,decision}`
(`result` = `hit` | `miss` | `grace`) and `grainfs_iam_pdp_cache_entries{scope}` (gauge).
A `grace` outcome means a cached decision was served during a PDP outage — watch
it alongside `grainfs_iam_pdp_requests_total{decision="error"}` to spot PDP
unavailability. Cache **hits are not re-audited** in the `iam.pdp` log (the
decision was audited when first computed); misses and grace-serves are.

### Data-plane enforcement

The slices above gate **control-plane** operations (admin routes, protocol
credentials). `iam.pdp.data_plane.enabled` extends the same external PDP, with
the same deny-override semantics, to **S3 object/bucket authorization** — after
GrainFS IAM allows an object/bucket operation, the PDP can still veto it.

It is **disabled by default** and is an **AND-gate**: data-plane enforcement
runs only when **both** `iam.pdp.enabled` and `iam.pdp.data_plane.enabled` are
`true`. "Data-plane on while control-plane off" is inexpressible by design —
turning off the top-level `iam.pdp.enabled` disables every PDP plane at once
(secure-by-default).

```json
{
  "enabled": true,
  "endpoint": "https://pdp.internal.example:8443",
  "timeout": "2s",
  "failure_policy": "closed",
  "tls": { "ca_pem": "-----BEGIN CERTIFICATE-----\n...", "min_version": "1.2" },
  "data_plane": { "enabled": true }
}
```

- **Scope: S3 only.**
- **Protocol/context on the wire.** A data-plane PDP request carries
  `protocol` = `s3`, `auth_method` = `sa` or `anonymous`, and an **empty**
  `target_sa` (the operation acts on an object, not a service account).
  Control-plane PDP requests are unchanged and still carry `protocol="admin"`.
- **Singleflight.** Concurrent duplicate authorization misses for the same cache
  key collapse to a **single** PDP call; the rest wait for and share that one
  result, so a burst of identical object requests does not fan out into a burst
  of PDP calls.
- **Availability (`failure_policy=closed`).** With the secure default, a PDP
  outage **denies** cache-cold data-plane traffic (object/bucket requests with
  no in-window cached decision). Set `cache.grace_ttl` (see above) to keep
  serving last-good allows through a brief outage. SSRF-blocked dials and
  broken-token failures are **HARD-DENY** and are exempt from grace and from
  `failure_policy` — they are fail-fast security gates, not ordinary outages.

> **Cross-plane fail-open warning.** `failure_policy` is **shared** across the
> control and data planes. Setting `failure_policy: open` to relieve a
> data-plane availability problem **also fail-opens the control-plane PDP gates**
> (the shipped admin and protocol-credential enforcement) — a PDP outage would
> then allow control-plane operations that GrainFS IAM permits. A per-scope
> failure policy (so data-plane and control-plane can differ) is a documented
> follow-up. Until then, prefer `failure_policy: closed` + `cache.grace_ttl` for
> data-plane availability rather than `open`.

Observe per plane with the `scope` label on the `grainfs_iam_pdp_*` metric
families (`scope` = `admin` | `protocol_credential` | `data_plane`). To read
just the data-plane series, filter on `scope="data_plane"`, e.g.
`grainfs_iam_pdp_requests_total{scope="data_plane"}` and
`grainfs_iam_pdp_cache_total{scope="data_plane"}`. This lets you split
data-plane request volume, cache hit/miss/grace, and latency from the
control-plane series.

Not yet supported: mTLS client certs to the PDP, a per-CIDR SSRF allowlist,
and a per-scope `failure_policy`.

## Current Boundary

HTTP bearer-token actors are wired for protocol credential, bucket policy, IAM,
bucket-upstream, config, and dashboard-token admin routes. An optional
External PDP adapter (remote `https` with a DEK-sealed bearer token + dial-time SSRF
egress filtering, or a local `http` loopback sidecar; disabled by default) chains
after GrainFS IAM for admin and protocol-credential operations, and — when
`iam.pdp.data_plane.enabled` is also set — for S3 object/bucket operations. mTLS, a per-CIDR SSRF allowlist, and a per-scope `failure_policy`
remain future slices.
