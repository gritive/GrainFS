# OIDC Federated IAM

## What This Enables

OIDC principals can be evaluated by the same policy engine as service accounts.
Selected admin operations use a typed actor when one is present in request
context, so group policies attached to mapped OIDC groups can authorize
credential create, read, list, rotate, revoke, bucket-policy decisions, and
read-only IAM admin inspection.

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

Admin action allows must name the credential, bucket-policy, or IAM admin action
family explicitly. Broad data-access policies that use `Action: "*"` do not
grant `grainfs:Credential*`, `grainfs:BucketPolicy*`, or `grainfs:IAM*` admin
privileges.

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
        "grainfs:IAMGroupPolicyDetach"
      ],
      "Resource": [
        "iam/sa/*",
        "iam/policy/*",
        "iam/policy/*/attach/sa/*",
        "iam/group/*",
        "iam/group/*/policy/*"
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
- Authorizer not configured: bearer actor credential, bucket policy, and IAM
  admin operations fail closed.
- Self-effective-policy guard not configured: bearer actor IAM mutation routes
  fail closed.
- OIDC issuers not configured: bearer-token credential requests fail with `401`;
  requests without bearer tokens continue through the existing admin UDS path.
- Unsupported actor principal kind: authorization denies with a resolver error.

## Current Boundary

HTTP bearer-token actors are wired for protocol credential, bucket policy, and
IAM admin routes. Mount-SA/upstream routes, config/dashboard route adoption, and
an external PDP adapter remain separate follow-ups.
