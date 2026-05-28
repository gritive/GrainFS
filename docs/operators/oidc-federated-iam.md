# OIDC Federated IAM

## What This Enables

OIDC principals can be evaluated by the same policy engine as service accounts.
Protocol credential admin operations use a typed actor when one is present in
request context, so group policies attached to mapped OIDC groups can authorize
credential create, read, list, rotate, and revoke decisions.

## Operator Flow

1. Configure each issuer with a unique `group_prefix`.
2. Ensure tokens carry the configured subject, audience, and groups claim.
3. Attach policies to mapped group names such as
   `oidc:example:storage-admins`.
4. Simulate the expected actor and action before granting production access.
5. Send admin protocol credential requests with `Authorization: Bearer <token>`.
6. Audit or debug denied protocol credential operations by checking the policy
   reason and mapped group names. Never persist or paste raw JWTs into audit
   notes.

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

## Failure Modes

- Issuer, audience, or key mismatch: token authentication fails before
  authorization.
- Missing or malformed groups claim: the actor has no mapped group policies.
- Policy attached to the target service account only: federated actors are
  denied unless their direct principal ID or mapped groups also carry policy.
- Authorizer not configured: protocol credential admin operations fail closed.
- OIDC issuers not configured: bearer-token credential requests fail with `401`;
  requests without bearer tokens continue through the existing admin UDS path.
- Unsupported actor principal kind: authorization denies with a resolver error.

## Current Boundary

This slice wires HTTP bearer-token actors only for protocol credential admin
routes. Broader admin route adoption and an external PDP adapter remain separate
follow-ups.
