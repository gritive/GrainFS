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
5. Audit denied protocol credential operations by checking the policy reason and
   mapped group names.

## Failure Modes

- Issuer, audience, or key mismatch: token authentication fails before
  authorization.
- Missing or malformed groups claim: the actor has no mapped group policies.
- Policy attached to the target service account only: federated actors are
  denied unless their direct principal ID or mapped groups also carry policy.
- Authorizer not configured: protocol credential admin operations fail closed.
- Unsupported actor principal kind: authorization denies with a resolver error.

## Current Boundary

This slice wires the typed actor boundary and protocol credential admin
evaluation. A later slice should add HTTP bearer-token middleware for
admin/protocol entry points and expand route adoption beyond protocol credential
operations.
