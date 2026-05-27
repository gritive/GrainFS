# Unified Protocol Credentials Design

## Goal

Unify GrainFS authentication around one IAM principal model while preserving existing protocol behavior. This foundation PR introduces a protocol credential domain, admin API shape, CLI shape, and documentation so S3, Iceberg, NFS, 9P, NBD, and future NVMe/TCP can converge without a risky all-at-once protocol cutover.

## Problem

GrainFS currently has several authentication surfaces:

- S3 uses ServiceAccounts, access keys, and IAM policy evaluation.
- Iceberg reuses ServiceAccounts through OAuth2 client credentials and bearer tokens.
- NFS and 9P use MountSA as a separate principal pool with `grainfs:NFSMount` and `grainfs:9PAttach`.
- NBD exposes volumes by export name and has no unified IAM credential path.

This fragmentation makes it hard to answer one operator question: which principal can access which resource, through which protocol, with which mode, and with which credential?

## Decision

Use `ServiceAccount` as the long-term principal for every protocol. Add a new protocol credential domain that issues protocol-specific credentials for an SA, resource, and access mode.

Protocol credentials are not the IAM policy engine. They are the credential material created after the control plane validates the requested principal/resource/protocol/mode shape. Future PRs will wire issuance to IAM authorization and wire data-plane protocol servers to validate these credentials.

## First PR Scope

This PR is a foundation slice.

It includes:

- `internal/protocred` domain model and in-memory service/store.
- Admin API request/response types and handlers for create/list/get/rotate/revoke.
- CLI skeleton under `grainfs credential`.
- IAM action names reserved for protocol credential management.
- Documentation for the unified model and migration direction.

It does not include:

- NBD enforcement.
- MountSA removal.
- Replacing existing S3 access key storage.
- Replacing Iceberg OAuth token issuance.
- Changing NFS or 9P mount wire behavior.
- Persisting protocol credentials through Raft snapshots.

Persistence and protocol enforcement are follow-up PRs. Keeping this PR non-enforcing prevents a partial credential implementation from breaking live protocol clients.

## Domain Model

### Principal

The principal is a ServiceAccount ID.

MountSA remains supported for NFS/9P during migration, but the new model treats it as legacy. New protocol credentials should be issued to ServiceAccounts.

### Resource

Resources are normalized strings:

```text
bucket/<name>
volume/<name>
catalog/<warehouse>
```

This PR validates only the prefix and non-empty name. It does not add a new ARN system.

### Protocol

Supported enum values:

```text
s3
iceberg
nfs
9p
nbd
```

The model intentionally leaves room for:

```text
nvme-tcp
```

### Mode

Supported enum values:

```text
ro
rw
```

Mode maps to protocol-specific meaning:

- `ro`: read/list/stat/attach-readonly.
- `rw`: read plus write/mutation.

The first PR stores mode but does not enforce it on protocol servers.

### Credential

Stored record:

```text
id
sa_id
protocol
resource
mode
secret_hash
secret_hint
created_at
created_by
expires_at nullable
revoked_at nullable
last_used_at nullable
```

Returned secret:

```text
credential_id
secret
connection_hint
```

The plaintext secret is returned only on create and rotate. List/get responses never include it.

## Protocol Shapes

Each protocol has a protocol-specific connection hint. This PR defines the shape but does not require existing clients to use it yet.

### S3

```text
credential:
  access_key_id
  secret_access_key
```

Existing SA access keys remain the live S3 mechanism. The new credential model documents how S3 credentials fit the unified inventory.

### Iceberg

```text
credential:
  client_id
  client_secret
  oauth_token_uri
```

Existing OAuth2 flow remains live. The unified model aligns Iceberg credentials with the same SA principal.

### NFS

```text
credential:
  mount_principal
  mount_path_hint: <bucket>/<credential-id>
```

Existing MountSA mount paths remain live. Future work maps NFS mount credentials to ServiceAccounts.

### 9P

```text
credential:
  aname_hint: <credential-id>@<bucket>
```

Existing `aname=<mount-sa>@<bucket>` remains live. Future work maps 9P attach credentials to ServiceAccounts.

### NBD

```text
credential:
  export_name: <volume>@<secret>
```

NBD is the first likely protocol to enforce the new model because it has no strong existing authentication model.

## Admin API

New endpoints:

```text
POST   /v1/credentials
GET    /v1/credentials
GET    /v1/credentials/:id
POST   /v1/credentials/:id/rotate
DELETE /v1/credentials/:id
```

Create request:

```json
{
  "sa_id": "node-a",
  "protocol": "nbd",
  "resource": "volume/devdisk",
  "mode": "rw",
  "expires_at": "2026-06-01T00:00:00Z"
}
```

Create response:

```json
{
  "id": "pc_...",
  "sa_id": "node-a",
  "protocol": "nbd",
  "resource": "volume/devdisk",
  "mode": "rw",
  "secret": "pcsec_...",
  "connection_hint": {
    "export_name": "devdisk@pcsec_..."
  }
}
```

List/get responses include metadata and connection hints that do not reveal plaintext secrets.

## CLI

New command group:

```bash
grainfs credential create --sa node-a --protocol nbd --resource volume/devdisk --mode rw
grainfs credential list [--sa node-a] [--protocol nbd]
grainfs credential get <credential-id>
grainfs credential rotate <credential-id>
grainfs credential revoke <credential-id>
```

Text output for create should be directly useful to operators:

```text
Credential: pc_...
SA: node-a
Protocol: nbd
Resource: volume/devdisk
Mode: rw
Export name: devdisk@pcsec_...
```

JSON output should mirror the admin response.

## IAM Actions

Reserve these action names:

```text
grainfs:CredentialCreate
grainfs:CredentialList
grainfs:CredentialRead
grainfs:CredentialRotate
grainfs:CredentialRevoke
```

Reserve these resource access actions for future protocol enforcement:

```text
grainfs:BucketRead
grainfs:BucketWrite
grainfs:BucketMount
grainfs:VolumeAttach
grainfs:VolumeRead
grainfs:VolumeWrite
grainfs:CatalogRead
grainfs:CatalogWrite
```

In this foundation PR, admin UDS remains the caller authentication boundary for credential management. IAM authorization for credential issuance is a follow-up once the admin request identity path is available.

## Migration Plan

1. Add protocol credential inventory and CLI without changing data-plane behavior.
2. Add NBD enforcement using `volume@secret` export names.
3. Add NFS/9P ServiceAccount-backed mount credentials while keeping MountSA compatibility.
4. Teach docs and CLI to prefer `grainfs credential create` over protocol-specific credential commands.
5. Eventually deprecate MountSA as a separate principal pool.

## Errors

Validation errors return `invalid`.

Missing credentials return `not_found`.

Operations on revoked credentials return `conflict`.

Disabled service dependencies return `unsupported`.

Credential secrets are never logged. Logs may include credential ID, SA ID, protocol, resource, and mode.

## Testing

Foundation PR tests:

- `internal/protocred` unit tests for create/list/get/rotate/revoke, no-expiry, expiry, secret hashing, and connection hint shape.
- Admin handler tests for wire shape and error mapping.
- CLI tests for text and JSON output.
- IAM policy validation tests proving new `grainfs:Credential*` actions do not fit the MountSA-only policy namespace.
- Docs/reference tests.

Protocol enforcement tests are deferred to the NBD/NFS/9P integration PRs.

## Follow-Ups

- Persist protocol credentials through MetaRaft and snapshots.
- Enforce NBD credentials during export negotiation.
- Add IAM authorization to credential issuance.
- Add ServiceAccount-backed NFS/9P mount credentials.
- Add `grainfs doctor auth` coverage for protocol credentials.
