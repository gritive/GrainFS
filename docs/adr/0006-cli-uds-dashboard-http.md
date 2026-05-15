# ADR 0006: CLI Uses Admin UDS, Dashboard Uses HTTP

**Date:** 2026-05-07
**Status:** Accepted

## Context

The `GrainFS` cluster CLI (`grainfs cluster status|peers|events|remove-peer`) used
HTTP endpoints such as `http://127.0.0.1:9000`, while the volume CLI
(`grainfs volume *`) already used the admin Unix socket
(`<data-dir>/admin.sock`). That made the endpoint scheme inconsistent between
the two admin CLI groups. At the same time, the dashboard web UI (`/ui/`) called
HTTP `/api/cluster/*` and `/api/eventlog` routes directly, so the same routes
served both CLI and dashboard traffic.

## Decision

Separate CLI and dashboard endpoint schemes clearly.

- **CLI uses admin UDS.** `grainfs cluster *`, except `rotate-key`, calls
  `/v1/cluster/*` routes through the admin Unix socket. This matches
  `grainfs volume *` and `grainfs dashboard`.
- **Dashboard uses HTTP.** The web UI continues to call HTTP `/api/cluster/*`
  and `/api/eventlog`. UDS is not appropriate for an externally reachable
  dashboard, which is protected by explicit authentication such as presigned
  tokens or S3 authentication.
- **Handlers have one source.** `*server.Server` methods such as
  `clusterStatus`, `removePeerHandler`, and `queryEventLog` are defined once
  and registered on admin UDS Hertz through `RegisterClusterAdminUDS`. HTTP
  `/api/...` and UDS `/v1/cluster/...` point at the same methods.
- **rotate-key keeps a separate socket and moves to Hertz HTTP.**
  `cluster rotate-key` keeps `rotate.sock` at mode 0600 for owner-only access,
  but migrates from line-delimited JSON to Hertz HTTP routes
  `/v1/rotate-key/{status,begin,abort}`. `rotate.sock` remains separate from
  `admin.sock` (0660 plus group) so PSK material is not exposed to other
  operators in the admin group.

## Consequences

**Positive**:

- Volume and cluster CLI endpoint UX match: `--endpoint <admin.sock>`.
- Authorization uses UDS file permissions, mode 0660 plus group, so duplicated
  `localhostOnly` middleware is no longer needed for these CLI paths.
- The dashboard keeps working through the existing HTTP routes.
- Operators get a simple rule: admin CLI commands dial admin UDS; dashboard
  browser traffic uses HTTP.

**Negative**:

- Breaking change: `grainfs cluster --endpoint http://...` no longer works.
  Operator scripts must migrate. CHANGELOG and the runbook document the change.
- The `cluster join` root also moves in the same release, although it is a
  separate decision.

**Out of scope**:

- Merging `rotate.sock` into `admin.sock`; the separate socket is an intentional
  security boundary.
- Removing HTTP `/api/cluster/*`; the dashboard depends on those routes.

## References

- Existing admin UDS pattern: `internal/volumeadmin/`, `internal/server/admin/`.
- Single-source handler methods: `(*server.Server).clusterStatus`
  (`internal/server/handlers.go`), `(*server.Server).removePeerHandler`
  (`internal/server/handlers.go`), and `(*server.Server).queryEventLog`
  (`internal/server/events_api.go`).
