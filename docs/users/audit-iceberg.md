# Audit Log Lake: Iceberg + Parquet

`GrainFS` stores S3 audit logs in its own `grainfs-audit` bucket as the Iceberg
table `audit.s3`. Operators can use the table for incident review, compliance
sampling, dashboard health checks, and DuckDB analysis without an external
warehouse.

## Capture Guarantees

Audit capture is request-envelope based:

1. The S3 middleware derives the request identity, target resource, operation,
   subresource, request id, source IP, and user agent.
2. Before authz and handler execution, it writes a durable attempt to the local
   Badger-backed audit outbox.
3. After the handler produces the response, it finalizes the same outbox row with status,
   bytes, latency, auth result, and error reason.
4. The audit committer drains durable rows into `audit.s3` and acks them only
   after a successful leader commit or follower ship.

If the initial durable outbox append fails, the request fails closed with HTTP
`503`. This is intentional: a request that cannot be durably audited should not
silently mutate or read user data.

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--audit-iceberg` | `true` | Enable the audit subsystem. |
| `--audit-commit-interval` | `60s` | Interval for flushing outbox rows to Iceberg. |

`GrainFS` bootstraps the audit bucket and Iceberg table. During bootstrap, it
migrates older audit metadata in place to the current schema and partition spec.

## Storage Layout

```text
grainfs-audit/
  data/
    2026-05-15/
      <uuid>.parquet
  metadata/s3/
    00000-<uuid>.metadata.json
    <seq>-<uuid>.metadata.json
    <snapshot-id>-<uuid>-manifest.avro
    snap-<snapshot-id>-<uuid>.avro
```

`grainfs-audit` is an internal bucket. `GrainFS` blocks S3 writes, bucket-level
reads, and listing. It allows object-level `GET` and `HEAD` only for the
server's loopback DuckDB reader when that reader signs requests with the
generated internal audit credential.

## Schema

Table: `audit.s3` (Iceberg v2)

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Request timestamp. |
| `node_id` | `string` | `GrainFS` node that handled the request. |
| `request_id` | `string` | Request id returned in S3 response headers. |
| `sa_id` | `string` | IAM service account id, when authenticated. |
| `source_ip` | `string` | Client IP address. |
| `method` | `string` | HTTP method. |
| `bucket` | `string` | Target bucket. |
| `key` | `string` | Target object key. |
| `http_status` | `int` | Final HTTP response status. |
| `bytes_in` | `long` | Request body bytes read. |
| `bytes_out` | `long` | Response bytes written. |
| `latency_ms` | `int` | Request latency in milliseconds. |
| `err_class` | `string` | S3 error class/code. |
| `event_id` | `string` | Durable audit event id. |
| `user_agent` | `string` | Request user agent. |
| `operation` | `string` | Classified S3 operation, for example `PutObject`. |
| `subresource` | `string` | Relevant S3 subresource, for example `uploads`. |
| `auth_status` | `string` | `allow`, `deny`, `error`, `incomplete`, or request-envelope status. |
| `err_reason` | `string` | Internal reason such as `internal_bucket` or authz denial reason. |
| `version_id` | `string` | Object version id when available. |
| `upload_id` | `string` | Multipart upload id when available. |
| `copy_source_bucket` | `string` | Source bucket for copy operations. |
| `copy_source_key` | `string` | Source key for copy operations. |

The table has a hidden `day(ts)` partition field named `ts_day`. New metadata
uses this partition spec, and migration upgrades older unpartitioned metadata.
DuckDB e2e coverage verifies date-bounded predicates against fresh audit
tables.

## Dashboard And API

The dashboard has an `Audit` tab with:

- guarantee state from `/api/audit/health`;
- durable outbox backlog and oldest pending timestamp;
- bounded search controls for bucket, key prefix, service account, request id,
  and HTTP status class.

APIs:

| Endpoint | Description |
|----------|-------------|
| `GET /api/audit/health` | Reports whether audit is enabled, guarantee state, outbox backlog, and oldest pending timestamp. |
| `GET /api/audit/s3` | Runs a bounded, server-generated audit search query. It never accepts raw SQL. |

Supported `/api/audit/s3` filters include `since`, `until`, `bucket`,
`key_prefix`, `sa_id`, `operation`, `status`, `status_class`, `err_class`,
`request_id`, and `limit`. `GrainFS` clamps `limit` to 500 and runs each query with
a 10 second context timeout. The audit API listens on localhost for the bundled
dashboard or local operator tooling.

The search endpoint requires a configured audit searcher. Unit tests use a mock
searcher; production wiring should provide a DuckDB searcher with an endpoint
and internal credentials that can read `grainfs-audit` object paths through the
local S3 API.
Without that wiring, the endpoint returns `503` and the dashboard marks search
as unavailable while health cards still work.

## DuckDB Analysis

DuckDB remains the advanced analysis path for ad hoc investigation:

```sql
INSTALL httpfs;
INSTALL iceberg;
LOAD httpfs;
LOAD iceberg;

CREATE OR REPLACE SECRET grainfs_s3 (
  TYPE s3,
  KEY_ID 'YOUR_ACCESS_KEY',
  SECRET 'YOUR_SECRET_KEY',
  REGION 'us-east-1',
  ENDPOINT 'localhost:9000',
  URL_STYLE 'path',
  USE_SSL false
);

ATTACH 'grainfs' AS grainfs_iceberg (
  TYPE iceberg,
  ENDPOINT 'http://localhost:9000/iceberg',
  AUTHORIZATION_TYPE 'sigv4',
  SIGV4_REGION 'us-east-1',
  SIGV4_SERVICE 's3'
);

SELECT bucket, operation, http_status, COUNT(*) AS requests
FROM grainfs_iceberg.audit.s3
WHERE ts >= NOW() - INTERVAL 1 DAY
GROUP BY bucket, operation, http_status
ORDER BY requests DESC;
```

## Cluster Behavior

- Leaders commit their durable outbox rows and any follower-shipped rows to
  Iceberg, then ack only committed event ids.
- Followers keep rows in their local durable outbox until `ShipToLeader`
  succeeds.
- On leader loss, followers keep unacked local rows pending and retry them after
  a new leader becomes available.

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `audit_drops_total{node="<node-id>"}` | Counter | Legacy ring/follower drop counter. Should stay at 0 in outbox-backed steady state. |
| `audit_commit_lag_seconds{node="<node-id>"}` | Histogram | Time from event creation to Iceberg commit. |
| `audit_committer_state{node="<node-id>"}` | Gauge | 1 = leader committing, 0 = follower shipping. |
| `audit_outbox_backlog` | Gauge | Durable audit events pending commit. |
| `audit_outbox_oldest_pending_us` | Gauge | Oldest pending event timestamp in Unix microseconds. |
