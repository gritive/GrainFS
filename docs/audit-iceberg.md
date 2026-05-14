# Audit Log Lake — Iceberg + Parquet

GrainFS ships a built-in audit subsystem that captures S3 API events
(PutObject, GetObject, DeleteObject, CreateBucket, …) and persists them as an
Apache Iceberg v2 table partitioned by day. The table is stored inside GrainFS
itself — no external data warehouse required.

## Quick Start

Audit is **enabled by default**. No flags are required for a fresh cluster.

```bash
# Start a single node (audit on, 60-second commit interval)
./bin/grainfs serve --data ./data --port 9000

# Verify the audit table exists after the first commit cycle
aws --endpoint-url http://localhost:9000 s3 ls s3://grainfs-audit/metadata/s3/
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--audit-iceberg` | `true` | Enable / disable the audit subsystem. |
| `--audit-commit-interval` | `60s` | How often the committer flushes the ring buffer to Iceberg. Shorter values reduce data loss window; longer values reduce write amplification. |

```bash
# Disable audit entirely
./bin/grainfs serve --audit-iceberg=false ...

# Flush every 30 seconds instead of 60
./bin/grainfs serve --audit-commit-interval=30s ...
```

## Storage Layout

```
grainfs-audit/                      ← dedicated bucket, auto-created
  data/
    dt=2025-01-15/                  ← day partition
      <uuid>.parquet
  metadata/s3/
    00000-<uuid>.metadata.json      ← initial snapshot
    <seqnum>-<uuid>.metadata.json   ← per-commit metadata
    <snapshotID>-<uuid>-manifest.avro
    snap-<snapshotID>-<uuid>.avro
```

## Schema

Table: `audit.s3` (Iceberg v2, partitioned by `day(ts)`)

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `timestamptz` | Request timestamp (microseconds UTC) |
| `node_id` | `string` | GrainFS node that handled the request |
| `request_id` | `string` | Unique request ID |
| `sa_id` | `string` | IAM service account ID |
| `source_ip` | `string` | Client IP address |
| `method` | `string` | S3 operation (e.g. `PutObject`) |
| `bucket` | `string` | Target bucket |
| `key` | `string` | Object key |
| `http_status` | `int` | HTTP response status code |
| `bytes_in` | `long` | Request body bytes |
| `bytes_out` | `long` | Response body bytes |
| `latency_ms` | `int` | End-to-end latency in milliseconds |
| `err_class` | `string` | Error class if the request failed |

## Querying with DuckDB

```sql
INSTALL httpfs; INSTALL iceberg;
LOAD httpfs; LOAD iceberg;

CREATE OR REPLACE SECRET grainfs (
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
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);

-- Count operations per bucket today
SELECT bucket, method, COUNT(*) AS ops
FROM grainfs_iceberg.audit.s3
WHERE ts >= NOW() - INTERVAL 1 DAY
GROUP BY bucket, method
ORDER BY ops DESC;
```

## Cluster Behaviour

- **Leader**: drains the ring buffer + any events forwarded by followers, writes
  Parquet, and commits the Iceberg snapshot on each interval tick.
- **Follower**: drains its own ring buffer and ships events to the current leader
  over a dedicated QUIC stream (`StreamAuditShip = 0x13`).
- If the leader is unreachable, followers log events via zerolog and retry on
  the next tick. Events in the ring buffer survive leader transitions up to the
  ring capacity (65 536 events ≈ 60 s × 1 000 req/s).

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `audit_drops_total{node}` | Counter | Events dropped due to ring overflow |
| `audit_commit_lag_seconds{node}` | Histogram | Time from event creation to Iceberg commit |
| `audit_committer_state{node}` | Gauge | 1 = leader (committing), 0 = follower (shipping) |
