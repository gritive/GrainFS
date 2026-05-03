# DuckDB Iceberg REST Catalog

GrainFS exposes an Iceberg REST Catalog compatible table API for DuckDB at
`/iceberg/v1`.

Single-node `grainfs serve` stores catalog namespace and table state in the
local Badger-backed metadata DB. Multi-peer cluster mode stores namespace and
table metadata pointers in meta-Raft, so clients can call `/iceberg/*` on leader
or follower nodes without creating node-local split-brain catalog state.

Table metadata JSON files remain ordinary GrainFS objects under the warehouse
bucket. Meta-Raft stores only the current namespace/table records and metadata
locations.

## Start GrainFS

```sh
grainfs serve --data ./data --port 9000
```

Create the warehouse bucket before writing table data:

```sh
curl -X PUT http://127.0.0.1:9000/grainfs-tables
```

## Attach from DuckDB

```sql
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

CREATE OR REPLACE SECRET grainfs_s3 (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'testkey',
    SECRET 'testsecret',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9000',
    USE_SSL false,
    URL_STYLE 'path'
);

ATTACH 'warehouse' AS grainfs_iceberg (
    TYPE iceberg,
    ENDPOINT 'http://127.0.0.1:9000/iceberg',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none',
    SUPPORT_STAGE_CREATE false
);
```

## Smoke Test

```sql
CREATE SCHEMA grainfs_iceberg.ns2;
CREATE TABLE grainfs_iceberg.ns2.t (a INTEGER);
INSERT INTO grainfs_iceberg.ns2.t VALUES (42);
SELECT * FROM grainfs_iceberg.ns2.t;
```

## E2E Test

Run the embedded DuckDB e2e with:

```sh
make test-e2e-iceberg
```

The single-node test starts GrainFS, creates the warehouse bucket, attaches
DuckDB through the Iceberg REST Catalog, creates and writes a table, restarts
GrainFS, reads the table again, then drops the table and namespace through
DuckDB.

Cluster coverage includes a three-node DuckDB e2e that creates the warehouse
bucket, writes through one node, appends through another node, then reads and
drops the table through a third node. Unit and server integration tests cover
meta-Raft-backed catalog state, follower-to-leader write and read forwarding,
snapshot restore, typed conflict propagation, stale metadata pointer
compare-and-swap handling, and forwarded delete-marker cleanup.
