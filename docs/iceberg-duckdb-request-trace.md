# DuckDB Iceberg REST Request Trace

Captured for the GrainFS Iceberg REST Catalog implementation.

Date: 2026-05-02
DuckDB CLI: `v1.5.2 (Variegata) 8a5851971f`
Platform: `Darwin arm64`
Extensions loaded: `iceberg`, `httpfs`

## Catalog Attach Options

```sql
ATTACH 'warehouse' AS grainfs_iceberg (
    TYPE iceberg,
    ENDPOINT 'http://127.0.0.1:18082',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none',
    SUPPORT_STAGE_CREATE false
);
```

The official DuckDB 1.5 docs list these options for REST Catalog attach:

- `AUTHORIZATION_TYPE`: default `OAUTH2`, use `none` for unauthenticated catalogs.
- `ACCESS_DELEGATION_MODE`: default `vended_credentials`, use `none` when the catalog does not vend credentials.
- `SUPPORT_STAGE_CREATE`: default `false`.

Source: https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs

## Observed: Attach + Show Tables

SQL:

```sql
LOAD iceberg;
LOAD httpfs;
ATTACH 'warehouse' AS grainfs_iceberg (
    TYPE iceberg,
    ENDPOINT 'http://127.0.0.1:18082',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none',
    SUPPORT_STAGE_CREATE false
);
SHOW ALL TABLES;
```

HTTP sequence:

```text
GET /v1/config?warehouse=warehouse
  User-Agent: duckdb/v1.5.2(osx_arm64) cli 8a5851971f
  Body: <empty>

GET /v1/namespaces
  User-Agent: duckdb/v1.5.2(osx_arm64) cli 8a5851971f
  Body: <empty>

GET /v1/namespaces/default/tables
  User-Agent: duckdb/v1.5.2(osx_arm64) cli 8a5851971f
  Body: <empty>

GET /v1/namespaces/ns1/tables
  User-Agent: duckdb/v1.5.2(osx_arm64) cli 8a5851971f
  Body: <empty>
```

Notes:

- `GET /v1/config` includes the `warehouse` attach string as a query parameter.
- `SHOW ALL TABLES` lists namespaces first, then lists tables for each namespace.

## Observed: Create Schema

SQL:

```sql
CREATE SCHEMA grainfs_iceberg.ns2;
```

HTTP sequence:

```text
GET /v1/namespaces/ns2
  Body: <empty>
  Expected response for missing namespace: 404 NoSuchNamespaceException

POST /v1/namespaces
  Content-Type: application/json
  Body:
  {
    "namespace": ["ns2"],
    "properties": {}
  }
```

Notes:

- DuckDB checks namespace existence before creating it.
- The implementation must return a typed Iceberg JSON error for missing namespaces, not an S3 XML error.

## Observed: Create Table

SQL:

```sql
CREATE TABLE grainfs_iceberg.ns2.t (a INTEGER);
```

HTTP sequence:

```text
GET /v1/namespaces/ns2/tables/t
  Body: <empty>
  Expected response for missing table: 404 NoSuchTableException

GET /v1/namespaces/ns2
  Body: <empty>

GET /v1/namespaces/ns2/tables/t
  Body: <empty>
  Expected response for missing table: 404 NoSuchTableException

GET /v1/namespaces/ns2
  Body: <empty>

POST /v1/namespaces/ns2/tables
  Content-Type: application/json
  Body:
  {
    "stage-create": false,
    "name": "t",
    "schema": {
      "type": "struct",
      "fields": [
        {
          "name": "a",
          "id": 1,
          "type": "int",
          "required": false
        }
      ],
      "schema-id": 0
    },
    "partition-spec": {
      "spec-id": 0,
      "type": "struct",
      "fields": []
    },
    "write-order": {
      "order-id": 0,
      "fields": []
    },
    "properties": {
      "format-version": "2"
    }
  }

POST /v1/transactions/commit
  Content-Type: application/json
  Body:
  {
    "table-changes": [
      {
        "requirements": [],
        "updates": [
          {
            "action": "assign-uuid",
            "uuid": "ns2-t-uuid"
          },
          {
            "action": "upgrade-format-version",
            "format-version": 2
          },
          {
            "action": "add-schema",
            "last-column-id": 1,
            "schema": {
              "type": "struct",
              "schema-id": 0,
              "fields": [
                {
                  "id": 1,
                  "name": "a",
                  "required": false,
                  "type": "int"
                }
              ],
              "identifier-field-ids": []
            }
          },
          {
            "action": "set-current-schema",
            "schema-id": 0
          },
          {
            "action": "add-spec",
            "spec": {
              "spec-id": 0,
              "fields": []
            }
          },
          {
            "action": "set-default-spec",
            "spec-id": 0
          },
          {
            "action": "add-sort-order",
            "sort-order": {
              "order-id": 0,
              "fields": []
            }
          },
          {
            "action": "set-default-sort-order",
            "sort-order-id": 0
          },
          {
            "action": "set-location",
            "location": "s3://grainfs-tables/warehouse/ns2/t"
          },
          {
            "action": "set-properties",
            "updates": {}
          }
        ],
        "identifier": {
          "name": "t",
          "namespace": ["ns2"]
        }
      }
    ]
  }
```

Important correction to the design:

- DuckDB 1.5.2 sends table creation through `POST /v1/transactions/commit` after `POST /v1/namespaces/{namespace}/tables`.
- GrainFS Phase 1 must implement `POST /v1/transactions/commit`; a table-specific commit endpoint alone is not enough.

## Implementation Implications

Minimum Phase 1 catalog routes for DuckDB attach/show/create:

```text
GET  /iceberg/v1/config
GET  /iceberg/v1/namespaces
POST /iceberg/v1/namespaces
GET  /iceberg/v1/namespaces/{namespace}
HEAD /iceberg/v1/namespaces/{namespace}
GET  /iceberg/v1/namespaces/{namespace}/tables
POST /iceberg/v1/namespaces/{namespace}/tables
GET  /iceberg/v1/namespaces/{namespace}/tables/{table}
HEAD /iceberg/v1/namespaces/{namespace}/tables/{table}
POST /iceberg/v1/transactions/commit
```

Error responses must be Iceberg JSON errors. S3 XML error helpers must not be reused.

## Trace Limitations

This trace used an instrumented mock REST Catalog to capture DuckDB's catalog request sequence. It did not complete `INSERT` or `SELECT` against real table data because that requires a real S3 warehouse endpoint, valid Iceberg metadata files, and Parquet data files. Those are Phase 2 e2e requirements after the Phase 1 catalog contract is implemented.
