# `GrainFS` Iceberg Compatibility Matrix

This document summarizes `GrainFS` Iceberg REST Catalog compatibility. It is not a
general Apache Iceberg engine support statement.

`Supported` means the behavior has e2e, conformance, or real client integration
coverage. Unit-test-only coverage is not enough.

## Status Definitions

| Status        | Meaning                                                                 |
| ------------- | ----------------------------------------------------------------------- |
| Supported     | Covered by e2e, conformance, or real client integration tests.          |
| Partial       | Integration-tested, but with known semantic or scope limits.            |
| Not supported | `GrainFS` does not implement or claim this compatibility surface. |
| Not planned   | Intentionally outside the product scope.                                |

## REST Catalog Surface

| Area                    | Surface                            | Status     | Notes                                                                                               |
| ----------------------- | ---------------------------------- | ---------- | --------------------------------------------------------------------------------------------------- |
| Client                  | DuckDB Iceberg REST Catalog        | Supported  |                                                                                                     |
| Client                  | MinIO `warp iceberg`               | Supported  | Benchmark smoke coverage runs through `make bench-iceberg-table` and `make bench-iceberg-table-cluster`. |
| Endpoint                | `/iceberg/v1/config`               | Supported  |                                                                                                     |
| Endpoint                | `/_iceberg/v1/config`              | Supported  | AIStor-compatible alias used by `warp iceberg`.                                                     |
| Namespace               | List/create/get/head namespace     | Supported  |                                                                                                     |
| Namespace               | Warehouse create/delete endpoints  | Supported  | No-op compatibility endpoints for clients that provision warehouses before namespace/table work.     |
| Table                   | List/create/get/head table         | Supported  |                                                                                                     |
| Commit                  | Transaction commit                 | Supported  |                                                                                                     |
| Errors                  | Iceberg JSON errors                | Supported  | Returns Iceberg JSON errors; S3 XML errors are not reused.                                          |
| Any-node cluster access | Leader and follower API calls      | Supported  | Catalog requests can enter through different cluster nodes.                                         |
| Access delegation       | `ACCESS_DELEGATION_MODE none`      | Supported  | Delegated access is not claimed.                                                                    |
| Stage create            | `SUPPORT_STAGE_CREATE false`       | Supported  | Stage create is disabled.                                                                           |
| Spark                   | Spark Iceberg REST Catalog client  | Not supported | No compatibility claim without real-client coverage.                                                |
| Trino                   | Trino Iceberg REST Catalog client  | Not supported | No compatibility claim without real-client coverage.                                                |
| PyIceberg               | PyIceberg REST Catalog client      | Not supported | No compatibility claim without real-client coverage.                                                |
