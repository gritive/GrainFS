# First Iceberg Query

This guide points DuckDB at GrainFS through the Iceberg REST Catalog.

## Start GrainFS

```bash
./grainfs serve --data ./tmp --port 9000
```

## Create credentials

Use the admin socket to create a service account and bucket policy before using authenticated Iceberg clients. The exact command flow is covered in [Configure OAuth2 Iceberg](../how-to/iceberg/configure-oauth2-iceberg.md).

## Query with DuckDB

Follow the tested workflow in [DuckDB Iceberg tutorial](../tutorials/duckdb-iceberg.md).

## Verify support

Use [Iceberg compatibility](../reference/iceberg-compatibility.md) for supported client behavior.
