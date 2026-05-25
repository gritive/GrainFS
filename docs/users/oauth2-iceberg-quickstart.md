# OAuth2 Iceberg Quickstart

This guide is for clusters where S3 auth is enabled
(`iam.anon-enabled=false`). Fresh anonymous clusters serve the catalog without
auth.

## 1. Get the config bundle

```bash
grainfs iceberg config --warehouse <bucket> --sa <id>
```

Output (5 fields, client-agnostic):

```
catalog_uri:       http://localhost:9000/iceberg
oauth_token_uri:   http://localhost:9000/iceberg/v1/oauth/tokens
warehouse:         analytics
client_id:         GRAIN…
client_secret:     …
```

Use `--no-reveal` to skip secret printing (e.g., in CI logs).

## 2a. DuckDB

```sql
INSTALL iceberg;
LOAD iceberg;
CREATE SECRET grain_sec (
  TYPE iceberg,
  CLIENT_ID '<client_id>',
  CLIENT_SECRET '<client_secret>',
  OAUTH2_SERVER_URI '<oauth_token_uri>'
);
ATTACH '<catalog_uri>' AS gr (TYPE iceberg, SECRET grain_sec);
```

## 2b. Trino

```properties
connector.name = iceberg
iceberg.catalog.type = rest
iceberg.rest-catalog.uri = <catalog_uri>
iceberg.rest-catalog.oauth2.token-uri = <oauth_token_uri>
iceberg.rest-catalog.oauth2.credential = <client_id>:<client_secret>
iceberg.rest-catalog.warehouse = <warehouse>
```

## 2c. Spark

```scala
spark.sql.catalog.gr = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.gr.type = rest
spark.sql.catalog.gr.uri = <catalog_uri>
spark.sql.catalog.gr.oauth2-server-uri = <oauth_token_uri>
spark.sql.catalog.gr.credential = <client_id>:<client_secret>
spark.sql.catalog.gr.warehouse = <warehouse>
```

## 2d. PyIceberg

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog("gr",
    **{"type": "rest",
       "uri": "<catalog_uri>",
       "oauth2-server-uri": "<oauth_token_uri>",
       "credential": "<client_id>:<client_secret>",
       "warehouse": "<warehouse>"})
```

## 2e. warp

```bash
warp iceberg --external-catalog=polaris \
  --host=localhost:9000 --warehouse=<warehouse> \
  --access-key=<client_id> --secret-key=<client_secret>
```

## Token lifecycle

- Tokens are valid for 3600 seconds (1 hour).
- All clients auto-mint a new token on expiry — you don't need to refresh manually.
- One token = one warehouse. To work with N warehouses, create N secrets / catalogs.
