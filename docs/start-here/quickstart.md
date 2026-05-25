# Quickstart

This path gets a local GrainFS node running and verifies the S3 API with one object.

## What you need

- A built `grainfs` binary or `go run ./cmd/grainfs` from the repository root.
- AWS CLI for the S3 smoke test.

## Start a local node

```bash
./grainfs serve --data ./tmp --port 9000
```

If you are running from source:

```bash
go run ./cmd/grainfs serve --data ./tmp --port 9000
```

## Write and list an object

In another terminal:

```bash
echo "hello grainfs" > file.txt
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp file.txt s3://default/
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/
```

A fresh Phase 0 node accepts unsigned requests on the S3 listener. Create a service account when you want authenticated S3 access. See [identity and admin socket](../concepts/identity-and-admin-socket.md).

## Next steps

- Mount the same bucket with [NFS](first-nfs-mount.md).
- Query Iceberg metadata with [DuckDB](first-iceberg-query.md).
- Review protocol limits in the [compatibility references](../reference/s3-compatibility.md).
