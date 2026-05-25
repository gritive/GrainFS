# First S3 Object

This guide writes one object through the S3-compatible API.

## Start GrainFS

```bash
./grainfs serve --data ./tmp --port 9000
```

## Upload and list

```bash
echo "hello grainfs" > file.txt
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp file.txt s3://default/
aws --no-sign-request --endpoint-url http://localhost:9000 s3 ls s3://default/
```

## Verify behavior

The listing should include `file.txt`. For supported operations and known gaps, see [S3 compatibility](../reference/s3-compatibility.md).
