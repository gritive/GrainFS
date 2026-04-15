import pytest
from botocore.exceptions import ClientError


@pytest.mark.e2e
class TestObjects:
    def test_put_and_get_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello grainfs", ContentType="text/plain")

        resp = s3.get_object(Bucket=bucket, Key="hello.txt")
        assert resp["Body"].read() == b"hello grainfs"
        assert resp["ContentType"] == "text/plain"
        assert resp["ContentLength"] == 13

    def test_head_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="meta.txt", Body=b"metadata test")

        resp = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert resp["ContentLength"] == 13
        assert "ETag" in resp

    def test_head_object_not_found(self, s3, bucket):
        with pytest.raises(ClientError) as exc:
            s3.head_object(Bucket=bucket, Key="nope.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_delete_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="to-delete.txt", Body=b"gone")
        s3.delete_object(Bucket=bucket, Key="to-delete.txt")

        with pytest.raises(ClientError) as exc:
            s3.head_object(Bucket=bucket, Key="to-delete.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_delete_nonexistent_object(self, s3, bucket):
        # S3 spec: deleting nonexistent returns 204, not error
        resp = s3.delete_object(Bucket=bucket, Key="never-existed.txt")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_put_to_nonexistent_bucket(self, s3):
        with pytest.raises(ClientError) as exc:
            s3.put_object(Bucket="nope", Key="file.txt", Body=b"data")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_get_nonexistent_object(self, s3, bucket):
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=bucket, Key="nope.txt")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_overwrite_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="file.txt", Body=b"v1")
        s3.put_object(Bucket=bucket, Key="file.txt", Body=b"version2")

        resp = s3.get_object(Bucket=bucket, Key="file.txt")
        assert resp["Body"].read() == b"version2"
        assert resp["ContentLength"] == 8

    def test_nested_key(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="path/to/deep/file.txt", Body=b"nested")

        resp = s3.get_object(Bucket=bucket, Key="path/to/deep/file.txt")
        assert resp["Body"].read() == b"nested"

    def test_list_objects(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="docs/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="docs/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="images/c.png", Body=b"c")

        resp = s3.list_objects(Bucket=bucket)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert len(keys) == 3

    def test_list_objects_with_prefix(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="docs/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="docs/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="images/c.png", Body=b"c")

        resp = s3.list_objects(Bucket=bucket, Prefix="docs/")
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        assert len(keys) == 2
        assert all(k.startswith("docs/") for k in keys)

    def test_large_object(self, s3, bucket):
        data = b"X" * (5 * 1024 * 1024)  # 5MB
        s3.put_object(Bucket=bucket, Key="large.bin", Body=data)

        resp = s3.get_object(Bucket=bucket, Key="large.bin")
        assert resp["ContentLength"] == len(data)
        assert resp["Body"].read() == data
