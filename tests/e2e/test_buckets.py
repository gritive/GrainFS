import pytest
from botocore.exceptions import ClientError


@pytest.mark.e2e
class TestBuckets:
    def test_create_bucket(self, s3):
        resp = s3.create_bucket(Bucket="new-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        s3.delete_bucket(Bucket="new-bucket")

    def test_head_bucket(self, s3, bucket):
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_head_bucket_not_found(self, s3):
        with pytest.raises(ClientError) as exc:
            s3.head_bucket(Bucket="nonexistent")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_create_bucket_conflict(self, s3, bucket):
        with pytest.raises(ClientError) as exc:
            s3.create_bucket(Bucket=bucket)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

    def test_list_buckets(self, s3, bucket):
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert bucket in names

    def test_delete_bucket(self, s3):
        s3.create_bucket(Bucket="to-delete")
        resp = s3.delete_bucket(Bucket="to-delete")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_delete_bucket_not_empty(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="file.txt", Body=b"data")
        with pytest.raises(ClientError) as exc:
            s3.delete_bucket(Bucket=bucket)
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
