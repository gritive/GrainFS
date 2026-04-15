import pytest


@pytest.mark.e2e
class TestMultipartUpload:
    def test_multipart_upload_complete(self, s3, bucket):
        key = "multipart-file.bin"
        part1_data = b"A" * 1024
        part2_data = b"B" * 512

        # Initiate
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
        upload_id = resp["UploadId"]
        assert upload_id

        # Upload parts
        p1 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1_data)
        p2 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2_data)

        # Complete
        resp = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": p1["ETag"]},
                    {"PartNumber": 2, "ETag": p2["ETag"]},
                ]
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify assembled object
        get_resp = s3.get_object(Bucket=bucket, Key=key)
        body = get_resp["Body"].read()
        assert body == part1_data + part2_data
        assert get_resp["ContentLength"] == len(part1_data) + len(part2_data)

    def test_multipart_upload_abort(self, s3, bucket):
        key = "aborted.bin"

        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]

        s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"data")

        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

        # Object should not exist
        with pytest.raises(s3.exceptions.NoSuchKey):
            s3.get_object(Bucket=bucket, Key=key)

    def test_multipart_three_parts(self, s3, bucket):
        key = "three-parts.bin"
        parts_data = [b"X" * 256, b"Y" * 512, b"Z" * 128]

        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]

        parts = []
        for i, data in enumerate(parts_data, 1):
            p = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=i, Body=data)
            parts.append({"PartNumber": i, "ETag": p["ETag"]})

        s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )

        get_resp = s3.get_object(Bucket=bucket, Key=key)
        expected = b"".join(parts_data)
        assert get_resp["Body"].read() == expected
