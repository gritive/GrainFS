"""
boto3 S3 compatibility E2E tests for GrainFS.

Usage:
    # Start GrainFS first:
    #   ./bin/grainfs serve --data /tmp/grainfs-boto3-test --port 9100
    #
    # Then run:
    #   GRAINFS_ENDPOINT=http://127.0.0.1:9100 python3 tests/e2e/test_boto3.py

Requires: boto3 (install via: uv pip install boto3)
"""

import io
import os
import sys
import hashlib

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("GRAINFS_ENDPOINT", "http://127.0.0.1:9100")

def make_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )

def test_bucket_crud(s3):
    bucket = "boto3-test-crud"

    # Create
    s3.create_bucket(Bucket=bucket)
    print(f"  ✓ create_bucket({bucket})")

    # Head
    s3.head_bucket(Bucket=bucket)
    print(f"  ✓ head_bucket({bucket})")

    # List (should contain our bucket)
    resp = s3.list_buckets()
    names = [b["Name"] for b in resp["Buckets"]]
    assert bucket in names, f"bucket {bucket} not in {names}"
    print(f"  ✓ list_buckets (found {bucket})")

    # Delete
    s3.delete_bucket(Bucket=bucket)
    print(f"  ✓ delete_bucket({bucket})")

def test_object_crud(s3):
    bucket = "boto3-test-objects"
    s3.create_bucket(Bucket=bucket)

    # PUT
    body = b"hello from boto3"
    s3.put_object(Bucket=bucket, Key="greeting.txt", Body=body, ContentType="text/plain")
    print("  ✓ put_object")

    # GET
    resp = s3.get_object(Bucket=bucket, Key="greeting.txt")
    data = resp["Body"].read()
    assert data == body, f"expected {body!r}, got {data!r}"
    assert resp["ContentType"] == "text/plain"
    print("  ✓ get_object (body matches)")

    # HEAD
    resp = s3.head_object(Bucket=bucket, Key="greeting.txt")
    assert resp["ContentLength"] == len(body)
    print("  ✓ head_object")

    # LIST
    resp = s3.list_objects_v2(Bucket=bucket)
    keys = [o["Key"] for o in resp.get("Contents", [])]
    assert "greeting.txt" in keys
    print(f"  ✓ list_objects_v2 (found {len(keys)} objects)")

    # DELETE
    s3.delete_object(Bucket=bucket, Key="greeting.txt")
    print("  ✓ delete_object")

    # Verify deleted
    try:
        s3.head_object(Bucket=bucket, Key="greeting.txt")
        assert False, "expected 404"
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    print("  ✓ head_object after delete → 404")

    # Cleanup
    s3.delete_bucket(Bucket=bucket)

def test_large_object(s3):
    bucket = "boto3-test-large"
    s3.create_bucket(Bucket=bucket)

    # 1MB object
    data = os.urandom(1024 * 1024)
    md5 = hashlib.md5(data).hexdigest()

    s3.put_object(Bucket=bucket, Key="large.bin", Body=data)
    print("  ✓ put_object (1MB)")

    resp = s3.get_object(Bucket=bucket, Key="large.bin")
    got = resp["Body"].read()
    assert len(got) == len(data), f"size mismatch: {len(got)} != {len(data)}"
    assert hashlib.md5(got).hexdigest() == md5, "MD5 mismatch"
    print("  ✓ get_object (1MB, MD5 verified)")

    s3.delete_object(Bucket=bucket, Key="large.bin")
    s3.delete_bucket(Bucket=bucket)

def test_multipart_upload(s3):
    bucket = "boto3-test-multipart"
    s3.create_bucket(Bucket=bucket)

    key = "multipart.bin"
    part1 = b"A" * 1024
    part2 = b"B" * 512

    # Create multipart upload
    mpu = s3.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = mpu["UploadId"]
    print(f"  ✓ create_multipart_upload (UploadId={upload_id[:8]}...)")

    # Upload parts
    p1 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1)
    p2 = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2)
    print("  ✓ upload_part x2")

    # Complete
    s3.complete_multipart_upload(
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
    print("  ✓ complete_multipart_upload")

    # Verify
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    assert data == part1 + part2, "multipart content mismatch"
    print("  ✓ get_object (multipart content verified)")

    s3.delete_object(Bucket=bucket, Key=key)
    s3.delete_bucket(Bucket=bucket)

def test_presigned_url(s3):
    bucket = "boto3-test-presigned"
    s3.create_bucket(Bucket=bucket)

    s3.put_object(Bucket=bucket, Key="signed.txt", Body=b"presigned content")

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": "signed.txt"},
        ExpiresIn=60,
    )
    print(f"  ✓ generate_presigned_url ({url[:60]}...)")

    # Fetch via presigned URL
    import urllib.request
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    assert data == b"presigned content"
    print("  ✓ presigned GET (content verified)")

    s3.delete_object(Bucket=bucket, Key="signed.txt")
    s3.delete_bucket(Bucket=bucket)

def main():
    s3 = make_client()
    tests = [
        ("Bucket CRUD", test_bucket_crud),
        ("Object CRUD", test_object_crud),
        ("Large Object (1MB)", test_large_object),
        ("Multipart Upload", test_multipart_upload),
        ("Presigned URL", test_presigned_url),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        print(f"\n[TEST] {name}")
        try:
            fn(s3)
            passed += 1
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"boto3 E2E: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
