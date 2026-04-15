import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import boto3
import pytest
from botocore.config import Config


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Server did not start on port {port} within {timeout}s")


@pytest.fixture(scope="session")
def grainfs_server():
    """Start a GrainFS server for the entire test session."""
    binary = os.environ.get("GRAINFS_BINARY", "./bin/grainfs")
    if not os.path.isfile(binary):
        subprocess.run(["make", "build"], check=True)

    port = _free_port()
    data_dir = tempfile.mkdtemp(prefix="grainfs-e2e-")

    proc = subprocess.Popen(
        [binary, "serve", "--data", data_dir, "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_port(port)
        yield {"port": port, "url": f"http://127.0.0.1:{port}", "data_dir": data_dir}
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture
def s3(grainfs_server):
    """Provide a boto3 S3 client connected to the test server."""
    return boto3.client(
        "s3",
        endpoint_url=grainfs_server["url"],
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


@pytest.fixture
def bucket(s3):
    """Create a fresh test bucket and clean it up after."""
    name = "test-bucket"
    s3.create_bucket(Bucket=name)
    yield name
    # cleanup: delete all objects, then the bucket
    try:
        resp = s3.list_objects_v2(Bucket=name)
        for obj in resp.get("Contents", []):
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)
    except Exception:
        pass
