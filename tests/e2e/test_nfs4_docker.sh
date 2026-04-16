#!/usr/bin/env bash
# NFSv4 E2E tests using Docker (Linux environment).
# Tests the NFSv4 server by running a Go test client inside Docker.
# Usage: ./test_nfs4_docker.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PASSED=0
FAILED=0

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

echo "=== GrainFS NFSv4 Docker E2E Tests ==="
echo ""

# --- Test 1: Build and run Go E2E tests inside Docker ---
echo "--- NFSv4 Go E2E Tests (via Docker) ---"

# Build a Docker image with Go and run the tests
if docker build -t grainfs-nfs4-test -f - "$PROJECT_ROOT" << 'DOCKERFILE' 2>/dev/null
FROM golang:1.26-alpine
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./internal/nfs4server/ -run TestE2E -count=1 -timeout 30s -v
DOCKERFILE
then
  pass "NFSv4 E2E tests pass in Linux Docker container"
else
  fail "NFSv4 E2E tests failed in Docker"
fi

# --- Test 2: Packed Blob + S3 CopyObject via Docker ---
echo ""
echo "--- Packed Blob + S3 CopyObject Docker Tests ---"

if docker build -t grainfs-packblob-test -f - "$PROJECT_ROOT" << 'DOCKERFILE' 2>/dev/null
FROM golang:1.26-alpine
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./internal/storage/packblob/ -count=1 -timeout 30s -v
DOCKERFILE
then
  pass "Packed Blob tests pass in Linux Docker container"
else
  fail "Packed Blob tests failed in Docker"
fi

# --- Test 3: Full test suite in Docker ---
echo ""
echo "--- Full Internal Test Suite (Docker) ---"

if docker build -t grainfs-full-test -f - "$PROJECT_ROOT" << 'DOCKERFILE' 2>/dev/null
FROM golang:1.26-alpine
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./internal/... -count=1 -timeout 180s
DOCKERFILE
then
  pass "Full internal test suite passes in Linux Docker container"
else
  fail "Full internal test suite failed in Docker"
fi

# --- Summary ---
echo ""
echo "=== Results: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
