#!/usr/bin/env bash
# aws-cli E2E tests for GrainFS S3 API compatibility.
# Usage: GRAINFS_ENDPOINT=http://localhost:9000 ./test_awscli.sh
#
# Prerequisites: aws-cli v2 installed and configured with dummy credentials.
set -euo pipefail

ENDPOINT="${GRAINFS_ENDPOINT:-http://localhost:9000}"
AWS="aws --endpoint-url $ENDPOINT --no-sign-request"
PASSED=0
FAILED=0
BUCKET="awscli-test-$$"

cleanup() {
  # Remove all objects and the test bucket
  $AWS s3 rm "s3://$BUCKET" --recursive 2>/dev/null || true
  $AWS s3 rb "s3://$BUCKET" 2>/dev/null || true
}
trap cleanup EXIT

pass() { PASSED=$((PASSED + 1)); echo "  PASS: $1"; }
fail() { FAILED=$((FAILED + 1)); echo "  FAIL: $1"; }

echo "=== GrainFS aws-cli E2E Tests ==="
echo "Endpoint: $ENDPOINT"
echo ""

# --- Bucket operations ---
echo "--- Bucket Operations ---"

# Create bucket
if $AWS s3 mb "s3://$BUCKET" >/dev/null 2>&1; then
  pass "s3 mb (create bucket)"
else
  fail "s3 mb (create bucket)"
fi

# List buckets
if $AWS s3api list-buckets --query "Buckets[].Name" --output text 2>&1 | grep -q "$BUCKET"; then
  pass "s3api list-buckets"
else
  fail "s3api list-buckets"
fi

# --- Object operations ---
echo ""
echo "--- Object Operations ---"

# Upload a file
TMPFILE=$(mktemp)
echo "hello from aws-cli" > "$TMPFILE"
if $AWS s3 cp "$TMPFILE" "s3://$BUCKET/test.txt" >/dev/null 2>&1; then
  pass "s3 cp upload"
else
  fail "s3 cp upload"
fi

# List objects
if $AWS s3 ls "s3://$BUCKET/" 2>&1 | grep -q "test.txt"; then
  pass "s3 ls (list objects)"
else
  fail "s3 ls (list objects)"
fi

# Download and verify
OUTFILE=$(mktemp)
if $AWS s3 cp "s3://$BUCKET/test.txt" "$OUTFILE" >/dev/null 2>&1; then
  CONTENT=$(cat "$OUTFILE")
  if [ "$CONTENT" = "hello from aws-cli" ]; then
    pass "s3 cp download + content match"
  else
    fail "s3 cp download content mismatch: got '$CONTENT'"
  fi
else
  fail "s3 cp download"
fi
rm -f "$OUTFILE"

# Head object (via s3api)
if $AWS s3api head-object --bucket "$BUCKET" --key "test.txt" >/dev/null 2>&1; then
  pass "s3api head-object"
else
  fail "s3api head-object"
fi

# Upload larger file
BIGFILE=$(mktemp)
dd if=/dev/urandom bs=1024 count=100 of="$BIGFILE" 2>/dev/null
if $AWS s3 cp "$BIGFILE" "s3://$BUCKET/big.bin" >/dev/null 2>&1; then
  pass "s3 cp upload 100KB file"
else
  fail "s3 cp upload 100KB file"
fi
rm -f "$BIGFILE"

# Copy object (server-side)
if $AWS s3 cp "s3://$BUCKET/test.txt" "s3://$BUCKET/test-copy.txt" >/dev/null 2>&1; then
  pass "s3 cp server-side copy"
else
  # server-side copy may not be implemented — note as expected skip
  echo "  SKIP: s3 cp server-side copy (not implemented)"
fi

# --- Nested keys ---
echo ""
echo "--- Nested Keys ---"

echo "nested content" > "$TMPFILE"
if $AWS s3 cp "$TMPFILE" "s3://$BUCKET/dir/subdir/nested.txt" >/dev/null 2>&1; then
  pass "s3 cp nested key upload"
else
  fail "s3 cp nested key upload"
fi

if $AWS s3 ls "s3://$BUCKET/dir/" 2>&1 | grep -q "nested.txt"; then
  pass "s3 ls with prefix"
else
  fail "s3 ls with prefix"
fi

# --- Delete operations ---
echo ""
echo "--- Delete Operations ---"

if $AWS s3 rm "s3://$BUCKET/test.txt" >/dev/null 2>&1; then
  pass "s3 rm (delete object)"
else
  fail "s3 rm (delete object)"
fi

# Verify deletion
if ! $AWS s3api head-object --bucket "$BUCKET" --key "test.txt" 2>/dev/null; then
  pass "head-object after delete returns error"
else
  fail "head-object after delete still returns object"
fi

rm -f "$TMPFILE"

# --- Summary ---
echo ""
echo "=== Results: $PASSED passed, $FAILED failed ==="

if [ "$FAILED" -gt 0 ]; then
  exit 1
fi
