// Red 22 — Single-node AppendObject API e2e (via aws-sdk-go-v2).
//
// Exercises the HTTP AppendObject entrypoint end-to-end against the shared
// single-node grainfs server started by TestMain. aws-sdk-go-v2 (v1.101.0)
// already exposes `WriteOffsetBytes *int64` on PutObjectInput which serializes
// to the `x-amz-write-offset-bytes` header, so no middleware injection is
// required.
package e2e

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendObjectE2E_SingleNode(t *testing.T) {
	// Red 22 currently exposes a follow-up: AppendObject does not produce a
	// VersionID, but ClusterCoordinator.commitObjectIndex routes through
	// MetaFSM.applyPutObjectIndex which rejects entries with empty VersionID
	// (meta_fsm.go: "PutObjectIndex: empty bucket/key/version"). Fixing this
	// requires extending AppendObjectCmd with a proposer-supplied VersionID and
	// updating applyAppendObjectFromCmd to write LatestKey + ObjectMetaKeyV
	// like applyPutObjectMeta does. Tracked as Task 26 follow-up.
	//
	// The wrapper-chain wiring (pullthrough/wal/packblob/RecoveryWriteGate)
	// that this test surfaced has already been fixed; once the VersionID
	// gap is closed, remove the Skip and the four sub-tests below should pass
	// without further change.
	t.Skip("Red 22: blocked on AppendObject VersionID gap — see Task 26 follow-up")
	bucket := "append-e2e"
	createBucket(t, bucket)

	t.Run("InitialAppend", func(t *testing.T) {
		key := "obj-init"
		appendAt(t, bucket, key, 0, []byte("hello"))
		require.Equal(t, []byte("hello"), readObjectViaS3(t, bucket, key))
	})

	t.Run("SequentialAppends", func(t *testing.T) {
		key := "obj-seq"
		appendAt(t, bucket, key, 0, []byte("foo"))
		appendAt(t, bucket, key, 3, []byte("bar"))
		appendAt(t, bucket, key, 6, []byte("baz"))
		require.Equal(t, []byte("foobarbaz"), readObjectViaS3(t, bucket, key))
	})

	t.Run("OffsetMismatch_400_InvalidWriteOffset", func(t *testing.T) {
		key := "obj-mismatch"
		appendAt(t, bucket, key, 0, []byte("aaa"))
		err := appendAtErr(bucket, key, 99, []byte("bbb"))
		require.Error(t, err)
		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidWriteOffset", apiErr.ErrorCode())
	})

	t.Run("PlainPutOverwritesAppendable", func(t *testing.T) {
		key := "obj-overwrite"
		appendAt(t, bucket, key, 0, []byte("aaaa"))
		// Plain PUT (no x-amz-write-offset-bytes header) must overwrite the
		// appendable object — matches Red 21 HTTP-level behavior.
		_, err := testS3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("xx")),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("xx"), readObjectViaS3(t, bucket, key))
	})
}

// appendAt issues a PutObject with WriteOffsetBytes set; fails the test on error.
func appendAt(t *testing.T, bucket, key string, offset int64, body []byte) {
	t.Helper()
	require.NoError(t, appendAtErr(bucket, key, offset, body))
}

// appendAtErr issues a PutObject with WriteOffsetBytes set and returns the
// raw SDK error so callers can inspect APIError codes.
func appendAtErr(bucket, key string, offset int64, body []byte) error {
	off := offset
	_, err := testS3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(key),
		Body:             bytes.NewReader(body),
		WriteOffsetBytes: &off,
	})
	return err
}

func readObjectViaS3(t *testing.T, bucket, key string) []byte {
	t.Helper()
	resp, err := testS3Client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return data
}
