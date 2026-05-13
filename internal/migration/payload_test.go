package migration

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeJobStartPayload(t *testing.T) {
	ts := int64(1700000000)
	enc := EncodeJobStartPayload("my-bucket", ts)
	bucket, startedAt, err := DecodeJobStartPayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", bucket)
	assert.Equal(t, ts, startedAt)
}

func TestEncodeDecodeJobDonePayload(t *testing.T) {
	ts := int64(1700001000)
	enc := EncodeJobDonePayload("b", 42, 3, ts)
	bucket, copied, errors, updatedAt, err := DecodeJobDonePayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, int64(42), copied)
	assert.Equal(t, int64(3), errors)
	assert.Equal(t, ts, updatedAt)
}

func TestEncodeDecodeJobFailedPayload(t *testing.T) {
	ts := int64(1700002000)
	enc := EncodeJobFailedPayload("b", "source unavailable", 2, ts)
	bucket, reason, errors, updatedAt, err := DecodeJobFailedPayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, "source unavailable", reason)
	assert.Equal(t, int64(2), errors)
	assert.Equal(t, ts, updatedAt)
}

func TestDecodeJobStartPayload_Truncated(t *testing.T) {
	_, _, err := DecodeJobStartPayload([]byte{0x00})
	require.Error(t, err)
}

func TestDecodePayload_BoundsChecks(t *testing.T) {
	t.Run("DecodeJobStart_BucketTooLong", func(t *testing.T) {
		buf := make([]byte, 4+8)
		// Write n > maxBucketLen
		buf[0], buf[1], buf[2], buf[3] = 0x00, 0x00, 0x01, 0x00 // 256 > 255
		_, _, err := DecodeJobStartPayload(buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bucket name too long")
	})
	t.Run("DecodeJobStart_BufferTooShort", func(t *testing.T) {
		// n=5, but buf only has 4+4 bytes (not 4+5+8)
		buf := make([]byte, 4+4)
		buf[3] = 5
		_, _, err := DecodeJobStartPayload(buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "buffer too short")
	})

	t.Run("DecodeJobDone_Truncated", func(t *testing.T) {
		_, _, _, _, err := DecodeJobDonePayload([]byte{0x01})
		require.Error(t, err)
	})
	t.Run("DecodeJobDone_BucketTooLong", func(t *testing.T) {
		buf := make([]byte, 4+24)
		buf[3] = 0xff // 255 exactly — still valid; use 256
		buf[2] = 0x01 // 0x0100 = 256 > 255
		buf[3] = 0x00
		_, _, _, _, err := DecodeJobDonePayload(buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bucket name too long")
	})
	t.Run("DecodeJobDone_BufferTooShort", func(t *testing.T) {
		buf := make([]byte, 4+4)
		buf[3] = 3 // n=3, need 4+3+24 bytes, only have 8
		_, _, _, _, err := DecodeJobDonePayload(buf)
		require.Error(t, err)
	})

	t.Run("DecodeJobFailed_Truncated", func(t *testing.T) {
		_, _, _, _, err := DecodeJobFailedPayload([]byte{0x01})
		require.Error(t, err)
	})
	t.Run("DecodeJobFailed_BucketTooLong", func(t *testing.T) {
		buf := make([]byte, 4+4)
		buf[2] = 0x01 // 256 > 255
		_, _, _, _, err := DecodeJobFailedPayload(buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bucket name too long")
	})
	t.Run("DecodeJobFailed_ReasonTooLong", func(t *testing.T) {
		// craft: bucket="b" (n=1), then reason length = maxReasonLen+1 = 4097
		// layout: [4 bucket-len][1 'b'][4 reason-len][...]
		// reason-len at offset 5 (off = 4+1 = 5): BigEndian 0x00001001
		buf := make([]byte, 4+1+4+16)
		buf[3] = 1 // bucket len = 1
		buf[4] = 'b'
		// off=5: reason len bytes [5,6,7,8] = 0x00001001 = 4097
		buf[5] = 0x00
		buf[6] = 0x00
		buf[7] = 0x10
		buf[8] = 0x01
		_, _, _, _, err := DecodeJobFailedPayload(buf)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "reason too long")
	})

	t.Run("EncodeJobFailed_ReasonTruncated", func(t *testing.T) {
		longReason := string(make([]byte, maxReasonLen+100))
		enc := EncodeJobFailedPayload("b", longReason, 0, 0)
		_, reason, _, _, err := DecodeJobFailedPayload(enc)
		require.NoError(t, err)
		assert.Equal(t, maxReasonLen, len(reason))
	})
}
