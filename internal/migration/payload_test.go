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
