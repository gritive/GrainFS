package nfsexport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeUpsertPayload(t *testing.T) {
	cfg := Config{ReadOnly: true, FsidMajor: 1, FsidMinor: 2, Generation: 3}
	buf, err := EncodeUpsertPayload("bucket-a", cfg)
	require.NoError(t, err)

	bucket, got, err := DecodeUpsertPayload(buf)
	require.NoError(t, err)
	require.Equal(t, "bucket-a", bucket)
	require.Equal(t, cfg, got)
}

func TestEncodeDecodeDeletePayload(t *testing.T) {
	buf, err := EncodeDeletePayload("bucket-a")
	require.NoError(t, err)

	bucket, err := DecodeDeletePayload(buf)
	require.NoError(t, err)
	require.Equal(t, "bucket-a", bucket)
}

func TestPayloadRejectsEmptyBucket(t *testing.T) {
	_, err := EncodeUpsertPayload("", Config{})
	require.Error(t, err)
	_, err = EncodeDeletePayload("")
	require.Error(t, err)
}

func TestPayloadRejectsMalformedInput(t *testing.T) {
	_, _, err := DecodeUpsertPayload([]byte{1, 2, 3})
	require.Error(t, err)
	_, err = DecodeDeletePayload([]byte{1, 2, 3})
	require.Error(t, err)
}
