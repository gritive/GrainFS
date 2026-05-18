package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodePutObjectQuarantineCmdStorage_RoundTrip(t *testing.T) {
	cmd := PutObjectQuarantineCmd{
		Bucket:    "b1",
		Key:       "k1",
		VersionID: "v-uuid",
		Cause:     "scrub-mismatch",
		Reason:    "ec parity fail",
	}
	data, err := encodePutObjectQuarantineCmd(cmd)
	require.NoError(t, err)

	got, err := decodePutObjectQuarantineCmdStorage(data)
	require.NoError(t, err)
	require.Equal(t, cmd, got)
}

func TestDecodePutObjectQuarantineCmdStorage_LegacyJSON(t *testing.T) {
	legacy := []byte(`{"bucket":"b","key":"k","version_id":"","cause":"x","reason":"y"}`)
	_, err := decodePutObjectQuarantineCmdStorage(legacy)
	require.True(t, errors.Is(err, ErrLegacyStorageFormat), "got %v", err)
}

func TestDecodePutObjectQuarantineCmdStorage_Corrupt(t *testing.T) {
	// Bytes that look FB-ish but are truncated/garbled.
	garbage := []byte{0x10, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, err := decodePutObjectQuarantineCmdStorage(garbage)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrLegacyStorageFormat))
}
