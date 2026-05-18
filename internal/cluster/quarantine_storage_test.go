package cluster

import (
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

func TestDecodePutObjectQuarantineCmdStorage_Corrupt(t *testing.T) {
	// Bytes that look FB-ish but are truncated/garbled. defer-recover must
	// turn the inevitable panic into a typed error.
	garbage := []byte{0x10, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, err := decodePutObjectQuarantineCmdStorage(garbage)
	require.Error(t, err)
}
