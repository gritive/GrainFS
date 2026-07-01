package lifecycle

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutPayload_RoundTrip(t *testing.T) {
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r1</ID></Rule></LifecycleConfiguration>`)
	enc := EncodePutPayload("my-bucket", raw)
	bucket, xml, err := DecodePutPayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", bucket)
	assert.Equal(t, raw, xml)
}

func TestDeletePayload_RoundTrip(t *testing.T) {
	enc := EncodeDeletePayload("b", UnconditionalDeleteGen)
	bucket, _, err := DecodeDeletePayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
}

func TestDeletePayload_RoundTripWithGen(t *testing.T) {
	b, g, err := DecodeDeletePayload(EncodeDeletePayload("mybkt", 7))
	require.NoError(t, err)
	require.Equal(t, "mybkt", b)
	require.Equal(t, uint64(7), g)
}

func TestDeletePayload_LegacyNoGen_IsUnconditional(t *testing.T) {
	// Legacy encoding: uint16 len | bucket, no 8-byte suffix.
	bb := []byte("mybkt")
	legacy := make([]byte, 2+len(bb))
	binary.BigEndian.PutUint16(legacy[:2], uint16(len(bb)))
	copy(legacy[2:], bb)
	b, g, err := DecodeDeletePayload(legacy)
	require.NoError(t, err)
	require.Equal(t, "mybkt", b)
	require.Equal(t, UnconditionalDeleteGen, g)
}

func TestDecodePutPayload_Truncated(t *testing.T) {
	_, _, err := DecodePutPayload([]byte{0x00})
	assert.Error(t, err)
}

func TestDecodePutPayload_BucketLenExceedsBuffer(t *testing.T) {
	bad := []byte{0x03, 0xE7, 'a', 'b', 'c'}
	_, _, err := DecodePutPayload(bad)
	assert.Error(t, err)
}
