package lifecycle

import (
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
	enc := EncodeDeletePayload("b")
	bucket, err := DecodeDeletePayload(enc)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
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
