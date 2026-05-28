package iampb

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

func TestKeyCreatePayload_DEKGenRoundTrip(t *testing.T) {
	b := flatbuffers.NewBuilder(64)
	ak := b.CreateString("AKIA-test")
	enc := b.CreateByteVector([]byte{1, 2, 3})
	sa := b.CreateString("sa-x")
	KeyCreatePayloadStart(b)
	KeyCreatePayloadAddAccessKey(b, ak)
	KeyCreatePayloadAddSecretKeyEnc(b, enc)
	KeyCreatePayloadAddSaId(b, sa)
	KeyCreatePayloadAddSecretKeyDekGen(b, 7)
	b.Finish(KeyCreatePayloadEnd(b))
	out := GetRootAsKeyCreatePayload(b.FinishedBytes(), 0)
	require.Equal(t, uint32(7), out.SecretKeyDekGen())
	require.Equal(t, "sa-x", string(out.SaId()))
	require.Equal(t, "AKIA-test", string(out.AccessKey()))
}

func TestBucketUpstreamPutPayload_DEKGenRoundTrip(t *testing.T) {
	b := flatbuffers.NewBuilder(64)
	bucket := b.CreateString("b1")
	enc := b.CreateByteVector([]byte{9, 8})
	BucketUpstreamPutPayloadStart(b)
	BucketUpstreamPutPayloadAddBucket(b, bucket)
	BucketUpstreamPutPayloadAddSecretKeyEnc(b, enc)
	BucketUpstreamPutPayloadAddSecretKeyDekGen(b, 3)
	b.Finish(BucketUpstreamPutPayloadEnd(b))
	out := GetRootAsBucketUpstreamPutPayload(b.FinishedBytes(), 0)
	require.Equal(t, uint32(3), out.SecretKeyDekGen())
	require.Equal(t, "b1", string(out.Bucket()))
}

func TestKeyCreatePayload_DEKGenDefaultsToZero(t *testing.T) {
	b := flatbuffers.NewBuilder(64)
	sa := b.CreateString("sa-x")
	KeyCreatePayloadStart(b)
	KeyCreatePayloadAddSaId(b, sa)
	// Note: SecretKeyDekGen intentionally NOT set — should default to 0.
	b.Finish(KeyCreatePayloadEnd(b))
	out := GetRootAsKeyCreatePayload(b.FinishedBytes(), 0)
	require.Equal(t, uint32(0), out.SecretKeyDekGen(), "FB default is 0")
}
