package cluster

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripeBytesMetaRoundTrip(t *testing.T) {
	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 4 << 20, VersionID: "v1",
		ECData: 2, ECParity: 2, StripeBytes: 1 << 20,
		NodeIDs: []string{"n0", "n1", "n2", "n3"},
	}
	enc, err := encodePutObjectMetaCmd(cmd)
	require.NoError(t, err)
	dec, err := decodePutObjectMetaCmd(enc)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<20), dec.StripeBytes)

	m := buildPutObjectMeta(cmd)
	require.Equal(t, uint32(1<<20), m.StripeBytes)
	raw, err := marshalObjectMeta(m)
	require.NoError(t, err)
	got, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<20), got.StripeBytes)

	zero := buildPutObjectMeta(PutObjectMetaCmd{Bucket: "b", Key: "k2", ECData: 4, ECParity: 2})
	rawz, err := marshalObjectMeta(zero)
	require.NoError(t, err)
	gz, err := unmarshalObjectMeta(rawz)
	require.NoError(t, err)
	require.Equal(t, uint32(0), gz.StripeBytes)
}

const goldenPreStripeBytesMeta = "JAAAACAANAAwACQAIAAcAAAAAAAbABoAFAAMABAAAAAIAAQAIAAAADAAAADYAAAAuAEAAHwBAAB8AQAAAAACArABAAC0AQAAAABAAAAAAAAAAAAArAEAAAEAAAAYAAAAFAAkACAAFAAQAAwAAAALAAoABAAUAAAAIAAAAAAAAgJMAAAAZAAAAAAAEAAAAAAAAAAAAGAAAAAEAAAAKAAAABwAAAAQAAAABAAAAAIAAABuMwAAAgAAAG4yAAACAAAAbjEAAAIAAABuMAAAFwAAAGsvY29hbGVzY2VkL2NvYWxlc2NlZC0wAAcAAABldGFnLWMwAAsAAABjb2FsZXNjZWQtMAABAAAAHAAAAAAAFgAoACQAHAAYABQAEAAMAAgABwAGABYAAAAAAAICIAAAAAAABABkAAAASAAAAEwAAAAAABAAAAAAAFwAAAAEAAAAKAAAABwAAAAQAAAABAAAAAIAAABuMwAAAgAAAG4yAAACAAAAbjEAAAIAAABuMAAABAAAAAECAwQIAAAAMDEwMjAzMDQAAAAABwAAAGdyb3VwLTAABgAAAGJsb2ItMAAAAAAAAAQAAAAoAAAAHAAAABAAAAAEAAAAAgAAAG4zAAACAAAAbjIAAAIAAABuMQAAAgAAAG4wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAawAAAA=="

func TestObjectMetaBackwardCompat_PreStripeBytes(t *testing.T) {
	raw, err := base64.StdEncoding.DecodeString(goldenPreStripeBytesMeta)
	require.NoError(t, err)
	m, err := unmarshalObjectMeta(raw)
	require.NoError(t, err)
	require.Equal(t, uint8(2), m.ECData)
	require.Equal(t, uint8(2), m.ECParity)
	require.GreaterOrEqual(t, len(m.Segments), 1)
	require.GreaterOrEqual(t, len(m.Coalesced), 1)
	require.Equal(t, uint32(0), m.StripeBytes) // absent in old blob ⟹ default 0
}
