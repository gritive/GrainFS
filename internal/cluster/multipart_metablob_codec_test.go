package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCompleteMultipartCmd_MetaBlob_RoundTrip + multipartDone meta_blob (S3a schema).
func TestMultipartMetaBlob_RoundTrip(t *testing.T) {
	mb := []byte{0x01, 0x02, 0x03, 0xff}

	cmdBlob, err := encodeCompleteMultipartCmd(CompleteMultipartCmd{Bucket: "b", Key: "k", UploadID: "u", VersionID: "v1", MetaBlob: mb})
	require.NoError(t, err)
	cmd, err := decodeCompleteMultipartCmd(cmdBlob)
	require.NoError(t, err)
	require.Equal(t, mb, cmd.MetaBlob, "CompleteMultipartCmd.MetaBlob must round-trip")

	// absent → nil
	emptyBlob, err := encodeCompleteMultipartCmd(CompleteMultipartCmd{Bucket: "b", Key: "k", UploadID: "u"})
	require.NoError(t, err)
	empty, err := decodeCompleteMultipartCmd(emptyBlob)
	require.NoError(t, err)
	require.Nil(t, empty.MetaBlob, "absent meta_blob decodes nil")

	dBlob, err := marshalMultipartDone(multipartDone{UploadID: "u", Bucket: "b", Key: "k", VersionID: "v1", MetaBlob: mb})
	require.NoError(t, err)
	d, err := unmarshalMultipartDone(dBlob)
	require.NoError(t, err)
	require.Equal(t, mb, d.MetaBlob, "multipartDone.MetaBlob must round-trip")
}
