package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultipartDone_RoundTrip(t *testing.T) {
	m := multipartDone{UploadID: "u1", Bucket: "b", Key: "k", VersionID: "v7", ModTime: 123}
	raw, err := marshalMultipartDone(m)
	require.NoError(t, err)
	got, err := unmarshalMultipartDone(raw)
	require.NoError(t, err)
	require.Equal(t, m, got)
}
