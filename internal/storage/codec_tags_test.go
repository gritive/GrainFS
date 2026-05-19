package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObject_TagsRoundtrip(t *testing.T) {
	original := Object{
		Key:          "k",
		ETag:         "e",
		Size:         5,
		LastModified: 1700000000,
		Tags:         []Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	}
	buf, err := marshalObject(&original)
	require.NoError(t, err)
	var got Object
	require.NoError(t, unmarshalObjectInto(buf, &got))
	require.Equal(t, original.Tags, got.Tags)
}
