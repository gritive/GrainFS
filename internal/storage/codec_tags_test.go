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

// TestMultipartMeta_TagsRoundtrip guards the marshalMultipartMeta /
// unmarshalMultipartMeta tags path directly (the existing coverage only goes
// through the backend integration test). Added alongside the encode-side
// buildTagsVector / decode-side readTagsVector dedup.
func TestMultipartMeta_TagsRoundtrip(t *testing.T) {
	original := &multipartMeta{
		UploadID:    "u",
		Bucket:      "bk",
		Key:         "k",
		ContentType: "ct",
		CreatedAt:   1700000000,
		Tags:        []Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	}
	buf, err := marshalMultipartMeta(original)
	require.NoError(t, err)
	got, err := unmarshalMultipartMeta(buf)
	require.NoError(t, err)
	require.Equal(t, original.Tags, got.Tags)
}
