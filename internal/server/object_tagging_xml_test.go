package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func TestTaggingXML_RoundTrip(t *testing.T) {
	tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
	out, err := MarshalTaggingXML(tags)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(out), `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`))

	got, err := ParseTaggingXML(out)
	require.NoError(t, err)
	require.Equal(t, tags, got)
}

func TestTaggingXML_EmptySet(t *testing.T) {
	got, err := ParseTaggingXML([]byte(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet/></Tagging>`))
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTaggingXML_WrongRootRejected(t *testing.T) {
	_, err := ParseTaggingXML([]byte(`<NotTagging/>`))
	require.Error(t, err)
}

func TestParseTaggingHeader(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []storage.Tag
	}{
		{"", nil},
		{"k1=v1", []storage.Tag{{Key: "k1", Value: "v1"}}},
		{"k1=v1&k2=v2", []storage.Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}},
		{"k1=v1&", []storage.Tag{{Key: "k1", Value: "v1"}}},
		{"k%201=v%20a", []storage.Tag{{Key: "k 1", Value: "v a"}}},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseTaggingHeader(tc.in)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestParseTaggingHeader_DuplicateRejected(t *testing.T) {
	_, err := ParseTaggingHeader("k=1&k=2")
	require.Error(t, err)
}
