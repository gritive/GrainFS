package tagging

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

func tag(k, v string) storage.Tag { return storage.Tag{Key: k, Value: v} }

func TestValidate_CountAndDup(t *testing.T) {
	cases := []struct {
		name    string
		tags    []storage.Tag
		wantErr string
	}{
		{"empty ok", nil, ""},
		{"one ok", []storage.Tag{tag("k", "v")}, ""},
		{"ten ok", makeN(10), ""},
		{"eleven rejected", makeN(11), "tag count"},
		{"duplicate rejected", []storage.Tag{tag("k", "1"), tag("k", "2")}, "duplicate"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.tags)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func makeN(n int) []storage.Tag {
	out := make([]storage.Tag, n)
	for i := 0; i < n; i++ {
		out[i] = tag(string(rune('a'+i)), "v")
	}
	return out
}
