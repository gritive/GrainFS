package tagging

import (
	"strings"
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

func TestValidate_KeyLen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"empty key rejected", "", true},
		{"len 1 ok", "a", false},
		{"len 128 ok", strings.Repeat("a", 128), false},
		{"len 129 rejected", strings.Repeat("a", 129), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate([]storage.Tag{tag(tc.key, "v")})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidate_ValueLen(t *testing.T) {
	for _, tc := range []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"empty value ok", "", false},
		{"len 256 ok", strings.Repeat("v", 256), false},
		{"len 257 rejected", strings.Repeat("v", 257), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate([]storage.Tag{tag("k", tc.val)})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidate_ReservedPrefix(t *testing.T) {
	for _, key := range []string{"aws:env", "AWS:env", "Aws:env"} {
		t.Run(key, func(t *testing.T) {
			err := Validate([]storage.Tag{tag(key, "v")})
			require.ErrorIs(t, err, ErrReservedTag)
		})
	}
}

func TestValidate_Charset(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		val     string
		wantErr bool
	}{
		{"alnum", "k1", "v1", false},
		{"allowed special", "team_name", "user.email+tag=v/path:@server-1", false},
		{"unicode letter", "팀", "값", false},
		{"control char", "a\x01", "v", true},
		{"emoji", "🚀", "v", true},
		{"angle bracket", "<k", "v", true},
		{"ampersand", "k&", "v", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate([]storage.Tag{tag(tc.key, tc.val)})
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
