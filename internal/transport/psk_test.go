package transport

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateClusterKey(t *testing.T) {
	cases := []struct {
		name    string
		key     string
		wantErr error
	}{
		{"empty", "", ErrEmptyClusterKey},
		{"single char", "a", ErrShortClusterKey},
		{"32 chars (too short)", strings.Repeat("a", 32), ErrShortClusterKey},
		{"63 chars (one short)", strings.Repeat("a", 63), ErrShortClusterKey},
		{"64 chars (minimum)", strings.Repeat("a", 64), nil},
		{"128 chars (long)", strings.Repeat("a", 128), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateClusterKey(tc.key)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("want errors.Is(err, %v), got %v", tc.wantErr, err)
			}
		})
	}
}
