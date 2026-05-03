package nfs4server

import (
	"errors"
	"testing"
)

func TestValidateComponentName(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		// RFC 7530 §6 illegal:
		{"", true},
		{".", true},
		{"..", true},
		// Path traversal via component name (caught because '/' is rejected):
		{"../foo", true},
		{"foo/bar", true},
		{"a/b/c", true},
		{"./foo", true},
		{"/", true},
		{"/etc/passwd", true},
		// Legal:
		{"foo", false},
		{"foo.txt", false},
		{"...trailing", false},
		{".hidden", false},
		{"..hidden", false},
		{".bashrc", false},
		{"a", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateComponentName(tc.name)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("name=%q: want error, got nil", tc.name)
				}
				if !errors.Is(err, errInvalidComponent) {
					t.Fatalf("name=%q: want errInvalidComponent, got %v", tc.name, err)
				}
			} else {
				if err != nil {
					t.Fatalf("name=%q: want nil, got %v", tc.name, err)
				}
			}
		})
	}
}
