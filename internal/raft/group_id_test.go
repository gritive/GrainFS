package raft

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateGroupID(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		wantErr error
	}{
		{"empty rejected", "", nil},
		{"meta reserved", "__meta__", ErrReservedGroupID},
		{"reserved prefix __sys", "__sys", ErrReservedGroupID},
		{"reserved prefix __anything", "__anything", ErrReservedGroupID},
		{"normal numeric", "group-0", nil},
		{"normal hash-y", "g-0", nil},
		{"single underscore prefix ok", "_internal", nil},
		{"trailing __ ok", "group__", nil},
		{"unicode ok", "그룹-0", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateGroupID(tc.id)
			if tc.id == "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "empty")
				return
			}
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, tc.wantErr), "expected ErrReservedGroupID, got %v", err)
			assert.Contains(t, err.Error(), tc.id)
		})
	}
}
