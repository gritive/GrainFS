package uuidutil

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMustNewV7(t *testing.T) {
	t.Run("returns a parseable UUIDv7 string", func(t *testing.T) {
		s := MustNewV7()

		parsed, err := uuid.Parse(s)
		require.NoError(t, err)
		assert.Equal(t, uuid.Version(7), parsed.Version())
		assert.Len(t, s, 36)
	})

	t.Run("consecutive calls return distinct values", func(t *testing.T) {
		seen := make(map[string]struct{}, 1000)
		for i := 0; i < 1000; i++ {
			s := MustNewV7()
			_, dup := seen[s]
			require.False(t, dup, "duplicate UUID generated: %s", s)
			seen[s] = struct{}{}
		}
	})
}
