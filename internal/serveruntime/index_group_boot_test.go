package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexGroupCount_DefaultAndNormalize(t *testing.T) {
	require.Equal(t, 1, normalizeIndexGroupCount(0))
	require.Equal(t, 1, normalizeIndexGroupCount(-3))
	require.Equal(t, 16, normalizeIndexGroupCount(16))
}
