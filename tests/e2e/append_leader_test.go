package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindOwnerForSingleGroup(t *testing.T) {
	require.Equal(t, -1, findOwnerForSingleGroup(nil))
	require.Equal(t, 2, findOwnerForSingleGroup(&e2eCluster{leaderIdx: 2}))
}
