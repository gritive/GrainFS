package badgerrole

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenRoleCreatesSmallOptionsDB(t *testing.T) {
	dir := t.TempDir()
	db, decision, err := OpenRole(DefaultRegistry(), RoleReceipts, PathContext{DataDir: dir})
	require.NoError(t, err)
	require.Equal(t, DecisionOK, decision.Status)
	require.NotNil(t, db)
	require.NoError(t, db.Close())
}

func TestOpenRoleRejectsUnknownRole(t *testing.T) {
	db, decision, err := OpenRole(DefaultRegistry(), Role("unknown"), PathContext{DataDir: t.TempDir()})
	require.Error(t, err)
	require.Nil(t, db)
	require.Equal(t, DecisionUnknownRole, decision.Status)
}
