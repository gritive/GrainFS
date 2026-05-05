package badgerrole

import (
	"os"
	"path/filepath"
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

func TestOpenRoleFailureActionMatchesRoleCriticality(t *testing.T) {
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))

	tests := []struct {
		name       string
		role       Role
		wantAction RecoveryAction
	}{
		{name: "optional", role: RoleReceipts, wantAction: RecoveryActionDisableFeature},
		{name: "read only", role: RoleGroupState, wantAction: RecoveryActionStartReadOnly},
		{name: "required", role: RoleMeta, wantAction: RecoveryActionBlockStart},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, decision, err := OpenRole(DefaultRegistry(), tt.role, PathContext{DataDir: dataFile, GroupID: "group-a"})
			require.Error(t, err)
			require.Nil(t, db)
			require.Equal(t, DecisionOpenFailed, decision.Status)
			require.Equal(t, tt.wantAction, decision.Action)
		})
	}
}
