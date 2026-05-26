package badgerrole

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryContainsEveryStartupRole(t *testing.T) {
	reg := DefaultRegistry()

	want := []Role{
		RoleMeta,
		RoleMetaRaftLog,
		RoleSharedRaftLog,
		RoleSharedFSM,
		RoleGroupState,
		RoleGroupRaftLog,
		RoleReceipts,
		RoleVolumeCatalog,
		RoleIncidentState,
	}

	require.Len(t, reg.All(), len(want))
	for _, role := range want {
		spec, ok := reg.Get(role)
		require.Truef(t, ok, "role %s missing", role)
		assert.Equal(t, role, spec.Role)
		assert.NotEmpty(t, spec.DisplayName)
		assert.NotEmpty(t, spec.RelativePath)
		assert.NotEmpty(t, spec.OptionsKind)
		assert.NotEmpty(t, spec.Criticality)
		assert.NotEmpty(t, spec.SourceContract)
	}
}

func TestRegistryResolvesPaths(t *testing.T) {
	reg := DefaultRegistry()

	tests := []struct {
		name string
		role Role
		ctx  PathContext
		want string
	}{
		{
			name: "meta",
			role: RoleMeta,
			ctx:  PathContext{DataDir: "/data/grainfs"},
			want: filepath.Join("/data/grainfs", "meta"),
		},
		{
			name: "group state",
			role: RoleGroupState,
			ctx:  PathContext{DataDir: "/data/grainfs", GroupID: "group-a"},
			want: filepath.Join("/data/grainfs", "groups", "group-a", "badger"),
		},
		{
			name: "group raft",
			role: RoleGroupRaftLog,
			ctx:  PathContext{DataDir: "/data/grainfs", GroupID: "group-a"},
			want: filepath.Join("/data/grainfs", "groups", "group-a", "raft"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reg.ResolvePath(tt.role, tt.ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRegistryRejectsGroupRoleWithoutGroupID(t *testing.T) {
	reg := DefaultRegistry()

	_, err := reg.ResolvePath(RoleGroupState, PathContext{DataDir: "/data/grainfs"})
	require.ErrorContains(t, err, "group id required")
}
