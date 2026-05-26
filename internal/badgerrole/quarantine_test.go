package badgerrole

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuarantineRoleRenamesDirectoryAndWritesManifest(t *testing.T) {
	root := t.TempDir()
	active := filepath.Join(root, "receipts")
	require.NoError(t, os.MkdirAll(active, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(active, "MANIFEST"), []byte("bad"), 0o644))

	result, err := QuarantineRole(QuarantineRequest{
		Role:   RoleReceipts,
		Path:   active,
		Reason: "operator approved quarantine",
	})
	require.NoError(t, err)

	assert.DirExists(t, result.QuarantinePath)
	assert.NoDirExists(t, active)
	assert.FileExists(t, filepath.Join(result.QuarantinePath, "recovery-manifest.json"))
}

func TestQuarantineRoleRejectsEmptyPath(t *testing.T) {
	_, err := QuarantineRole(QuarantineRequest{Role: RoleReceipts})
	require.ErrorContains(t, err, "path required")
}
