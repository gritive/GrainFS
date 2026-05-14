package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiExportGenerationDriftReturnsFHExpired(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(map[string]exportConfig{
		"bucket": {generation: 2},
	}))

	require.Equal(t, NFS4ERR_FHEXPIRED, d.opPutFH(fh[:]).Status)
}

func TestMultiExportRemovedExportReturnsAdminRevoked(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(nil))

	require.Equal(t, NFS4ERR_ADMIN_REVOKED, d.opPutFH(fh[:]).Status)
}
