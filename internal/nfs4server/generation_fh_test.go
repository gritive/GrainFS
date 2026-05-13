package nfs4server

import "testing"

func TestPutFHGenerationDriftReturnsFHExpired(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(map[string]exportConfig{
		"bucket": {generation: 2},
	}))

	if got := d.opPutFH(fh[:]).Status; got != NFS4ERR_FHEXPIRED {
		t.Fatalf("status = %d, want NFS4ERR_FHEXPIRED", got)
	}
}

func TestPutFHRemovedExportReturnsAdminRevoked(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {generation: 1},
	})
	d.currentPath = "/bucket/file.bin"
	fh := d.state.GetOrCreateFH(d.currentPath)
	d.state.BindFHGeneration(fh, "bucket", 1)

	d.server.SetExportsForTest(buildSnap(nil))

	if got := d.opPutFH(fh[:]).Status; got != NFS4ERR_ADMIN_REVOKED {
		t.Fatalf("status = %d, want NFS4ERR_ADMIN_REVOKED", got)
	}
}
