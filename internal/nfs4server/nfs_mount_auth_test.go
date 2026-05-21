package nfs4server

import (
	"bytes"
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

// TestDecisionAllow_SyncedWithPolicy verifies that the package-local
// decisionAllow constant stays in sync with policy.DecisionAllow.
func TestDecisionAllow_SyncedWithPolicy(t *testing.T) {
	if decisionAllow != policy.DecisionAllow {
		t.Fatalf("decisionAllow out of sync: got %v want %v", decisionAllow, policy.DecisionAllow)
	}
}

// ---- test helpers ----

func newTestMountSAStore(t *testing.T) *mountsastore.Store {
	t.Helper()
	opts := badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s, err := mountsastore.NewStore(db)
	require.NoError(t, err)
	return s
}

// allowAllAuthz implements nfsAuthorizer: always returns DecisionAllow.
type allowAllAuthz struct{}

func (allowAllAuthz) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	return policy.EvalResult{Decision: policy.DecisionAllow, Reason: "test allow-all"}
}

// denyAllAuthz implements nfsAuthorizer: always returns DecisionDeny.
type denyAllAuthz struct{}

func (denyAllAuthz) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "test deny-all"}
}

func newAllowAuthorizer() nfsAuthorizer { return allowAllAuthz{} }
func newDenyAuthorizer() nfsAuthorizer  { return denyAllAuthz{} }

// newAuthDispatcher builds a Dispatcher wired to srv, positioned at root.
func newAuthDispatcher(srv *Server) *Dispatcher {
	state := NewStateManager()
	d := &Dispatcher{
		backend:     srv.backend,
		state:       state,
		server:      srv,
		currentFH:   state.RootFH(),
		currentPath: "/",
	}
	return d
}

// bucketExport returns a minimal exportSnap with one bucket registered.
func bucketExport(bucket string) *exportSnap {
	return buildSnap(map[string]exportConfig{
		bucket: {fsidMajor: 1, fsidMinor: 1, generation: 1},
	})
}

// ---- tests ----

// TestNFSLookup_NoIAMGate_NoPending checks that without the IAM gate, bucket
// LOOKUP does not mark the fh as "(pending)".
func TestNFSLookup_NoIAMGate_NoPending(t *testing.T) {
	srv := NewServer(nil)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status)

	b, ok := d.state.FHBinding(d.currentFH)
	require.True(t, ok)
	assert.NotEqual(t, fhSAIDPending, b.saID, "no IAM gate: saID must not be (pending)")
}

// TestNFSLookup_WithIAMGate_BucketFHPending checks that with IAM gate wired,
// a bucket LOOKUP issues a fh with saID="(pending)".
func TestNFSLookup_WithIAMGate_BucketFHPending(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status)

	b, ok := d.state.FHBinding(d.currentFH)
	require.True(t, ok)
	assert.Equal(t, fhSAIDPending, b.saID)
	assert.Equal(t, "default", b.bucket)
}

// TestNFSLookup_MountSAHit_Allowed checks that a registered mount-SA resolves
// to saID="<name>" when the IAM gate allows it.
func TestNFSLookup_MountSAHit_Allowed(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(
		mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status, "bucket LOOKUP")

	res = d.opLookup([]byte("alice-mount"))
	require.Equal(t, NFS4_OK, res.Status, "mount-SA LOOKUP must succeed")

	b, ok := d.state.FHBinding(d.currentFH)
	require.True(t, ok)
	assert.Equal(t, "alice-mount", b.saID)
	assert.Equal(t, "default", b.bucket)
}

// TestNFSLookup_MountSAHit_Denied checks that a denied mount-SA returns
// NFS4ERR_ACCESS.
func TestNFSLookup_MountSAHit_Denied(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(
		mountsastore.MountSA{Name: "bob-mount", NumericUID: 200002}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newDenyAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("mybucket"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("mybucket"))
	require.Equal(t, NFS4_OK, res.Status)

	res = d.opLookup([]byte("bob-mount"))
	assert.Equal(t, NFS4ERR_ACCESS, res.Status)
}

// TestNFSLookup_PoolMiss_NoFile_NOENT checks that a typo-ed mount-SA name
// with no backend file returns NFS4ERR_NOENT.
func TestNFSLookup_PoolMiss_NoFile_NOENT(t *testing.T) {
	msaStore := newTestMountSAStore(t) // empty pool

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status)

	res = d.opLookup([]byte("typo-sa-name"))
	assert.Equal(t, NFS4ERR_NOENT, res.Status, "pool miss + no backend file → NOENT")
}

// TestNFSLookup_AnonPath_Denied checks that when the authorizer denies anon
// and the component is a real backend file, NFS4ERR_ACCESS is returned.
func TestNFSLookup_AnonPath_Denied(t *testing.T) {
	dir := t.TempDir()
	bk, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, bk.CreateBucket(ctx, "mybucket"))
	_, err = bk.PutObject(ctx, "mybucket", "realfile.txt",
		bytes.NewReader([]byte("content")), "text/plain")
	require.NoError(t, err)

	msaStore := newTestMountSAStore(t) // empty pool

	srv := NewServer(bk,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newDenyAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("mybucket"))
	d := newAuthDispatcher(srv)

	res := d.opLookup([]byte("mybucket"))
	require.Equal(t, NFS4_OK, res.Status)

	res = d.opLookup([]byte("realfile.txt"))
	assert.Equal(t, NFS4ERR_ACCESS, res.Status, "anon denied on real file → ACCESS")
}
