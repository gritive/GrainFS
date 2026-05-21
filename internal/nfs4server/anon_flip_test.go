package nfs4server

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/mountsastore"
)

// stubAnonCfg is a minimal ConfigReader test stub. It serves only the
// "iam.anon-enabled" key via an atomic.Bool so the test can flip it
// during the test body without locks.
type stubAnonCfg struct {
	anonEnabled atomic.Bool
}

func newStubAnonCfg(initial bool) *stubAnonCfg {
	c := &stubAnonCfg{}
	c.anonEnabled.Store(initial)
	return c
}

func (c *stubAnonCfg) GetBool(key string) (bool, bool) {
	if key == "iam.anon-enabled" {
		return c.anonEnabled.Load(), true
	}
	return false, false
}

func (c *stubAnonCfg) flip(v bool) {
	c.anonEnabled.Store(v)
}

// TestAnonSession_FlipAtPhase2_NextOpRejected — anon mount, first GETATTR ok,
// flip iam.anon-enabled to false, next GETATTR returns NFS4ERR_ACCESS.
func TestAnonSession_FlipAtPhase2_NextOpRejected(t *testing.T) {
	msaStore := newTestMountSAStore(t) // empty pool

	cfg := newStubAnonCfg(true)
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithConfigReader(cfg),
	)
	srv.SetExportsForTest(bucketExport("default"))

	// Manually establish an anon-bound fh (saID="") at /default.
	state := srv.state
	bucketFH := state.GetOrCreateFH("/default")
	state.BindFHWithSAID(bucketFH, "default", "", 1)

	d := &Dispatcher{
		backend:     srv.backend,
		state:       state,
		server:      srv,
		currentFH:   bucketFH,
		currentPath: "/default",
	}

	// First GETATTR succeeds (anon-enabled=true).
	res := d.opGetAttr(nil)
	require.Equal(t, NFS4_OK, res.Status, "first GETATTR must succeed while anon-enabled=true")

	// Phase 2 flip: anon disabled.
	cfg.flip(false)

	// Next GETATTR must be rejected.
	res = d.opGetAttr(nil)
	assert.Equal(t, NFS4ERR_ACCESS, res.Status, "next GETATTR must be ACCESS after flip")
}

// TestAnonSession_NoFlip_OpsContinue — anon mount, multiple ops succeed
// while anon-enabled stays true.
func TestAnonSession_NoFlip_OpsContinue(t *testing.T) {
	msaStore := newTestMountSAStore(t)

	cfg := newStubAnonCfg(true)
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithConfigReader(cfg),
	)
	srv.SetExportsForTest(bucketExport("default"))

	state := srv.state
	bucketFH := state.GetOrCreateFH("/default")
	state.BindFHWithSAID(bucketFH, "default", "", 1)

	d := &Dispatcher{
		backend:     srv.backend,
		state:       state,
		server:      srv,
		currentFH:   bucketFH,
		currentPath: "/default",
	}

	for i := 0; i < 3; i++ {
		res := d.opGetAttr(nil)
		require.Equal(t, NFS4_OK, res.Status, "GETATTR #%d must succeed while anon-enabled=true", i)
		res = d.opAccess(nil)
		require.Equal(t, NFS4_OK, res.Status, "ACCESS #%d must succeed while anon-enabled=true", i)
	}
}

// TestNonAnonSession_FlipAtPhase2_OpsContinue — mount-sa-bound session is
// unaffected by the iam.anon-enabled flip.
func TestNonAnonSession_FlipAtPhase2_OpsContinue(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(
		mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001}))

	cfg := newStubAnonCfg(true)
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithConfigReader(cfg),
	)
	srv.SetExportsForTest(bucketExport("default"))

	state := srv.state
	bucketFH := state.GetOrCreateFH("/default/alice-mount")
	state.BindFHWithSAID(bucketFH, "default", "alice-mount", 1)

	d := &Dispatcher{
		backend:     srv.backend,
		state:       state,
		server:      srv,
		currentFH:   bucketFH,
		currentPath: "/default/alice-mount",
	}

	// First GETATTR succeeds.
	res := d.opGetAttr(nil)
	require.Equal(t, NFS4_OK, res.Status)

	// Flip anon-enabled false.
	cfg.flip(false)

	// Mount-SA-bound session must continue.
	res = d.opGetAttr(nil)
	assert.Equal(t, NFS4_OK, res.Status, "mount-SA-bound session must be unaffected by anon flip")
	res = d.opAccess(nil)
	assert.Equal(t, NFS4_OK, res.Status)
}

// TestLookup_SubdirInheritsParentSAID — a real NFS client walks bucket →
// mount-SA → subdir via three sequential LOOKUPs. The 3rd LOOKUP creates a
// fresh subdir fh, which must inherit the parent's mount-SA binding (not
// reset to anon""). Without this, anonRejected would block mount-SA-bound
// subdir ops after the Phase 2 flip — the very thing T12 must NOT do.
func TestLookup_SubdirInheritsParentSAID(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(
		mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001}))

	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)

	// Mark a subdir as known so the 3rd LOOKUP succeeds without a backend.
	d.state.MarkDir("/default/alice-mount/subdir")

	// LOOKUP "default" — pending fh.
	res := d.opLookup([]byte("default"))
	require.Equal(t, NFS4_OK, res.Status, "LOOKUP default")
	b, _ := d.state.FHBinding(d.currentFH)
	require.Equal(t, fhSAIDPending, b.saID)

	// LOOKUP "alice-mount" — mount-SA confirmed fh.
	res = d.opLookup([]byte("alice-mount"))
	require.Equal(t, NFS4_OK, res.Status, "LOOKUP alice-mount")
	b, _ = d.state.FHBinding(d.currentFH)
	require.Equal(t, "alice-mount", b.saID)

	// LOOKUP "subdir" — subdir fh must inherit "alice-mount" saID.
	res = d.opLookup([]byte("subdir"))
	require.Equal(t, NFS4_OK, res.Status, "LOOKUP subdir")
	b, _ = d.state.FHBinding(d.currentFH)
	assert.Equal(t, "alice-mount", b.saID,
		"subdir fh must inherit parent's mount-SA saID, not be re-anonymized")
}

// TestSubdirSession_FlipAtPhase2_OpsContinue — the cross-cut of the two
// tests above: walking into a mount-SA subdir, then flipping anon, must
// NOT reject the subdir's GETATTR.
func TestSubdirSession_FlipAtPhase2_OpsContinue(t *testing.T) {
	msaStore := newTestMountSAStore(t)
	require.NoError(t, msaStore.ApplyCreate(
		mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001}))

	cfg := newStubAnonCfg(true)
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
		WithConfigReader(cfg),
	)
	srv.SetExportsForTest(bucketExport("default"))
	d := newAuthDispatcher(srv)
	d.state.MarkDir("/default/alice-mount/subdir")

	require.Equal(t, NFS4_OK, d.opLookup([]byte("default")).Status)
	require.Equal(t, NFS4_OK, d.opLookup([]byte("alice-mount")).Status)
	require.Equal(t, NFS4_OK, d.opLookup([]byte("subdir")).Status)

	// First GETATTR.
	require.Equal(t, NFS4_OK, d.opGetAttr(nil).Status)

	// Flip anon disabled.
	cfg.flip(false)

	// Subdir GETATTR must remain OK — it inherits alice-mount saID.
	assert.Equal(t, NFS4_OK, d.opGetAttr(nil).Status,
		"mount-SA-bound subdir session must be unaffected by anon flip")
}

// TestAnonRejected_NoConfigReader_NoGate — without ConfigReader wired, the
// guard is inert (backward-compat). The flip cannot reject because the
// server doesn't read cfg at all.
func TestAnonRejected_NoConfigReader_NoGate(t *testing.T) {
	msaStore := newTestMountSAStore(t)

	// No WithConfigReader.
	srv := NewServer(nil,
		WithMountSAStore(msaStore),
		WithNFS4Authorizer(newAllowAuthorizer()),
	)
	srv.SetExportsForTest(bucketExport("default"))

	state := srv.state
	bucketFH := state.GetOrCreateFH("/default")
	state.BindFHWithSAID(bucketFH, "default", "", 1)

	d := &Dispatcher{
		backend:     srv.backend,
		state:       state,
		server:      srv,
		currentFH:   bucketFH,
		currentPath: "/default",
	}

	// With no ConfigReader, anon binding stays usable.
	res := d.opGetAttr(nil)
	assert.Equal(t, NFS4_OK, res.Status)
}
