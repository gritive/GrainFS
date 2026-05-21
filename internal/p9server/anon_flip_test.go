package p9server

import (
	"bytes"
	"context"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hugelgupf/p9/p9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubAnonCfg is a minimal ConfigReader stub. It serves the
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

// walkAnonBucket performs a fresh root.Walk to obtain an anon-bound bucketFile.
func walkAnonBucket(t *testing.T, root *rootFile, name string) *bucketFile {
	t.Helper()
	_, file, err := root.Walk([]string{name})
	require.NoError(t, err)
	bf, ok := file.(*bucketFile)
	require.True(t, ok)
	require.Equal(t, "", bf.binding.saID, "must be anon-bound")
	return bf
}

// TestAnon9PSession_FlipAtPhase2_NextOpRejected — anon attach, first GetAttr
// succeeds, flip iam.anon-enabled=false, next op (Readdir/Create/etc.)
// returns EACCES.
//
// Uses "userbucket" (not "default") because FU#6 carved the "default" bucket
// out of the per-op anon flip gate to honor D#2 implicit-anon. Default-bucket
// flip behavior is covered by TestAnon9PSession_DefaultBucket_FlipNotRejected.
func TestAnon9PSession_FlipAtPhase2_NextOpRejected(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "userbucket"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "userbucket"}: true}}
	cfg := newStubAnonCfg(true)
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   authz,
		cfg:          cfg,
	}

	bf := walkAnonBucket(t, root, "userbucket")
	require.Equal(t, cfg, bf.cfg, "cfg must propagate")

	// First op succeeds.
	_, _, _, err := bf.GetAttr(p9.AttrMask{Mode: true})
	require.NoError(t, err, "GetAttr must succeed while anon-enabled=true")

	// Flip.
	cfg.flip(false)

	// Next op must reject with EACCES.
	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	assert.ErrorIs(t, err, syscall.EACCES, "GetAttr after flip must be EACCES")

	// Other anon-gated ops also reject.
	_, err = bf.Readdir(0, 16)
	assert.ErrorIs(t, err, syscall.EACCES, "Readdir after flip must be EACCES")

	_, _, err = bf.Walk([]string{"foo"})
	assert.ErrorIs(t, err, syscall.EACCES, "Walk after flip must be EACCES")

	_, _, _, err = bf.Create("newfile", p9.WriteOnly, 0644, 0, 0)
	assert.ErrorIs(t, err, syscall.EACCES, "Create after flip must be EACCES")

	_, err = bf.Mkdir("newdir", 0755, 0, 0)
	assert.ErrorIs(t, err, syscall.EACCES, "Mkdir after flip must be EACCES")

	err = bf.UnlinkAt("foo", 0)
	assert.ErrorIs(t, err, syscall.EACCES, "UnlinkAt after flip must be EACCES")

	err = bf.RenameAt("a", bf, "b")
	assert.ErrorIs(t, err, syscall.EACCES, "RenameAt after flip must be EACCES")
}

// TestAnon9PSession_NoFlip_OpsContinue — anon attach, multiple ops succeed
// while anon-enabled stays true.
func TestAnon9PSession_NoFlip_OpsContinue(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "default"}: true}}
	cfg := newStubAnonCfg(true)
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   authz,
		cfg:          cfg,
	}

	bf := walkAnonBucket(t, root, "default")

	for i := 0; i < 3; i++ {
		_, _, _, err := bf.GetAttr(p9.AttrMask{Mode: true})
		require.NoError(t, err, "GetAttr #%d must succeed while anon-enabled=true", i)
		_, err = bf.Readdir(0, 16)
		require.NoError(t, err, "Readdir #%d must succeed", i)
	}
}

// TestNonAnon9PSession_FlipAtPhase2_OpsContinue — mount-SA-bound session is
// unaffected by the iam.anon-enabled flip.
func TestNonAnon9PSession_FlipAtPhase2_OpsContinue(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"alice-mount", "default"}: true}}
	cfg := newStubAnonCfg(true)
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "alice-mount"),
		authorizer:   authz,
		cfg:          cfg,
	}

	_, file, err := root.Walk([]string{"alice-mount@default"})
	require.NoError(t, err)
	bf, ok := file.(*bucketFile)
	require.True(t, ok)
	require.Equal(t, "alice-mount", bf.binding.saID)

	// First op succeeds.
	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	require.NoError(t, err)

	// Flip anon disabled.
	cfg.flip(false)

	// Mount-SA-bound ops must continue.
	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "mount-SA session unaffected by anon flip — GetAttr")
	_, err = bf.Readdir(0, 16)
	assert.NoError(t, err, "mount-SA session unaffected by anon flip — Readdir")
}

// TestAnon9PObjectFile_FlipAtPhase2_NextOpRejected — anon-bound objectFile
// (created via bucketFile.Create or .Walk) also rejects ops after flip.
//
// Uses "userbucket" (not "default") because FU#6 carved the "default" bucket
// out of the per-op anon flip gate.
func TestAnon9PObjectFile_FlipAtPhase2_NextOpRejected(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "userbucket"))
	_, err := backend.PutObject(ctx, "userbucket", "file.txt",
		bytes.NewReader([]byte("hello")), "text/plain")
	require.NoError(t, err)

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "userbucket"}: true}}
	cfg := newStubAnonCfg(true)
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   authz,
		cfg:          cfg,
	}

	bf := walkAnonBucket(t, root, "userbucket")

	_, file, err := bf.Walk([]string{"file.txt"})
	require.NoError(t, err)
	of, ok := file.(*objectFile)
	require.True(t, ok)
	require.Equal(t, "", of.binding.saID, "objectFile binding must inherit anon")

	// First op succeeds.
	_, _, _, err = of.GetAttr(p9.AttrMask{Mode: true})
	require.NoError(t, err)

	// Flip.
	cfg.flip(false)

	// Object-level ops also reject.
	_, _, _, err = of.GetAttr(p9.AttrMask{Mode: true})
	assert.ErrorIs(t, err, syscall.EACCES, "objectFile.GetAttr after flip must be EACCES")

	buf := make([]byte, 4)
	_, err = of.ReadAt(buf, 0)
	assert.ErrorIs(t, err, syscall.EACCES, "objectFile.ReadAt after flip must be EACCES")

	_, err = of.WriteAt([]byte("x"), 0)
	assert.ErrorIs(t, err, syscall.EACCES, "objectFile.WriteAt after flip must be EACCES")

	err = of.SetAttr(p9.SetAttrMask{Permissions: true}, p9.SetAttr{Permissions: 0600})
	assert.ErrorIs(t, err, syscall.EACCES, "objectFile.SetAttr after flip must be EACCES")
}

// TestAnon9PSession_DefaultBucket_FlipNotRejected — FU#6: anon-bound session
// on the "default" bucket must NOT be rejected after the Phase 0→2 flip. The
// D#2 carve-out (ReasonDefaultBucketImplicitAnon) guarantees default-bucket
// anon access survives the flip; the per-op gate honors that.
func TestAnon9PSession_DefaultBucket_FlipNotRejected(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "default"}: true}}
	cfg := newStubAnonCfg(true)
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   authz,
		cfg:          cfg,
	}

	bf := walkAnonBucket(t, root, "default")

	// First op succeeds.
	_, _, _, err := bf.GetAttr(p9.AttrMask{Mode: true})
	require.NoError(t, err, "GetAttr must succeed while anon-enabled=true")

	// Flip Phase 2.
	cfg.flip(false)

	// Default-bucket anon ops must continue to work — D#2 implicit anon is
	// Phase-agnostic.
	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "GetAttr on /default after flip must succeed (D#2)")
	_, err = bf.Readdir(0, 16)
	assert.NoError(t, err, "Readdir on /default after flip must succeed (D#2)")
}

// TestAnon9P_NoConfigReader_NoGate — without cfg wired, the guard is inert
// (backward compat).
func TestAnon9P_NoConfigReader_NoGate(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "default"}: true}}
	// No cfg field set.
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   authz,
	}

	bf := walkAnonBucket(t, root, "default")
	_, _, _, err := bf.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "without cfg, anon binding stays usable")
}
