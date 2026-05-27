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

// stubAnonCfg is a minimal ConfigReader stub. The old global anonymous config
// key is gone, so flips here must not affect established bindings.
type stubAnonCfg struct {
	anonEnabled atomic.Bool
}

func newStubAnonCfg(initial bool) *stubAnonCfg {
	c := &stubAnonCfg{}
	c.anonEnabled.Store(initial)
	return c
}

func (c *stubAnonCfg) GetBool(key string) (bool, bool) {
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

func TestAnon9PSession_ConfigFlipDoesNotRevokeBinding(t *testing.T) {
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
	require.NoError(t, err, "GetAttr must succeed")

	cfg.flip(false)

	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "removed anon config must not revoke GetAttr")

	_, err = bf.Readdir(0, 16)
	assert.NoError(t, err, "removed anon config must not revoke Readdir")

	_, _, err = bf.Walk([]string{"foo"})
	assert.ErrorIs(t, err, syscall.ENOENT, "Walk for missing file should reach backend")

	_, _, _, err = bf.Create("newfile", p9.WriteOnly, 0644, 0, 0)
	assert.NoError(t, err, "removed anon config must not revoke Create")

	_, err = bf.Mkdir("newdir", 0755, 0, 0)
	assert.NoError(t, err, "removed anon config must not revoke Mkdir")

	err = bf.UnlinkAt("foo", 0)
	assert.ErrorIs(t, err, syscall.ENOENT, "UnlinkAt for missing file should reach backend")

	err = bf.RenameAt("a", bf, "b")
	assert.ErrorIs(t, err, syscall.ENOENT, "RenameAt for missing file should reach backend")
}

// TestAnon9PSession_NoFlip_OpsContinue — anon attach, multiple ops succeed.
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
		require.NoError(t, err, "GetAttr #%d must succeed", i)
		_, err = bf.Readdir(0, 16)
		require.NoError(t, err, "Readdir #%d must succeed", i)
	}
}

func TestNonAnon9PSession_ConfigFlipOpsContinue(t *testing.T) {
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

	// Flip the inert legacy test config.
	cfg.flip(false)

	// Mount-SA-bound ops must continue.
	_, _, _, err = bf.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "mount-SA session unaffected by anon flip — GetAttr")
	_, err = bf.Readdir(0, 16)
	assert.NoError(t, err, "mount-SA session unaffected by anon flip — Readdir")
}

func TestAnon9PObjectFile_ConfigFlipDoesNotRevokeBinding(t *testing.T) {
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

	cfg.flip(false)

	_, _, _, err = of.GetAttr(p9.AttrMask{Mode: true})
	assert.NoError(t, err, "removed anon config must not revoke objectFile.GetAttr")

	buf := make([]byte, 4)
	_, err = of.ReadAt(buf, 0)
	assert.NoError(t, err, "removed anon config must not revoke objectFile.ReadAt")

	_, err = of.WriteAt([]byte("x"), 0)
	assert.NoError(t, err, "removed anon config must not revoke objectFile.WriteAt")

	err = of.SetAttr(p9.SetAttrMask{Permissions: true}, p9.SetAttr{Permissions: 0600})
	assert.ErrorIs(t, err, syscall.EPERM, "SetAttr permission behavior should reach backend")
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
	require.NoError(t, err, "GetAttr must succeed")

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
