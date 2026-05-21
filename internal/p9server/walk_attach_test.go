package p9server

// Walk-attach auth tests for D#6 aname encoding.
//
// hugelgupf/p9 library consumes aname into Walk(names), so the IAM gate lives
// in rootFile.Walk on names[0] rather than in Attach. See the comment at the
// top of rootFile.Walk for full spec reference.

import (
	"context"
	"syscall"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// newTestBadger opens an in-memory BadgerDB and closes it on t.Cleanup.
func newTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// stubAuthorizer lets tests inject canned allow/deny per (saID, bucket).
type stubAuthorizer struct {
	// allow contains (saID, bucket) pairs that should be allowed.
	allow map[[2]string]bool
}

func (s *stubAuthorizer) Authorize(_ context.Context, saID, bucket string, _ policy.RequestContext) policy.EvalResult {
	if s.allow[[2]string{saID, bucket}] {
		return policy.EvalResult{Decision: policy.DecisionAllow, Reason: "stub-allow"}
	}
	return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "stub-deny"}
}

// newStubMountSAStore builds an in-memory store backed by a temp BadgerDB.
// It adds each name from sas as a MountSA entry.
func newStubMountSAStore(t *testing.T, sas ...string) *mountsastore.Store {
	t.Helper()
	db := newTestBadger(t)
	store, err := mountsastore.NewStore(db)
	require.NoError(t, err)
	for _, name := range sas {
		require.NoError(t, store.ApplyCreate(mountsastore.MountSA{Name: name}))
	}
	return store
}

// TestWalk_AnameAnonOnPublicBucket_OK verifies that names[0]="default" with no
// mount-SA in the pool is treated as the anon path: authorizer is called with
// saID="" and bucket="default", and on allow, a bucketFile is returned.
func TestWalk_AnameAnonOnPublicBucket_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"", "default"}: true}}
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t), // empty pool
		authorizer:   authz,
	}

	qids, file, err := root.Walk([]string{"default"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	bf, ok := file.(*bucketFile)
	require.True(t, ok)
	require.Equal(t, "default", bf.bucket)
	require.Equal(t, "", bf.binding.saID)
	require.Equal(t, "default", bf.binding.bucket)
}

// TestWalk_AnameMountSAHit_OK verifies that names[0]="alice-mount@default"
// where alice-mount is in the pool and the authorizer allows, returns a
// bucketFile with the mount-SA binding.
func TestWalk_AnameMountSAHit_OK(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"alice-mount", "default"}: true}}
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "alice-mount"),
		authorizer:   authz,
	}

	qids, file, err := root.Walk([]string{"alice-mount@default"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	bf, ok := file.(*bucketFile)
	require.True(t, ok)
	require.Equal(t, "default", bf.bucket)
	require.Equal(t, "alice-mount", bf.binding.saID)
	require.Equal(t, "default", bf.binding.bucket)
}

// TestWalk_AnameMountSAMiss_ENOENT verifies that names[0]="typo@default" where
// "typo" is not in the mount-SA pool returns ENOENT.
func TestWalk_AnameMountSAMiss_ENOENT(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{}}
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "alice-mount"), // typo not in pool
		authorizer:   authz,
	}

	_, _, err := root.Walk([]string{"typo@default"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

// TestWalk_AnameMountSANoPolicy_EACCES verifies that a mount-SA in the pool but
// denied by policy returns EACCES (not ENOENT — mount-SA existence leak avoidance).
func TestWalk_AnameMountSANoPolicy_EACCES(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{}} // deny everything
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "bob-mount"),
		authorizer:   authz,
	}

	_, _, err := root.Walk([]string{"bob-mount@default"})
	require.ErrorIs(t, err, syscall.EACCES)
}

// TestWalk_AnameMalformedMultipleAt_SplitFirst verifies that names[0]="a@b@c"
// splits on the FIRST '@': mountSA="a", bucket="b@c". Since "b@c" contains '@'
// which is not a valid bucket-name character, HeadBucket will fail → ENOENT.
// This test documents the split-first behaviour.
func TestWalk_AnameMalformedMultipleAt_SplitFirst(t *testing.T) {
	backend := newTestBackend(t)
	// "b@c" is not a valid bucket name (@ disallowed), so no bucket created.

	authz := &stubAuthorizer{allow: map[[2]string]bool{{"a", "b@c"}: true}}
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "a"),
		authorizer:   authz,
	}

	// "a" is in pool; authz allows; but HeadBucket("b@c") fails → ENOENT.
	_, _, err := root.Walk([]string{"a@b@c"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

// TestWalk_AnameEmptyBucket_AfterAt_Error verifies that names[0]="alice-mount@"
// (empty bucket after '@') returns an error.
func TestWalk_AnameEmptyBucket_AfterAt_Error(t *testing.T) {
	backend := newTestBackend(t)

	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t, "alice-mount"),
		authorizer:   &stubAuthorizer{allow: map[[2]string]bool{}},
	}

	_, _, err := root.Walk([]string{"alice-mount@"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

// TestWalk_AnameEmptyMountSA_BeforeAt verifies that names[0]="@default"
// (empty mount-SA before '@') is rejected with ENOENT.
// Rationale: if anon access was intended, the user should write "default".
// The leading '@' is a malformed token with no defined meaning.
func TestWalk_AnameEmptyMountSA_BeforeAt(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "default"))

	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t),
		authorizer:   &stubAuthorizer{allow: map[[2]string]bool{{"", "default"}: true}},
	}

	_, _, err := root.Walk([]string{"@default"})
	require.ErrorIs(t, err, syscall.ENOENT)
}

// TestWalk_NoIAMGate_FallsBackToHeadBucket verifies that when mountSAStore is
// nil (no IAM gate wired), Walk behaves exactly as before: treats names[0] as a
// raw bucket name (the existing pre-T9 behaviour).
func TestWalk_NoIAMGate_FallsBackToHeadBucket(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "my-bucket"))

	root := &rootFile{backend: backend, locks: newObjectLocks()}
	qids, file, err := root.Walk([]string{"my-bucket"})
	require.NoError(t, err)
	require.Len(t, qids, 1)
	_, ok := file.(*bucketFile)
	require.True(t, ok)
}

// TestWalk_AnameAnonDenied_EACCES verifies that an anon request that is denied
// by the authorizer returns EACCES.
func TestWalk_AnameAnonDenied_EACCES(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()
	require.NoError(t, backend.CreateBucket(ctx, "private"))

	authz := &stubAuthorizer{allow: map[[2]string]bool{}} // deny all
	root := &rootFile{
		backend:      backend,
		locks:        newObjectLocks(),
		mountSAStore: newStubMountSAStore(t), // empty pool → anon path
		authorizer:   authz,
	}

	_, _, err := root.Walk([]string{"private"})
	require.ErrorIs(t, err, syscall.EACCES)
}
