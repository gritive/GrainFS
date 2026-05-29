package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestDistributedBackendHasNoCopyFastPath pins the prod-copy invariant behind the
// #667 AAD fix: a cluster CopyObject MUST route through Operations' re-encode
// fallback (openCopySource → putObjectWithRequest), never a backend fast-path.
//
// Operations picks a copy fast-path by type-asserting the backend for the Copier
// or copyObjectAccelerator interfaces (storage/operations.go). DistributedBackend
// implements neither, so both stay nil and copyObject falls through to the
// fallback path — openCopySource (GetObject → decrypt) then putObjectWithRequest
// (re-seal under the dst identity). That fallback RE-ENCRYPTS, so it is immune to
// the bug class by construction: it cannot preserve source ciphertext, and a
// broken fallback would fail loudly rather than silently emit an un-decryptable
// dst. (#667's TestCopyObjectSegmentedEncryptedReEncodes exercises a different
// path — LocalBackend.CopyObject's internal re-encode branch, taken because
// LocalBackend IS a Copier — not this Operations fallback.)
//
// The only way to introduce ciphertext-preservation on the cluster copy is to add
// a copy fast-path; this guard fences that. If a future change adds a Copier or
// accelerator to DistributedBackend that raw-copies EC shards, the dst would
// inherit segment AAD bound to the SOURCE (bucket,key,blobID) and be
// un-decryptable — the exact bug class #667 fixed for the test-fixture
// LocalBackend. This assertion goes RED the moment such a method is added.
//
// Scope note: the true invariant is "no cluster backend type implements these"
// (today verified by an empty grep for CopyObject/CopyObjectWithRequest across
// internal/cluster, since Operations walks the whole backend unwrap chain). This
// assertion fences the concrete prod backend; a Copier hidden on a wrapper would
// not trip it.
func TestDistributedBackendHasNoCopyFastPath(t *testing.T) {
	var b storage.Backend = (*DistributedBackend)(nil)

	_, isCopier := b.(storage.Copier)
	require.False(t, isCopier,
		"DistributedBackend must not implement storage.Copier — a copy fast-path would bypass the re-encode path and raw-copy EC shards (un-decryptable dst, #667 bug class)")

	// Mirror of storage's unexported copyObjectAccelerator interface (the other
	// fast-path Operations probes). Kept local because the interface is package-
	// private to storage.
	type copyObjectAccelerator interface {
		CopyObjectWithRequest(ctx context.Context, req storage.CopyObjectAccelerationRequest) (*storage.Object, error)
	}
	_, isAccel := b.(copyObjectAccelerator)
	require.False(t, isAccel,
		"DistributedBackend must not implement a copy accelerator — same reason: it would bypass the re-encode path")
}
