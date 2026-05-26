package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestKEKStatusReaderAdapter_KEKStoreVersions_UnionPrunedVersion verifies that
// the status reader reports the union of the live keystore versions and the
// lifecycle-tracked versions. A pruned version is unlinked from the keystore
// but its kek_status record persists; the union keeps it visible so operators
// can still confirm the prune ("pruned" status) via GET /v1/encrypt/kek/status.
//
// This regression covers the gap surfaced by the Task 15 single-node prune
// e2e: enumerating only keystore.Versions() dropped a pruned version from the
// status response entirely.
func TestKEKStatusReaderAdapter_KEKStoreVersions_UnionPrunedVersion(t *testing.T) {
	fsm := cluster.NewMetaFSM()
	store := encrypt.NewKEKStore()
	for _, v := range []uint32{0, 1, 2} {
		require.NoError(t, store.Add(v, make([]byte, encrypt.KEKSize)))
	}
	require.NoError(t, store.SetActiveVersion(2))
	fsm.SetKEKStore(store)

	// Version 1 is retired then pruned: lifecycle record exists, keystore entry
	// removed (simulating RemoveAndUnlink dropping the in-memory entry).
	fsm.SetKEKStatus(1, cluster.KEKLifecycleRetiring, 10)
	fsm.SetKEKStatus(1, cluster.KEKLifecyclePruned, 11)
	require.NoError(t, store.RemoveAndUnlink("", 1))

	adapter := kekStatusReaderAdapter{fsm: fsm}

	got := adapter.KEKStoreVersions()
	require.Equal(t, []uint32{0, 1, 2}, got,
		"pruned version 1 must remain visible via the keystore∪lifecycle union")

	// And it must report as pruned, not active.
	require.Equal(t, "pruned", kekVersionStatusFor(t, adapter, 1))
}

// kekVersionStatusFor resolves the lifecycle status string the status handler
// would render for version v, exercising the same LookupKEKStatus path.
func kekVersionStatusFor(t *testing.T, r kekStatusReaderAdapter, v uint32) string {
	t.Helper()
	_, status, _, ok := r.LookupKEKStatus(v)
	require.True(t, ok, "version %d must have a lifecycle record", v)
	require.Equal(t, cluster.KEKLifecyclePruned, status)
	return "pruned"
}
