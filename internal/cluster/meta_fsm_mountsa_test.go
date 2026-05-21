package cluster

import (
	"context"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/iam/policyattach"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestMountSAStore creates an in-memory Badger-backed mountsastore for testing.
func newTestMountSAStore(t *testing.T) *mountsastore.Store {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	s, err := mountsastore.NewStore(db)
	require.NoError(t, err)
	return s
}

// buildMountSACreateCmd encodes a MountSACreate MetaCmd.
func buildMountSACreateCmd(t *testing.T, sa mountsastore.MountSA) []byte {
	t.Helper()
	payload := mountsastore.EncodeCreatePayload(sa)
	cmd, err := encodeMetaCmd(MetaCmdTypeMountSACreate, payload)
	require.NoError(t, err, "encodeMetaCmd MountSACreate")
	return cmd
}

// buildMountSADeleteCmd encodes a MountSADelete MetaCmd.
func buildMountSADeleteCmd(t *testing.T, name string) []byte {
	t.Helper()
	payload := mountsastore.EncodeDeletePayload(name)
	cmd, err := encodeMetaCmd(MetaCmdTypeMountSADelete, payload)
	require.NoError(t, err, "encodeMetaCmd MountSADelete")
	return cmd
}

// buildMountSAAttachPolicyCmd encodes a MountSAAttachPolicy MetaCmd.
func buildMountSAAttachPolicyCmd(t *testing.T, mountSA, policy string) []byte {
	t.Helper()
	payload := mountsastore.EncodeAttachPolicyPayload(mountSA, policy)
	cmd, err := encodeMetaCmd(MetaCmdTypeMountSAAttachPolicy, payload)
	require.NoError(t, err, "encodeMetaCmd MountSAAttachPolicy")
	return cmd
}

// buildMountSADetachPolicyCmd encodes a MountSADetachPolicy MetaCmd.
func buildMountSADetachPolicyCmd(t *testing.T, mountSA, policy string) []byte {
	t.Helper()
	payload := mountsastore.EncodeDetachPolicyPayload(mountSA, policy)
	cmd, err := encodeMetaCmd(MetaCmdTypeMountSADetachPolicy, payload)
	require.NoError(t, err, "encodeMetaCmd MountSADetachPolicy")
	return cmd
}

// TestApplyMountSACreate verifies that applying a MountSACreate command
// persists the entry in the store.
func TestApplyMountSACreate(t *testing.T) {
	f := NewMetaFSM()
	store := newTestMountSAStore(t)
	f.SetMountSAStore(store)

	sa := mountsastore.MountSA{Name: "alice-mount", NumericUID: 200001, CreatedAt: 1700000000}
	require.NoError(t, f.applyCmd(buildMountSACreateCmd(t, sa)))

	got, ok := store.Get("alice-mount")
	require.True(t, ok)
	require.Equal(t, uint32(200001), got.NumericUID)
}

// TestApplyMountSADelete verifies that a MountSADelete command removes the entry.
func TestApplyMountSADelete(t *testing.T) {
	f := NewMetaFSM()
	store := newTestMountSAStore(t)
	f.SetMountSAStore(store)

	sa := mountsastore.MountSA{Name: "a", NumericUID: 1, CreatedAt: 1}
	require.NoError(t, f.applyCmd(buildMountSACreateCmd(t, sa)))
	require.NoError(t, f.applyCmd(buildMountSADeleteCmd(t, "a")))

	_, ok := store.Get("a")
	require.False(t, ok)
}

// TestApplyMountSAAttachDetachPolicy verifies that policy attachment/detachment
// are reflected in the policyAttachStore.
func TestApplyMountSAAttachDetachPolicy(t *testing.T) {
	f := NewMetaFSM()
	mountStore := newTestMountSAStore(t)
	attachStore := policyattach.NewInMemoryStore()
	f.SetMountSAStore(mountStore)
	f.SetPolicyAttachStore(attachStore)

	// Attach
	require.NoError(t, f.applyCmd(buildMountSAAttachPolicyCmd(t, "bob-mount", "nfs-readonly")))

	pols, err := attachStore.MountSAPolicies(context.Background(), "bob-mount")
	require.NoError(t, err)
	require.Equal(t, []string{"nfs-readonly"}, pols)

	// Detach
	require.NoError(t, f.applyCmd(buildMountSADetachPolicyCmd(t, "bob-mount", "nfs-readonly")))

	pols, err = attachStore.MountSAPolicies(context.Background(), "bob-mount")
	require.NoError(t, err)
	require.Empty(t, pols)
}

// TestSnapshotRestore_MountSAIncluded verifies that MountSA entries (both store
// and policy attachments) survive a Snapshot → Restore round-trip.
func TestSnapshotRestore_MountSAIncluded(t *testing.T) {
	// Source FSM: wire IPST stores + MountSA store.
	src := NewMetaFSM()
	wireIPSTStores(src)
	srcMountStore := newTestMountSAStore(t)
	src.SetMountSAStore(srcMountStore)

	// Seed: create two MountSAs.
	sa1 := mountsastore.MountSA{Name: "nfs-client-1", NumericUID: 300001, CreatedAt: 1700000001}
	sa2 := mountsastore.MountSA{Name: "nfs-client-2", NumericUID: 300002, CreatedAt: 1700000002}
	require.NoError(t, src.applyCmd(buildMountSACreateCmd(t, sa1)))
	require.NoError(t, src.applyCmd(buildMountSACreateCmd(t, sa2)))

	// Attach a policy to sa1.
	require.NoError(t, src.applyCmd(buildMountSAAttachPolicyCmd(t, "nfs-client-1", "nfs-readonly")))

	// Take snapshot.
	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Restore into a fresh FSM.
	dst := NewMetaFSM()
	wireIPSTStores(dst)
	dstMountStore := newTestMountSAStore(t)
	dstAttachStore := policyattach.NewInMemoryStore()
	dst.SetMountSAStore(dstMountStore)
	dst.SetPolicyAttachStore(dstAttachStore)

	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))

	// Verify MountSA store.
	got1, ok := dstMountStore.Get("nfs-client-1")
	require.True(t, ok, "nfs-client-1 must be restored")
	require.Equal(t, uint32(300001), got1.NumericUID)

	got2, ok := dstMountStore.Get("nfs-client-2")
	require.True(t, ok, "nfs-client-2 must be restored")
	require.Equal(t, uint32(300002), got2.NumericUID)

	// Verify policy attachment.
	pols, err := dstAttachStore.MountSAPolicies(context.Background(), "nfs-client-1")
	require.NoError(t, err)
	require.Equal(t, []string{"nfs-readonly"}, pols)

	// sa2 has no attachments.
	pols2, err := dstAttachStore.MountSAPolicies(context.Background(), "nfs-client-2")
	require.NoError(t, err)
	require.Empty(t, pols2)
}

// TestSnapshotRestore_MountSANilStore_WarnOnly verifies that restoring a snapshot
// with MountSA entries into an FSM that has no mountSAStore wired returns no error.
func TestSnapshotRestore_MountSANilStore_WarnOnly(t *testing.T) {
	src := NewMetaFSM()
	wireIPSTStores(src)
	srcMountStore := newTestMountSAStore(t)
	src.SetMountSAStore(srcMountStore)

	sa := mountsastore.MountSA{Name: "x", NumericUID: 1, CreatedAt: 1}
	require.NoError(t, src.applyCmd(buildMountSACreateCmd(t, sa)))

	snap, err := src.Snapshot()
	require.NoError(t, err)

	// Restore into FSM with no mountSAStore wired — must not error.
	dst := NewMetaFSM()
	wireIPSTStores(dst)
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))
}

// TestApplyMountSACreate_StoreNotWired verifies that commands return an error
// when the store has not been wired.
func TestApplyMountSACreate_StoreNotWired(t *testing.T) {
	f := NewMetaFSM()
	// mountSAStore deliberately not wired

	sa := mountsastore.MountSA{Name: "x", NumericUID: 1, CreatedAt: 1}
	err := f.applyCmd(buildMountSACreateCmd(t, sa))
	require.Error(t, err)
	require.Contains(t, err.Error(), "store not wired")
}
