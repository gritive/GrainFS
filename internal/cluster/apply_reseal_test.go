package cluster

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// newFSMWithDEKAtGen creates an FSM whose keeper has been rotated so the active
// DEK generation equals activeGen. The FSM db is also wired to the keeper.
// activeGen must be >= 1 (gen 0 is always installed on creation; Rotate once -> gen 1).
func newFSMWithDEKAtGen(t *testing.T, activeGen uint32) (*FSM, *encrypt.DEKKeeper) {
	t.Helper()
	db := newTestDB(t)
	clusterID := bytes.Repeat([]byte{0x71}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	// Advance keeper to activeGen: it starts at 0; Rotate advances by 1.
	for i := uint32(0); i < activeGen; i++ {
		require.NoError(t, keeper.Rotate())
	}
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	fsm.SetDEKKeeper(keeper, clusterID)
	return fsm, keeper
}

// frameGenOf reads the frame gen from the raw DB value for key.
func frameGenOf(t *testing.T, f *FSM, key []byte) uint32 {
	t.Helper()
	var gen uint32
	require.NoError(t, f.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		g, _, ok, derr := decodeFSMValueFrameV2(raw)
		if derr != nil {
			return derr
		}
		if !ok {
			t.Fatal("expected a V2 frame")
		}
		gen = g
		return nil
	}))
	return gen
}

// mustOpenValue reads and decrypts the value for key from the FSM DB.
func mustOpenValue(t *testing.T, f *FSM, key []byte) []byte {
	t.Helper()
	var plain []byte
	require.NoError(t, f.db.View(func(txn MetadataTxn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		plain, err = f.itemValueCopy(item)
		return err
	}))
	return plain
}

// mustApplyCmd encodes cmdType+payload and applies it through the FSM.
func mustApplyCmd(t *testing.T, f *FSM, cmdType CommandType, payload any) {
	t.Helper()
	data, err := EncodeCommand(cmdType, payload)
	require.NoError(t, err)
	require.NoError(t, f.Apply(data))
}

// TestApplyResealFSMValues_ResealsStaleSkipsActive verifies that:
//   - a value at gen 1 (stale) is resealed to gen 2 (active), preserving plaintext
//   - a value at gen 2 (already active) is skipped (idempotent)
//
// This uses the rotation-based seeding strategy: seal at gen 1 by having the
// keeper at gen 1 when seeding, then advance to gen 2 as the active gen.
func TestApplyResealFSMValues_ResealsStaleSkipsActive(t *testing.T) {
	// Step 1: create keeper starting at gen 0.
	db := newTestDB(t)
	clusterID := bytes.Repeat([]byte{0x71}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)

	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Step 2: wire keeper at gen 0, write the object meta key value (will be sealed at gen 0).
	// Then rotate so keeper becomes gen 1.
	// Then write the policy key value (sealed at gen 1).
	// Then rotate so keeper becomes gen 2 = final activeGen.

	// At gen 0: seal "already-active" key (we'll make this appear as gen 2 by
	// doing TWO rotates before writing... actually we write AFTER both rotates
	// to get gen 2). Let's re-order:
	//
	// Write stale value: need keeper at gen 1 active.
	// Write active value: need keeper at gen 2 active.
	//
	// Sequence:
	//   Rotate 0→1. Wire keeper. Seal stale at gen 1.
	//   Rotate 1→2. Seal "active" at gen 2.
	//   Run applyResealFSMValues with activeGen=2.

	// Advance to gen 1 and seal the stale value.
	require.NoError(t, keeper.Rotate()) // now gen 1 is active
	fsm.SetDEKKeeper(keeper, clusterID)

	polKey := []byte("policy:b1")
	staleRaw, err := fsm.sealValue(polKey, []byte(`{"v":1}`)) // sealed at gen 1
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(polKey, staleRaw)
	}))
	// Verify gen 1 is on disk.
	require.Equal(t, uint32(1), frameGenOf(t, fsm, polKey))

	// Advance to gen 2 and seal the already-active value.
	require.NoError(t, keeper.Rotate()) // now gen 2 is active

	objKey := []byte("obj:b1/k")
	activeRaw, err := fsm.sealValue(objKey, []byte(`{"m":1}`)) // sealed at gen 2
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(objKey, activeRaw)
	}))
	require.Equal(t, uint32(2), frameGenOf(t, fsm, objKey))

	// Apply CmdResealFSMValues with both keys and activeGen=2.
	cmd := ResealFSMValuesCmd{Keys: []string{string(polKey), string(objKey)}, ActiveGen: 2}
	mustApplyCmd(t, fsm, CmdResealFSMValues, cmd)

	// stale policy should now be at gen 2.
	require.Equal(t, uint32(2), frameGenOf(t, fsm, polKey), "stale key should be migrated to gen 2")
	// active obj should remain at gen 2.
	require.Equal(t, uint32(2), frameGenOf(t, fsm, objKey), "active key must not change gen")

	// Plaintext is preserved after reseal.
	require.JSONEq(t, `{"v":1}`, string(mustOpenValue(t, fsm, polKey)))
	require.JSONEq(t, `{"m":1}`, string(mustOpenValue(t, fsm, objKey)))
}

// TestApplyResealFSMValues_MissingKeyIsSkipped verifies that a key not found
// in the DB is skipped cleanly (not an error).
func TestApplyResealFSMValues_MissingKeyIsSkipped(t *testing.T) {
	fsm, _ := newFSMWithDEKAtGen(t, 1)
	cmd := ResealFSMValuesCmd{Keys: []string{"policy:missing"}, ActiveGen: 1}
	mustApplyCmd(t, fsm, CmdResealFSMValues, cmd)
	// no error = pass
}
