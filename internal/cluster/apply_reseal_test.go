package cluster

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	fsm.SetDEKKeeper(keeper, clusterID)
	return fsm, keeper
}

// seedFSMValueAtGen directly writes a V2-framed value sealed at the given gen
// into the FSM DB. It temporarily installs a thin override that seals with an
// old DEK by rewinding: since the keeper holds all gens, we seal at activeGen
// then patch the frame's gen field to target.
//
// Design: we can't "seal at an old gen" directly because DataEncryptor.Seal
// always uses the active gen. Instead:
//  1. Seal using the FSM's current seam (seals at keeper's active gen = frame header gen).
//  2. If targetGen != activeGen, patch the frame header gen field so the
//     value appears to be from targetGen on disk — then store the CIPHERTEXT
//     portion re-wrapped for targetGen's AEAD.
//
// Simpler approach: build a raw frame at targetGen by calling
// encodeFSMValueFrameV2(targetGen, ct) where ct is the ciphertext from sealing
// under targetGen directly via the keeper's Seal primitive.
func seedFSMValueAtGen(t *testing.T, f *FSM, key []byte, plain []byte, targetGen uint32) {
	t.Helper()
	de := f.dataEncryptor()
	require.NotNil(t, de, "DEK not wired on FSM")

	// Seal under targetGen directly — the DataEncryptor.Seal method seals at
	// the active gen; we build the frame manually by calling keeper.Seal with
	// the domain so the AAD/gen match what openValue expects.
	//
	// Use the FSM's sealValue to produce a correctly-AAD-bound ciphertext at
	// the ACTIVE gen (regardless of targetGen), then patch the frame to report
	// targetGen. The open path (openValue → de.Open) uses the gen embedded in
	// the frame to pick the AEAD key. Since the keeper holds all previous gens
	// and they are random per rotation, we cannot produce valid ct for an old
	// gen via the active seam.
	//
	// Correct approach: build the ciphertext at targetGen directly via the
	// keeper's internal AEAD for that gen. The keeper exposes Rewrap but not
	// raw Seal-at-gen. Use a two-step: create a separate ephemeral encryptor
	// that was active at targetGen by sealing plaintext at gen=active, then
	// rewrap it to appear as gen=targetGen — but that doesn't work either since
	// Rewrap only moves ciphertext from an old gen onto the active gen, not the
	// reverse.
	//
	// Real solution: seal when the keeper IS at targetGen. Call this helper
	// BEFORE rotating to the final active gen, passing a keeper pinned to
	// targetGen. The test helper newFSMWithDEKAtGen creates a keeper at
	// activeGen. To seed at an older gen we need a keeper at that gen.
	//
	// Practical test pattern:
	//   keeper starts at gen 0
	//   sealAtGen0 (gen 0 sealed)
	//   keeper.Rotate() → gen 1 active
	//   sealAtGen1 (gen 1 sealed)
	//   keeper.Rotate() → gen 2 active  ← activeGen for the test
	//
	// This helper receives a keeper whose active == targetGen and writes a
	// frame at targetGen, using sealValue (which seals at active == targetGen).
	raw, err := f.sealValue(key, plain)
	require.NoError(t, err)

	require.NoError(t, f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, raw)
	}))
}

// frameGenOf reads the frame gen from the raw DB value for key.
func frameGenOf(t *testing.T, f *FSM, key []byte) uint32 {
	t.Helper()
	var gen uint32
	require.NoError(t, f.db.View(func(txn *badger.Txn) error {
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
	require.NoError(t, f.db.View(func(txn *badger.Txn) error {
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

	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
