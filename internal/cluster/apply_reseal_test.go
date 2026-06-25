package cluster

import (
	"bytes"
	"testing"

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
