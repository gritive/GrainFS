package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
