package receipt

import (
	"testing"
)

// TestKeystoreSupportsRotationOutOfBox documents that the receipt KeyStore
// already supports the multi-PSK verification window required by the
// cluster-key rotation spec D11. No wiring change is needed in this
// package; the rotation FSM in internal/cluster/ calls
// receipt.KeyStore.Rotate(next) when phase 4 commits, and Lookup() falls
// back through the retention window for receipts signed under the prior
// active key during phase 2/3 transitions.
//
// This test is a regression guard: if KeyStore.Rotate or the
// previous-window Lookup ever changes shape, the cluster-key rotation
// integration breaks silently. Catch it here.
func TestKeystoreSupportsRotationOutOfBox(t *testing.T) {
	old := Key{ID: "old", Secret: []byte("0123456789abcdef0123456789abcdef")}
	ks, err := NewKeyStore(old)
	if err != nil {
		t.Fatal(err)
	}

	// Rotate: install new active, demote old to previous.
	newKey := Key{ID: "new", Secret: []byte("fedcba9876543210fedcba9876543210")}
	if err := ks.Rotate(newKey); err != nil {
		t.Fatal(err)
	}

	gotActive, ok := ks.Active()
	if !ok || gotActive.ID != "new" {
		t.Fatalf("post-rotate active should be 'new', got %+v", gotActive)
	}

	// Lookup of the previous key still succeeds (rotation phase 2/3
	// receipts signed under OLD must still verify).
	gotOld, ok := ks.Lookup("old")
	if !ok || gotOld.ID != "old" {
		t.Fatalf("post-rotate Lookup(\"old\") should hit retention window, got %+v", gotOld)
	}

	// Lookup of an unknown key fails.
	if _, ok := ks.Lookup("nonexistent"); ok {
		t.Fatal("Lookup of unknown key should return ok=false")
	}
}
