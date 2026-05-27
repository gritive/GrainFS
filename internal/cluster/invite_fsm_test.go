package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

func testPub(t *testing.T) ed25519.PublicKey {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func TestInviteFSM_MintConsumeOnce(t *testing.T) {
	fsm := newInviteFSM()
	id, pub := "inv-a", testPub(t)
	now := time.Unix(1000, 0)
	fsm.applyMint(id, pub, now.Add(15*time.Minute).UnixNano())
	gotPub, ok := fsm.lookup(id, now)
	if !ok || string(gotPub) != string(pub) {
		t.Fatal("freshly minted invite should be valid and return its pubkey")
	}
	if err := fsm.applyConsume(id, now); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if _, ok := fsm.lookup(id, now); ok {
		t.Fatal("consumed invite must no longer be valid (single-use)")
	}
	if err := fsm.applyConsume(id, now); err == nil {
		t.Fatal("second consume must fail (already used)")
	}
}

func TestInviteFSM_Expiry(t *testing.T) {
	fsm := newInviteFSM()
	id, pub := "inv-b", testPub(t)
	now := time.Unix(1000, 0)
	fsm.applyMint(id, pub, now.Add(1*time.Minute).UnixNano())
	if _, ok := fsm.lookup(id, now.Add(2*time.Minute)); ok {
		t.Fatal("expired invite must be invalid")
	}
	if err := fsm.applyConsume(id, now.Add(2*time.Minute)); err == nil {
		t.Fatal("consuming an expired invite must fail")
	}
}

func TestInviteFSM_ExactlyAtExpiry(t *testing.T) {
	fsm := newInviteFSM()
	id, pub := "inv-c", testPub(t)
	ttl := 5 * time.Minute
	now := time.Unix(2000, 0)
	expiry := now.Add(ttl)
	fsm.applyMint(id, pub, expiry.UnixNano())
	// At exactly expiry (now.UnixNano() >= expiryNanos) → invalid.
	if _, ok := fsm.lookup(id, expiry); ok {
		t.Fatal("invite at exactly expiry boundary must be invalid (>= check)")
	}
	if err := fsm.applyConsume(id, expiry); err == nil {
		t.Fatal("consuming at exactly expiry must fail")
	}
}

func TestInviteFSM_ConsumeUnminted(t *testing.T) {
	fsm := newInviteFSM()
	if err := fsm.applyConsume("never-minted", time.Unix(1000, 0)); err == nil {
		t.Fatal("consuming a never-minted invite must error")
	}
}

// TestInviteFSM_PendingBindFirstRedeemer covers the Phase-1 pending-redemption
// record: a present/unexpired/unused invite is bound to the first (nodeID,SPKI)
// that redeems it, re-redemption by the SAME identity is idempotent, and a
// DIFFERENT identity is rejected (the invite stays bound to the first redeemer).
func TestInviteFSM_PendingBindFirstRedeemer(t *testing.T) {
	now := time.Unix(3000, 0)
	mint := func() *inviteFSM {
		fsm := newInviteFSM()
		fsm.applyMint("inv-p", testPub(t), now.Add(10*time.Minute).UnixNano())
		return fsm
	}
	s1, s2 := spki(1), spki(2)

	t.Run("bind first redeemer then lookup", func(t *testing.T) {
		fsm := mint()
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.UnixNano()); err != nil {
			t.Fatalf("first applyPending: %v", err)
		}
		gotNode, gotSPKI, gotAddr, ok := fsm.lookupPending("inv-p")
		if !ok {
			t.Fatal("lookupPending must report bound after applyPending")
		}
		if gotNode != "node-1" || gotSPKI != s1 || gotAddr != "10.0.0.1:7001" {
			t.Fatalf("lookupPending = (%q,%v,%q), want (node-1,s1,10.0.0.1:7001)", gotNode, gotSPKI, gotAddr)
		}
	})

	t.Run("same identity is idempotent", func(t *testing.T) {
		fsm := mint()
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.UnixNano()); err != nil {
			t.Fatalf("first applyPending: %v", err)
		}
		// Re-retrieval by the same redeemer: no error, same binding.
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.Add(time.Second).UnixNano()); err != nil {
			t.Fatalf("idempotent applyPending: %v", err)
		}
		gotNode, gotSPKI, _, ok := fsm.lookupPending("inv-p")
		if !ok || gotNode != "node-1" || gotSPKI != s1 {
			t.Fatal("binding must be unchanged after idempotent re-pend")
		}
	})

	t.Run("different node rejected", func(t *testing.T) {
		fsm := mint()
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.UnixNano()); err != nil {
			t.Fatalf("first applyPending: %v", err)
		}
		if err := fsm.applyPending("inv-p", "node-2", s1, "10.0.0.2:7001", now.UnixNano()); !errors.Is(err, errInvitePendingMismatch) {
			t.Fatalf("different node must be rejected with errInvitePendingMismatch, got %v", err)
		}
		// Stays bound to first redeemer.
		gotNode, _, _, ok := fsm.lookupPending("inv-p")
		if !ok || gotNode != "node-1" {
			t.Fatal("invite must stay bound to first redeemer after mismatch")
		}
	})

	t.Run("different spki rejected", func(t *testing.T) {
		fsm := mint()
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.UnixNano()); err != nil {
			t.Fatalf("first applyPending: %v", err)
		}
		if err := fsm.applyPending("inv-p", "node-1", s2, "10.0.0.1:7001", now.UnixNano()); !errors.Is(err, errInvitePendingMismatch) {
			t.Fatalf("different spki must be rejected with errInvitePendingMismatch, got %v", err)
		}
	})

	t.Run("different addr rejected (P2)", func(t *testing.T) {
		// Same (nodeID, spki) but a re-redeem with a CHANGED addr. Phase-2 finalizes
		// membership at the persisted pendingAddr, so silently keeping the stale addr
		// would admit the joiner at an address the cluster can never dial. Reject it;
		// an address change between phases requires a fresh invite.
		fsm := mint()
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.1:7001", now.UnixNano()); err != nil {
			t.Fatalf("first applyPending: %v", err)
		}
		if err := fsm.applyPending("inv-p", "node-1", s1, "10.0.0.9:7001", now.UnixNano()); !errors.Is(err, errInvitePendingMismatch) {
			t.Fatalf("different addr must be rejected with errInvitePendingMismatch, got %v", err)
		}
		// Stays bound to the first addr.
		_, _, gotAddr, ok := fsm.lookupPending("inv-p")
		if !ok || gotAddr != "10.0.0.1:7001" {
			t.Fatalf("invite must stay bound to first addr after mismatch, got %q", gotAddr)
		}
	})
}

// TestInviteFSM_PendingInvalidStates asserts applyPending rejects an absent,
// already-used, or expired invite, and lookupPending reports not-pending when
// no pending record exists.
func TestInviteFSM_PendingInvalidStates(t *testing.T) {
	now := time.Unix(4000, 0)
	s := spki(3)

	t.Run("absent invite rejected", func(t *testing.T) {
		fsm := newInviteFSM()
		if err := fsm.applyPending("never-minted", "n", s, "a", now.UnixNano()); err == nil {
			t.Fatal("applyPending on absent invite must error")
		}
		if _, _, _, ok := fsm.lookupPending("never-minted"); ok {
			t.Fatal("lookupPending on absent invite must report not-pending")
		}
	})

	t.Run("used invite rejected", func(t *testing.T) {
		fsm := newInviteFSM()
		fsm.applyMint("inv-used", testPub(t), now.Add(10*time.Minute).UnixNano())
		if err := fsm.applyConsume("inv-used", now); err != nil {
			t.Fatalf("consume: %v", err)
		}
		if err := fsm.applyPending("inv-used", "n", s, "a", now.UnixNano()); err == nil {
			t.Fatal("applyPending on used invite must error")
		}
	})

	t.Run("expired invite rejected", func(t *testing.T) {
		fsm := newInviteFSM()
		fsm.applyMint("inv-exp", testPub(t), now.Add(time.Minute).UnixNano())
		if err := fsm.applyPending("inv-exp", "n", s, "a", now.Add(2*time.Minute).UnixNano()); err == nil {
			t.Fatal("applyPending on expired invite must error")
		}
	})

	t.Run("minted-but-not-pending reports not-pending", func(t *testing.T) {
		fsm := newInviteFSM()
		fsm.applyMint("inv-fresh", testPub(t), now.Add(10*time.Minute).UnixNano())
		if _, _, _, ok := fsm.lookupPending("inv-fresh"); ok {
			t.Fatal("freshly minted (not yet pending) invite must report not-pending")
		}
	})
}

// TestInviteFSM_PendingThenConsume asserts that consuming a pending invite
// transitions pending→used (existing single-use consume semantics still hold).
func TestInviteFSM_PendingThenConsume(t *testing.T) {
	now := time.Unix(5000, 0)
	fsm := newInviteFSM()
	fsm.applyMint("inv-pc", testPub(t), now.Add(10*time.Minute).UnixNano())
	if err := fsm.applyPending("inv-pc", "node-1", spki(4), "10.0.0.9:7001", now.UnixNano()); err != nil {
		t.Fatalf("applyPending: %v", err)
	}
	if err := fsm.applyConsume("inv-pc", now); err != nil {
		t.Fatalf("consume of pending invite: %v", err)
	}
	if _, ok := fsm.lookup("inv-pc", now); ok {
		t.Fatal("consumed invite must no longer be valid")
	}
	if err := fsm.applyConsume("inv-pc", now); err == nil {
		t.Fatal("second consume must fail (already used)")
	}
}
