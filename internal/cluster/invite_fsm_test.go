package cluster

import (
	"crypto/ed25519"
	"crypto/rand"
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
