package jwt

import (
	"testing"
	"time"
)

func TestMintVerify_RoundTrip(t *testing.T) {
	keys := NewKeySet()
	kid, _ := keys.GenerateCurrent()
	tok, err := keys.Mint(Claims{Sub: "sa-1", Warehouse: "analytics", TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	c, err := keys.Verify(tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if c.Sub != "sa-1" || c.Warehouse != "analytics" {
		t.Fatalf("claims: %+v", c)
	}
	if c.Kid != kid {
		t.Fatalf("kid: %s vs %s", c.Kid, kid)
	}
}

func TestVerify_RejectsAlgNone(t *testing.T) {
	keys := NewKeySet()
	keys.GenerateCurrent()
	hdr := base64URLEncode([]byte(`{"alg":"none","typ":"JWT"}`))
	body := base64URLEncode([]byte(`{"sub":"sa-1"}`))
	if _, err := keys.Verify(hdr + "." + body + "."); err == nil {
		t.Fatal("alg=none must be rejected unconditionally")
	}
}

func TestVerify_RejectsRS256(t *testing.T) {
	keys := NewKeySet()
	keys.GenerateCurrent()
	hdr := base64URLEncode([]byte(`{"alg":"RS256","typ":"JWT","kid":"x"}`))
	body := base64URLEncode([]byte(`{"sub":"sa-1"}`))
	sig := base64URLEncode([]byte("fake-sig"))
	if _, err := keys.Verify(hdr + "." + body + "." + sig); err == nil {
		t.Fatal("alg=RS256 must be rejected (HS256 only)")
	}
}

func TestKidRotation_PreviousAcceptedDuringWindow(t *testing.T) {
	keys := NewKeySet()
	prevKid, _ := keys.GenerateCurrent()
	tokPrev, _ := keys.Mint(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour})
	currKid, _ := keys.Rotate()
	if prevKid == currKid {
		t.Fatal("rotation did not change kid")
	}
	if _, err := keys.Verify(tokPrev); err != nil {
		t.Fatalf("token minted by prev kid must verify during dual-window: %v", err)
	}
}

func TestKidRotation_PrunePreviousRejectsOldToken(t *testing.T) {
	keys := NewKeySet()
	keys.GenerateCurrent()
	tokPrev, _ := keys.Mint(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour})
	keys.Rotate()
	if err := keys.Prune(true); err != nil {
		t.Fatalf("prune safe: %v", err)
	}
	if _, err := keys.Verify(tokPrev); err == nil {
		t.Fatal("post-prune, previous-kid token must be rejected")
	}
}

func TestPrune_RefusedWhenTokensMayBeLive(t *testing.T) {
	keys := NewKeySet()
	keys.GenerateCurrent()
	keys.Mint(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour})
	keys.Rotate()
	if err := keys.Prune(false); err == nil {
		t.Fatal("prune must refuse when previous-signed tokens may still be live (F#1)")
	}
}

func TestVerify_ClockSkewBoundary(t *testing.T) {
	keys := NewKeySet()
	keys.GenerateCurrent()
	tok, _ := keys.MintAt(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour}, time.Now().Add(30*time.Second))
	if _, err := keys.Verify(tok); err != nil {
		t.Fatalf("iat+30s must be accepted: %v", err)
	}
	tok2, _ := keys.MintAt(Claims{Sub: "sa-1", Warehouse: "w", TTL: time.Hour}, time.Now().Add(31*time.Second))
	if _, err := keys.Verify(tok2); err == nil {
		t.Fatal("iat+31s must be rejected (F#7)")
	}
}
