package encrypt

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"testing"
	"time"
)

// helper: build a deterministic 32B KEK for tests.
func testKEK(b byte) []byte {
	k := make([]byte, KEKSize)
	for i := range k {
		k[i] = b
	}
	return k
}

func TestHandshake_Succeeds_SameKEK(t *testing.T) {
	kek := testKEK(0xA1)
	peer := NewHandshakeVerifier(kek)

	nonce, err := peer.IssueChallenge()
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}
	if len(nonce) != 32 {
		t.Fatalf("nonce length = %d, want 32", len(nonce))
	}

	resp := ComputeHandshakeResponse(kek, nonce)
	if len(resp) != sha256.Size {
		t.Fatalf("response length = %d, want %d", len(resp), sha256.Size)
	}

	// Sanity: matches an independent HMAC computation.
	mac := hmac.New(sha256.New, kek)
	mac.Write(nonce)
	want := mac.Sum(nil)
	if !bytes.Equal(resp, want) {
		t.Fatalf("ComputeHandshakeResponse mismatch with reference HMAC-SHA256")
	}

	if err := peer.VerifyResponse(nonce, resp); err != nil {
		t.Fatalf("VerifyResponse: %v", err)
	}
}

func TestHandshake_Fails_WrongKEK(t *testing.T) {
	peerKEK := testKEK(0xA1)
	joinerKEK := testKEK(0xB2)
	peer := NewHandshakeVerifier(peerKEK)

	nonce, err := peer.IssueChallenge()
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}

	resp := ComputeHandshakeResponse(joinerKEK, nonce)
	err = peer.VerifyResponse(nonce, resp)
	if !errors.Is(err, ErrHandshakeMismatch) {
		t.Fatalf("VerifyResponse err = %v, want ErrHandshakeMismatch", err)
	}

	// Spec: mismatch burns the nonce. A subsequent correct response must also
	// be rejected (as replay) to block guess-and-retry.
	correct := ComputeHandshakeResponse(peerKEK, nonce)
	err = peer.VerifyResponse(nonce, correct)
	if !errors.Is(err, ErrHandshakeReplay) {
		t.Fatalf("burned nonce retry err = %v, want ErrHandshakeReplay", err)
	}
}

func TestHandshake_NonceReplayRejected(t *testing.T) {
	kek := testKEK(0xA1)
	peer := NewHandshakeVerifier(kek)

	nonce, err := peer.IssueChallenge()
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}
	resp := ComputeHandshakeResponse(kek, nonce)

	if err := peer.VerifyResponse(nonce, resp); err != nil {
		t.Fatalf("first VerifyResponse: %v", err)
	}
	// Second call with the same nonce must be rejected as replay.
	err = peer.VerifyResponse(nonce, resp)
	if !errors.Is(err, ErrHandshakeReplay) {
		t.Fatalf("replay err = %v, want ErrHandshakeReplay", err)
	}

	// Unknown nonce (never issued) is also a replay-class rejection.
	unknown := make([]byte, 32)
	for i := range unknown {
		unknown[i] = 0xCC
	}
	err = peer.VerifyResponse(unknown, ComputeHandshakeResponse(kek, unknown))
	if !errors.Is(err, ErrHandshakeReplay) {
		t.Fatalf("unknown-nonce err = %v, want ErrHandshakeReplay", err)
	}
}

func TestHandshake_NonceTTL(t *testing.T) {
	kek := testKEK(0xA1)
	peer := NewHandshakeVerifierWithTTL(kek, 20*time.Millisecond)

	nonce, err := peer.IssueChallenge()
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}

	// Wait past TTL.
	time.Sleep(60 * time.Millisecond)

	resp := ComputeHandshakeResponse(kek, nonce)
	err = peer.VerifyResponse(nonce, resp)
	if !errors.Is(err, ErrHandshakeExpired) {
		t.Fatalf("VerifyResponse err = %v, want ErrHandshakeExpired", err)
	}

	// Expired nonce was cleared; verifying again gets replay (not issued).
	err = peer.VerifyResponse(nonce, resp)
	if !errors.Is(err, ErrHandshakeReplay) {
		t.Fatalf("post-expiry retry err = %v, want ErrHandshakeReplay", err)
	}
}
