package encrypt

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func newTestHandshakeFixtures(t *testing.T) (*KEKStore, []byte) {
	t.Helper()
	store := NewKEKStore()
	if err := store.Add(0, bytes.Repeat([]byte{0x55}, KEKSize)); err != nil {
		t.Fatalf("seed v0: %v", err)
	}
	clusterID := bytes.Repeat([]byte{0xAB}, 16)
	return store, clusterID
}

func makeTranscript(clusterID, nonce []byte) JoinTranscript {
	return JoinTranscript{
		ClusterID:           clusterID,
		Nonce:               nonce,
		NodeID:              "node-x",
		Address:             "10.0.0.5:7000",
		JoinerVersion:       0,
		LeaderActiveVersion: 0,
	}
}

func TestHandshakeVerifier_TranscriptBindingRoundTrip(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, err := v.IssueChallenge(0)
	if err != nil {
		t.Fatalf("IssueChallenge: %v", err)
	}
	transcript := makeTranscript(clusterID, nonce)
	mac, err := ComputeHandshakeResponse(store, 0, transcript)
	if err != nil {
		t.Fatalf("ComputeHandshakeResponse: %v", err)
	}
	if err := v.VerifyResponse(0, transcript, mac); err != nil {
		t.Errorf("VerifyResponse: %v", err)
	}
}

func TestHandshakeVerifier_RejectsTamperedNodeID(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	good := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, good)
	tampered := good
	tampered.NodeID = "evil-node"
	if err := v.VerifyResponse(0, tampered, mac); err == nil {
		t.Errorf("VerifyResponse accepted tampered node_id")
	}
}

func TestHandshakeVerifier_RejectsTamperedAddress(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	good := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, good)
	tampered := good
	tampered.Address = "10.0.0.99:7000"
	if err := v.VerifyResponse(0, tampered, mac); err == nil {
		t.Errorf("VerifyResponse accepted tampered address")
	}
}

func TestHandshakeVerifier_RejectsTamperedVersions(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	good := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, good)
	tampered := good
	tampered.LeaderActiveVersion = 99
	if err := v.VerifyResponse(0, tampered, mac); err == nil {
		t.Errorf("VerifyResponse accepted tampered leader_active_version")
	}
}

func TestHandshakeVerifier_RejectsForeignClusterID(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	transcript := makeTranscript(bytes.Repeat([]byte{0xFF}, 16), nonce) // wrong cluster
	mac, _ := ComputeHandshakeResponse(store, 0, transcript)
	err := v.VerifyResponse(0, transcript, mac)
	if !errors.Is(err, ErrHandshakeBadCluster) {
		t.Errorf("expected ErrHandshakeBadCluster, got %v", err)
	}
}

func TestHandshakeVerifier_RejectsUnknownVersionOnIssue(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	if _, err := v.IssueChallenge(99); !errors.Is(err, ErrHandshakeUnknownVer) {
		t.Errorf("expected ErrHandshakeUnknownVer, got %v", err)
	}
}

func TestHandshakeVerifier_RejectsReplay(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	transcript := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, transcript)
	if err := v.VerifyResponse(0, transcript, mac); err != nil {
		t.Fatalf("first verify: %v", err)
	}
	if err := v.VerifyResponse(0, transcript, mac); !errors.Is(err, ErrHandshakeReplay) {
		t.Errorf("expected ErrHandshakeReplay on second use, got %v", err)
	}
}

func TestHandshakeVerifier_RejectsExpired(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifierWithTTL(store, clusterID, 1*time.Millisecond)
	nonce, _ := v.IssueChallenge(0)
	transcript := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, transcript)
	time.Sleep(50 * time.Millisecond)
	if err := v.VerifyResponse(0, transcript, mac); !errors.Is(err, ErrHandshakeExpired) {
		t.Errorf("expected ErrHandshakeExpired, got %v", err)
	}
}

func TestHandshakeVerifier_MismatchBurnsNonce(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	transcript := makeTranscript(clusterID, nonce)
	badMAC := make([]byte, 32) // all zeros
	if err := v.VerifyResponse(0, transcript, badMAC); !errors.Is(err, ErrHandshakeMismatch) {
		t.Fatalf("expected ErrHandshakeMismatch on bad MAC, got %v", err)
	}
	// Same nonce should now be unusable.
	goodMAC, _ := ComputeHandshakeResponse(store, 0, transcript)
	if err := v.VerifyResponse(0, transcript, goodMAC); !errors.Is(err, ErrHandshakeReplay) {
		t.Errorf("expected ErrHandshakeReplay after mismatch burn, got %v", err)
	}
}

func TestHandshakeVerifier_RejectsTamperedJoinerVersion(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	v := NewHandshakeVerifier(store, clusterID)
	nonce, _ := v.IssueChallenge(0)
	good := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, good)
	tampered := good
	tampered.JoinerVersion = 99
	if err := v.VerifyResponse(0, tampered, mac); err == nil {
		t.Errorf("VerifyResponse accepted tampered joiner_version")
	}
}

func TestHandshakeVerifier_UsedMapGCsExpiredEntries(t *testing.T) {
	store, clusterID := newTestHandshakeFixtures(t)
	// Short TTL so entries expire quickly in test.
	v := NewHandshakeVerifierWithTTL(store, clusterID, 1*time.Millisecond)

	// Burn a few nonces via successful verifies.
	for i := 0; i < 5; i++ {
		nonce, err := v.IssueChallenge(0)
		if err != nil {
			t.Fatalf("IssueChallenge %d: %v", i, err)
		}
		transcript := makeTranscript(clusterID, nonce)
		mac, _ := ComputeHandshakeResponse(store, 0, transcript)
		if err := v.VerifyResponse(0, transcript, mac); err != nil {
			t.Fatalf("VerifyResponse %d: %v", i, err)
		}
	}

	v.mu.Lock()
	beforeGC := len(v.used)
	v.mu.Unlock()
	if beforeGC < 5 {
		t.Fatalf("expected at least 5 used entries before GC, got %d", beforeGC)
	}

	// Wait past TTL, then trigger GC via IssueChallenge.
	time.Sleep(20 * time.Millisecond)
	_, _ = v.IssueChallenge(0)

	v.mu.Lock()
	afterGC := len(v.used)
	v.mu.Unlock()
	if afterGC != 0 {
		t.Errorf("expected used map empty after GC past TTL, got %d", afterGC)
	}
}

func TestHandshakeVerifier_VersionMismatchAtVerify(t *testing.T) {
	store := NewKEKStore()
	_ = store.Add(0, bytes.Repeat([]byte{0x10}, KEKSize))
	_ = store.Add(1, bytes.Repeat([]byte{0x20}, KEKSize))
	clusterID := bytes.Repeat([]byte{0x33}, 16)
	v := NewHandshakeVerifier(store, clusterID)

	// Issue under v0 but Verify claims v1 — must mismatch (not replay).
	nonce, _ := v.IssueChallenge(0)
	transcript := makeTranscript(clusterID, nonce)
	mac, _ := ComputeHandshakeResponse(store, 0, transcript)
	if err := v.VerifyResponse(1, transcript, mac); !errors.Is(err, ErrHandshakeMismatch) {
		t.Errorf("expected ErrHandshakeMismatch on version cross, got %v", err)
	}
}
