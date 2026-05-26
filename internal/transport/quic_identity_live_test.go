package transport

import (
	"context"
	"testing"
	"time"
)

// TestListener_HonorsLiveSnapshotAfterSwap is a regression test for the bug
// where Listen() captures the server identity snapshot once at startup.
//
// After SwapIdentity changes the server's PresentCert (and SPKI), the TLS
// listener must serve the new cert on subsequent inbound handshakes. Without
// the fix, the listener keeps serving the stale cert; a client that accepts
// ONLY the new SPKI will reject the handshake via its own VerifyPeerCertificate
// (client-side enforcement is reliable in quic-go v0.59.x).
//
// Note: server-side VerifyPeerCertificate is NOT reliably enforced by
// quic-go — the real inbound auth boundary is the acceptLoop's app-layer
// check against t.identity.Load(). The fix (GetConfigForClient) targets the
// server PresentCert staleness, not the inbound-accept SPKI check.
func TestListener_HonorsLiveSnapshotAfterSwap(t *testing.T) {
	// Phase 1: server starts with the PSK-derived identity (certA / spkiA).
	srv := MustNewQUICTransport(longPSK("server"))
	defer srv.Close()

	if err := srv.Listen(context.Background(), "127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Phase 2: generate a new server identity (certNew / spkiNew).
	// This simulates a key rotation on the server after Listen.
	certNew, spkiNew, err := GenerateNodeIdentity("cluster-x", "node-server-rotated")
	if err != nil {
		t.Fatal(err)
	}

	// Phase 3: generate a separate client identity (cliCert / cliSPKI).
	cliCert, cliSPKI, err := GenerateNodeIdentity("cluster-x", "node-client")
	if err != nil {
		t.Fatal(err)
	}

	// Phase 4: swap the server to the new identity.
	// AcceptSPKIs includes both spkiNew (server itself) and cliSPKI (client)
	// so the app-layer acceptLoop passes; the test discriminator is whether
	// the client accepts the server cert, not the server's inbound check.
	srv.SwapIdentity(NewIdentitySnapshot(
		[][32]byte{spkiNew, cliSPKI},
		certNew,
		spkiNew,
	))

	// Phase 5: build a client that presents cliCert and accepts ONLY spkiNew.
	// If the server still serves the old PSK-derived cert (spkiA ≠ spkiNew),
	// the client's VerifyPeerCertificate rejects and Connect fails.
	// If the server serves certNew (spkiNew), the client accepts and Connect
	// succeeds.
	cli := MustNewQUICTransport(longPSK("server")) // same PSK → same initial spkiA
	defer cli.Close()
	cli.SwapIdentity(NewIdentitySnapshot(
		[][32]byte{spkiNew}, // accept ONLY the rotated server SPKI
		cliCert,
		cliSPKI,
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cli.Connect(ctx, srv.LocalAddr()); err != nil {
		t.Fatalf("dial after server identity swap should succeed: %v", err)
	}
}
