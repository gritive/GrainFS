package transport

import (
	"context"
	"testing"
	"time"
)

// listenHTTP binds an HTTPTransport server on a loopback port and returns its addr.
func listenHTTP(t *testing.T, tr *HTTPTransport) string {
	t.Helper()
	if err := tr.Listen(context.Background(), "127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	return tr.LocalAddr()
}

// pingReady retries Ping until the Hertz server is accepting (the listener is
// bound before Run() but the serve loop starts in a goroutine).
func pingReady(t *testing.T, client *HTTPTransport, addr string) error {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = client.Ping(ctx, addr)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

func TestHTTPTransport_HandshakeRoundTrip(t *testing.T) {
	srv := MustNewHTTPTransport("cluster-psk")
	defer srv.Close()
	cli := MustNewHTTPTransport("cluster-psk")
	defer cli.Close()

	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("same-PSK ping must succeed: %v", err)
	}
}

func TestHTTPTransport_SPKIReject_ServerPinsClient(t *testing.T) {
	srv := MustNewHTTPTransport("cluster-psk")
	defer srv.Close()
	// Client presents a cert derived from a DIFFERENT PSK → server's
	// VerifyPeerCertificate (SPKI pin) rejects the handshake.
	cli := MustNewHTTPTransport("intruder-psk")
	defer cli.Close()

	addr := listenHTTP(t, srv)
	// Give the server a moment to start accepting so the failure is a handshake
	// rejection, not connection-refused.
	_ = pingReady(t, MustNewHTTPTransport("cluster-psk"), addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx, addr); err == nil {
		t.Fatal("wrong-PSK client must be rejected by the server SPKI pin")
	}
}

func TestHTTPTransport_SPKIReject_ClientPinsServer(t *testing.T) {
	// Server present cert = PSK B; client accepts only PSK A → client's
	// VerifyPeerCertificate rejects the server cert.
	srv := MustNewHTTPTransport("psk-b")
	defer srv.Close()
	cli := MustNewHTTPTransport("psk-a")
	defer cli.Close()

	addr := listenHTTP(t, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx, addr); err == nil {
		t.Fatal("client must reject a server presenting an unaccepted SPKI")
	}
}

// TestHTTPTransport_RotationFreshRead proves the server reads the LIVE snapshot
// per handshake: after SetDropped removes the base SPKI from the accept-set, a new
// client dial is rejected. RED if Listen captured a static config.
func TestHTTPTransport_RotationFreshRead(t *testing.T) {
	srv := MustNewHTTPTransport("cluster-psk")
	defer srv.Close()
	cli := MustNewHTTPTransport("cluster-psk")
	defer cli.Close()

	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("baseline same-PSK ping must succeed: %v", err)
	}

	// Drop the cluster key: the server must stop accepting the base SPKI on NEW
	// handshakes (registry is empty → accept nothing). Close the pooled keep-alive
	// conn so the next ping is a FRESH dial (a reused conn handshaked under the old
	// identity would mask the fresh-read).
	srv.SetDropped()
	if cli.client != nil {
		cli.client.CloseIdleConnections()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx, addr); err == nil {
		t.Fatal("after SetDropped a fresh dial must be rejected (server reads live snapshot)")
	}
}

// TestHTTPTransport_RotationFreshRead_Client proves the CLIENT dialer
// (httpFreshDialer) reads the LIVE snapshot per dial: after the client SetDropped
// empties its accept-set, a fresh dial rejects the server's still-valid cert. The
// server does NOT drop, so the rejection is attributable purely to the client-side
// verify — distinct from the initial-dial rejection in SPKIReject_ClientPinsServer
// (which a static captured config would also produce). RED if httpFreshDialer
// captured a static config instead of calling build() per dial.
func TestHTTPTransport_RotationFreshRead_Client(t *testing.T) {
	srv := MustNewHTTPTransport("cluster-psk")
	defer srv.Close()
	cli := MustNewHTTPTransport("cluster-psk")
	defer cli.Close()

	addr := listenHTTP(t, srv)
	if err := pingReady(t, cli, addr); err != nil {
		t.Fatalf("baseline same-PSK ping must succeed: %v", err)
	}

	// Drop the cluster key on the CLIENT only: its accept-set becomes registry-only
	// (empty) → it must reject the server's still-valid cert on a FRESH dial.
	cli.SetDropped()
	if cli.client != nil {
		cli.client.CloseIdleConnections()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx, addr); err == nil {
		t.Fatal("after client SetDropped a fresh dial must reject the server cert (client dialer reads live snapshot)")
	}
}
