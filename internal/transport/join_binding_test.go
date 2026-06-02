package transport

import (
	"context"
	"crypto/tls"
	"io"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestDialJoin_ReturnsBindMatchingHandler(t *testing.T) {
	srvCert, srvSPKI, err := GenerateNodeIdentity("cid", "server")
	if err != nil {
		t.Fatalf("server identity: %v", err)
	}
	gotBind := make(chan []byte, 1)
	handler := func(ctx context.Context, peerSPKI [32]byte, bind []byte, stream io.ReadWriteCloser) {
		gotBind <- append([]byte(nil), bind...)
		_ = stream.Close()
	}
	ln, err := NewJoinListener("127.0.0.1:0", srvCert, handler)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	defer ln.Close()

	cliCert, _, err := GenerateNodeIdentity("cid", "client")
	if err != nil {
		t.Fatalf("client identity: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, cliBind, closeConn, err := DialJoin(ctx, ln.Addr(), srvSPKI, cliCert)
	if err != nil {
		t.Fatalf("DialJoin: %v", err)
	}
	defer func() { _ = closeConn() }()
	_ = stream.Close()
	_, _ = io.Copy(io.Discard, stream)

	select {
	case srvBind := <-gotBind:
		if len(cliBind) != 32 {
			t.Fatalf("client bind len=%d want 32", len(cliBind))
		}
		if string(cliBind) != string(srvBind) {
			t.Fatalf("bind mismatch client %x server %x", cliBind, srvBind)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler not invoked")
	}
}

func TestJoinListener_SingleRequestPerConnection(t *testing.T) {
	// Genuinely exercise the single-AcceptStream/no-loop invariant: open TWO
	// streams on ONE join connection and prove the handler fires exactly once.
	// DialJoin only returns the stream (not the conn), so dial raw here.
	srvCert, _, err := GenerateNodeIdentity("cid", "server")
	if err != nil {
		t.Fatalf("server identity: %v", err)
	}
	calls := make(chan struct{}, 8)
	handler := func(ctx context.Context, peerSPKI [32]byte, bind []byte, stream io.ReadWriteCloser) {
		calls <- struct{}{}
		_ = stream.Close()
	}
	ln, err := NewJoinListener("127.0.0.1:0", srvCert, handler)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	defer ln.Close()
	cliCert, _, err := GenerateNodeIdentity("cid", "client")
	if err != nil {
		t.Fatalf("client identity: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliTLS := &tls.Config{
		Certificates:       []tls.Certificate{cliCert},
		NextProtos:         []string{JoinALPN},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}
	conn, err := quic.DialAddr(ctx, ln.Addr(), cliTLS, defaultQUICConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseWithError(0, "test done") }()

	// First stream: the listener's single AcceptStream returns this one and the
	// handler must fire exactly once. Write a byte so AcceptStream unblocks.
	s1, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream 1: %v", err)
	}
	if _, err := s1.Write([]byte{0}); err != nil {
		t.Fatalf("write stream 1: %v", err)
	}
	_ = s1.Close()

	// Wait for the first (and only legitimate) handler call.
	select {
	case <-calls:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler call missing")
	}

	// Second stream on the SAME conn: with no accept loop, the listener never
	// accepts it, so the handler must NOT fire again.
	s2, err := conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream 2: %v", err)
	}
	if _, err := s2.Write([]byte{1}); err != nil {
		t.Fatalf("write stream 2: %v", err)
	}
	_ = s2.Close()

	select {
	case <-calls:
		t.Fatal("handler fired a second time on same connection")
	case <-time.After(500 * time.Millisecond):
	}
}
