package transport

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

// TestQUICExporterSymmetry proves client and server derive the same RFC 5705
// exporter for one QUIC/TLS session — the primitive channel binding relies on.
func TestQUICExporterSymmetry(t *testing.T) {
	srvCert, _, err := GenerateNodeIdentity("cid", "server")
	if err != nil {
		t.Fatalf("server identity: %v", err)
	}
	srvTLS := &tls.Config{
		Certificates: []tls.Certificate{srvCert},
		NextProtos:   []string{JoinALPN},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	ln, err := quic.ListenAddr("127.0.0.1:0", srvTLS, defaultQUICConfig())
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type result struct {
		bind []byte
		err  error
	}
	srvCh := make(chan result, 1)
	go func() {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			srvCh <- result{err: err}
			return
		}
		tlsState := conn.ConnectionState().TLS
		b, err := tlsState.ExportKeyingMaterial("grainfs-join-binding", nil, 32)
		srvCh <- result{bind: b, err: err}
	}()

	cliCert, _, err := GenerateNodeIdentity("cid", "client")
	if err != nil {
		t.Fatalf("client identity: %v", err)
	}
	cliTLS := &tls.Config{
		Certificates:       []tls.Certificate{cliCert},
		NextProtos:         []string{JoinALPN},
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := quic.DialAddr(ctx, ln.Addr().String(), cliTLS, defaultQUICConfig())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cliTLSState := conn.ConnectionState().TLS
	cliBind, err := cliTLSState.ExportKeyingMaterial("grainfs-join-binding", nil, 32)
	if err != nil {
		t.Fatalf("client ExportKeyingMaterial: %v", err)
	}

	srv := <-srvCh
	if srv.err != nil {
		t.Fatalf("server ExportKeyingMaterial: %v", srv.err)
	}
	if len(cliBind) != 32 {
		t.Fatalf("client bind len = %d, want 32", len(cliBind))
	}
	if string(cliBind) != string(srv.bind) {
		t.Fatalf("exporter mismatch: client %x != server %x", cliBind, srv.bind)
	}
	allZero := true
	for _, b := range cliBind {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Fatal("exporter is all-zero; ekm not populated")
	}
}
