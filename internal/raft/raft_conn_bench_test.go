package raft

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"io"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
)

func benchmarkRaftTLSConfig(b testing.TB, server bool) *tls.Config {
	b.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		b.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "raft-conn-bench"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		b.Fatal(err)
	}
	cert := tls.Certificate{Certificate: [][]byte{certDER}, PrivateKey: priv}
	conf := &tls.Config{
		NextProtos:         []string{"grainfs-raft-conn-bench"},
		InsecureSkipVerify: true,
	}
	if server {
		conf.Certificates = []tls.Certificate{cert}
	}
	return conf
}

func benchmarkRaftQUICConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout: 5 * time.Second,
		MaxIdleTimeout:       30 * time.Second,
		MaxIncomingStreams:   16,
	}
}

func benchmarkRaftConnPair(b *testing.B) (context.Context, context.CancelFunc, *quic.Listener, *quic.Conn, <-chan *quic.Conn) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	listener, err := quic.ListenAddr("127.0.0.1:0", benchmarkRaftTLSConfig(b, true), benchmarkRaftQUICConfig())
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	accepted := make(chan *quic.Conn, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err == nil {
			accepted <- conn
		}
		close(accepted)
	}()

	clientConn, err := quic.DialAddr(ctx, listener.Addr().String(), benchmarkRaftTLSConfig(b, false), benchmarkRaftQUICConfig())
	if err != nil {
		cancel()
		_ = listener.Close()
		b.Fatal(err)
	}

	b.Cleanup(func() {
		_ = clientConn.CloseWithError(0, "benchmark done")
		_ = listener.Close()
		cancel()
	})

	return ctx, cancel, listener, clientConn, accepted
}

func writeBenchmarkFrame(w io.Writer, op uint8, corrID uint64, payload []byte) error {
	var hdr [frameHeaderSize]byte
	frameLen := uint32(frameHeaderSize - 4 + len(payload))
	binary.BigEndian.PutUint32(hdr[0:4], frameLen)
	hdr[4] = op
	hdr[5] = 0
	binary.BigEndian.PutUint64(hdr[6:14], corrID)
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

func BenchmarkRaftConnSendFrame(b *testing.B) {
	ctx, _, _, clientConn, accepted := benchmarkRaftConnPair(b)
	clientStream, err := clientConn.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}

	serverConn := <-accepted
	if serverConn == nil {
		b.Fatal("server connection was not accepted")
	}
	b.Cleanup(func() { _ = serverConn.CloseWithError(0, "benchmark done") })
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		serverStream, err := serverConn.AcceptStream(ctx)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, serverStream)
	}()

	rc := &RaftConn{closeChan: make(chan struct{})}
	stream := &RaftStream{parent: rc, stream: clientStream}
	payload := []byte("raft-wire-payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := rc.sendFrame(stream, opNotify, uint64(i+1), payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = clientStream.Close()
	<-drainDone
}

func BenchmarkRaftConnReadFrame(b *testing.B) {
	ctx, _, _, clientConn, accepted := benchmarkRaftConnPair(b)
	clientStream, err := clientConn.OpenStreamSync(ctx)
	if err != nil {
		b.Fatal(err)
	}

	serverConn := <-accepted
	if serverConn == nil {
		b.Fatal("server connection was not accepted")
	}
	b.Cleanup(func() { _ = serverConn.CloseWithError(0, "benchmark done") })
	if err := writeBenchmarkFrame(clientStream, opStreamInit, 0, nil); err != nil {
		b.Fatal(err)
	}
	serverStream, err := serverConn.AcceptStream(ctx)
	if err != nil {
		b.Fatal(err)
	}

	delivered := make(chan struct{}, 1)
	var seen atomic.Uint64
	rc := &RaftConn{
		closeChan:  make(chan struct{}),
		handlerSem: make(chan struct{}, 1),
		notifyHandler: func(payload []byte) {
			seen.Add(1)
			delivered <- struct{}{}
		},
	}
	stream := &RaftStream{parent: rc, stream: serverStream}
	go stream.readLoop()

	payload := []byte("raft-wire-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writeBenchmarkFrame(clientStream, opNotify, uint64(i+1), payload); err != nil {
			b.Fatal(err)
		}
		<-delivered
	}
	b.StopTimer()
	if got := seen.Load(); got != uint64(b.N) {
		b.Fatalf("delivered %d frames, want %d", got, b.N)
	}
	_ = clientStream.Close()
	rc.Close()
}
