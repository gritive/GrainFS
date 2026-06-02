package transport

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// S5 acceptance criterion 2 (deferred from S2b-2): capability-exchange parity.
//
// AUDIT RESULT — no production code needed. The QUIC mux handshake's capability
// exchange (quic.go doCapabilityExchangeDial / handleCapabilityExchange) puts
// exactly two bytes on the wire: []byte{ceVersion, ceFeatures}. ceFeatures is
// always 0x00 and ceFeaturesSupportedMask is 0x00, so NO feature is negotiated —
// the feature byte is a reserved, always-zero placeholder. The ONLY load-bearing
// field is the version byte (ceVersion = 0x01).
//
// The TCP mux carrier carries that version in the ALPN suffix (tcpMuxALPN =
// "grainfs-tcp-mux-v1"). A version mismatch fails the TLS handshake itself
// (no_application_protocol) — earlier and stronger than QUIC's post-handshake CE
// rejection — and dialMux additionally double-checks NegotiatedProtocol ==
// tcpMuxALPN. So version parity holds and no other capability is lost.
//
// This test gives the audit teeth: a mux dialer offering ONLY a bumped mux ALPN
// version cannot complete the handshake against a server advertising v1.
func TestTCPMux_VersionMismatchRejectedViaALPN(t *testing.T) {
	const psk = "mux-version-parity-key"
	srv := startTCP(t, psk)

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	// A future mux protocol version would bump the ALPN to "grainfs-tcp-mux-v2".
	// The server advertises [tcpMuxALPN ("...-v1"), tcpALPN]; with no overlap the
	// TLS handshake must fail — version is enforced at the ALPN.
	wrongTLS := cli.muxClientTLS.Clone()
	wrongTLS.NextProtos = []string{"grainfs-tcp-mux-v2"}
	require.NotEqual(t, tcpMuxALPN, wrongTLS.NextProtos[0]) // sanity: it really is a mismatch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", srv.LocalAddr())
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()

	conn := tls.Client(raw, wrongTLS)
	err = conn.HandshakeContext(ctx)
	require.Error(t, err, "a mux dial offering a mismatched ALPN version must fail the handshake (no_application_protocol)")
}

// TestTCPMux_VersionMatchHandshakeSucceeds is the positive control: the correct
// mux ALPN (the real cluster mux dialer config) negotiates tcpMuxALPN cleanly.
func TestTCPMux_VersionMatchHandshakeSucceeds(t *testing.T) {
	const psk = "mux-version-match-key"
	srv := startTCP(t, psk)

	cli := MustNewTCPTransport(psk)
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", srv.LocalAddr())
	require.NoError(t, err)
	defer func() { _ = raw.Close() }()

	conn := tls.Client(raw, cli.muxClientTLS.Clone())
	require.NoError(t, conn.HandshakeContext(ctx))
	require.Equal(t, tcpMuxALPN, conn.ConnectionState().NegotiatedProtocol)
	_ = conn.SetDeadline(time.Now().Add(time.Second))
}
