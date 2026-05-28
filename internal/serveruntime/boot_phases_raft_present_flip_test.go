package serveruntime

import (
	"crypto/tls"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

func TestOnPresentFlipCallback_CallsFlipPresentOnly(t *testing.T) {
	cert := tls.Certificate{Certificate: [][]byte{{0x01, 0x02}}}
	spki := [32]byte{0xAA, 0xBB}

	var flipCert atomic.Value
	var flipSPKI atomic.Value
	var recycleCalls atomic.Int32

	st := &bootState{perNodeCert: cert, perNodeSPKI: spki}
	tr := &fakeFlipTransport{
		onFlip: func(c tls.Certificate, s [32]byte) {
			flipCert.Store(c)
			flipSPKI.Store(s)
		},
		onRecycle: func() { recycleCalls.Add(1) },
	}

	cb := buildOnPresentFlipCallback(st, tr)
	require.NotNil(t, cb)
	cb()

	gotCert := flipCert.Load().(tls.Certificate)
	gotSPKI := flipSPKI.Load().([32]byte)
	require.Equal(t, cert.Certificate, gotCert.Certificate)
	require.Equal(t, spki, gotSPKI)
	require.Equal(t, int32(0), recycleCalls.Load(), "PR-2a MUST NOT call RecycleConns (F4 user decision)")
}

func TestOnPresentFlipCallback_NilTransport(t *testing.T) {
	st := &bootState{}
	cb := buildOnPresentFlipCallback(st, nil)
	require.Nil(t, cb, "single-node path: nil transport must return nil callback")
}

func TestOnPresentFlipCallback_EmptyCertSkips(t *testing.T) {
	var flipCalled atomic.Bool
	st := &bootState{} // perNodeCert is zero value — Certificate slice is nil
	tr := &fakeFlipTransport{
		onFlip: func(c tls.Certificate, s [32]byte) { flipCalled.Store(true) },
	}

	cb := buildOnPresentFlipCallback(st, tr)
	require.NotNil(t, cb)
	cb() // must not panic and must not call FlipPresent
	require.False(t, flipCalled.Load(), "empty perNodeCert must skip FlipPresent")
}

type fakeFlipTransport struct {
	onFlip    func(tls.Certificate, [32]byte)
	onRecycle func()
}

func (f *fakeFlipTransport) FlipPresent(c tls.Certificate, s [32]byte) {
	if f.onFlip != nil {
		f.onFlip(c, s)
	}
}
func (f *fakeFlipTransport) RecycleConns() {
	if f.onRecycle != nil {
		f.onRecycle()
	}
}

var _ presentFlipTarget = (*fakeFlipTransport)(nil)

// compile-time import reference so the transport package is wired in.
var _ = transport.StreamAppliedIndexProbe
