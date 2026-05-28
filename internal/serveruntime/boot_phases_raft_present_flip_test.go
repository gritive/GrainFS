package serveruntime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"fmt"
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

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, nil)
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
	cb := buildOnPresentFlipCallbackWithRegistrar(st, nil, nil)
	require.Nil(t, cb, "single-node path: nil transport must return nil callback")
}

func TestOnPresentFlipCallback_EmptyCertSkips(t *testing.T) {
	var flipCalled atomic.Bool
	st := &bootState{} // perNodeCert is zero value — Certificate slice is nil
	tr := &fakeFlipTransport{
		onFlip: func(c tls.Certificate, s [32]byte) { flipCalled.Store(true) },
	}

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, nil)
	require.NotNil(t, cb)
	cb() // must not panic and must not call FlipPresent
	require.False(t, flipCalled.Load(), "empty perNodeCert must skip FlipPresent")
}

func TestBuildOnPresentFlipCallback_ProposesPresentsPerNode(t *testing.T) {
	cert := tls.Certificate{Certificate: [][]byte{{0x01}}}
	spki := [32]byte{0xAB}
	st := &bootState{
		nodeID:      "node-A",
		raftAddr:    "127.0.0.1:4001",
		perNodeCert: cert,
		perNodeSPKI: spki,
	}

	var flipCalled atomic.Bool
	tr := &fakeFlipTransport{
		onFlip: func(tls.Certificate, [32]byte) {
			flipCalled.Store(true)
		},
	}
	reg := &recordingPresentRegistrar{}

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, reg)
	require.NotNil(t, cb)
	cb()

	require.True(t, flipCalled.Load(), "FlipPresent must be called")
	require.True(t, reg.called.Load(), "ProposeRegisterMember must be called")
	require.Equal(t, "node-A", reg.nodeID)
	require.Equal(t, spki, reg.spki)
	require.Equal(t, "127.0.0.1:4001", reg.addr)
	require.True(t, reg.presentsPerNode, "presentsPerNode must be true")
}

func TestBuildOnPresentFlipCallback_RegistrarErrorNonFatal(t *testing.T) {
	st := &bootState{
		nodeID:      "node-A",
		raftAddr:    "127.0.0.1:4001",
		perNodeCert: tls.Certificate{Certificate: [][]byte{{0x01}}},
		perNodeSPKI: [32]byte{0xAB},
	}
	tr := &fakeFlipTransport{}
	reg := &recordingPresentRegistrar{err: fmt.Errorf("leader unavailable")}

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, reg)
	require.NotNil(t, cb)
	require.NotPanics(t, cb)
	require.True(t, reg.called.Load(), "non-fatal errors should still prove the propose was attempted")
}

func TestOnClusterKeyDropped_CallsSetDroppedAndRecycle(t *testing.T) {
	tr := &fakeDropTransport{}
	cb := buildOnClusterKeyDroppedCallback(tr)
	require.NotNil(t, cb)

	cb()

	require.True(t, tr.droppedCalled.Load(), "SetDropped must be called")
	require.True(t, tr.recycleCalled.Load(), "RecycleConns must be called")
}

func TestApplyPostDropInviteJoinIdentity_FlipsAndDrops(t *testing.T) {
	dir := t.TempDir()
	encKey := []byte("0123456789abcdef0123456789abcdef")
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, encKey, tls.Certificate{PrivateKey: priv}))

	state := &bootState{
		cfg: Config{
			DataDir:          dir,
			RawEncryptionKey: encKey,
		},
		inviteJoin: &inviteJoinState{clusterKeyDropped: true},
	}
	tr := &fakeDropTransport{}

	require.NoError(t, applyPostDropInviteJoinIdentity(state, tr))
	require.True(t, tr.flipCalled.Load(), "FlipPresent must be called before Listen")
	require.True(t, tr.droppedCalled.Load(), "SetDropped must be called before Listen")
	require.NotNil(t, state.perNodeCert.Certificate)
	require.NotEqual(t, [32]byte{}, state.perNodeSPKI)
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

type recordingPresentRegistrar struct {
	called          atomic.Bool
	nodeID          string
	spki            [32]byte
	addr            string
	presentsPerNode bool
	err             error
}

func (r *recordingPresentRegistrar) ProposeRegisterMember(_ context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool) error {
	r.called.Store(true)
	r.nodeID = nodeID
	r.spki = spki
	r.addr = addr
	r.presentsPerNode = presentsPerNode
	return r.err
}

type fakeDropTransport struct {
	fakeFlipTransport
	flipCalled    atomic.Bool
	droppedCalled atomic.Bool
	recycleCalled atomic.Bool
}

func (f *fakeDropTransport) FlipPresent(c tls.Certificate, s [32]byte) {
	f.flipCalled.Store(true)
	f.fakeFlipTransport.FlipPresent(c, s)
}

func (f *fakeDropTransport) SetDropped() {
	f.droppedCalled.Store(true)
}

func (f *fakeDropTransport) RecycleConns() {
	f.recycleCalled.Store(true)
	f.fakeFlipTransport.RecycleConns()
}

var _ presentFlipTarget = (*fakeFlipTransport)(nil)

// compile-time import reference so the transport package is wired in.
var _ = transport.StreamAppliedIndexProbe
