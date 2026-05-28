package serveruntime

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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
		nodeID:           "node-A",
		raftAddr:         "127.0.0.1:4001",
		perNodeCert:      cert,
		perNodeSPKI:      spki,
		perNodeKeyKEKGen: 7,
	}

	var flipCalled atomic.Bool
	tr := &fakeFlipTransport{
		onFlip: func(tls.Certificate, [32]byte) {
			flipCalled.Store(true)
		},
	}
	reg := newRecordingPresentRegistrar(nil)

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, reg)
	require.NotNil(t, cb)
	cb()

	require.True(t, flipCalled.Load(), "FlipPresent must be called")
	reg.wait(t)
	require.Equal(t, "node-A", reg.nodeID)
	require.Equal(t, spki, reg.spki)
	require.Equal(t, "127.0.0.1:4001", reg.addr)
	require.True(t, reg.presentsPerNode, "presentsPerNode must be true")
	require.Equal(t, uint32(7), reg.nodeKeyKEKGen)
}

func TestBuildOnPresentFlipCallback_RegistrarErrorNonFatal(t *testing.T) {
	st := &bootState{
		nodeID:           "node-A",
		raftAddr:         "127.0.0.1:4001",
		perNodeCert:      tls.Certificate{Certificate: [][]byte{{0x01}}},
		perNodeSPKI:      [32]byte{0xAB},
		perNodeKeyKEKGen: 7,
	}
	tr := &fakeFlipTransport{}
	reg := newRecordingPresentRegistrar(fmt.Errorf("leader unavailable"))

	cb := buildOnPresentFlipCallbackWithRegistrar(st, tr, reg)
	require.NotNil(t, cb)
	require.NotPanics(t, cb)
	reg.wait(t)
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
	kek := bytes.Repeat([]byte{0x21}, 32)
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, kek, tls.Certificate{PrivateKey: priv}))
	keysDir := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keysDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(keysDir, "3.key"), kek, 0o600))

	state := &bootState{
		cfg: Config{DataDir: dir},
		inviteJoin: &inviteJoinState{
			clusterKeyDropped: true,
			nodeKeyKEKGen:     3,
		},
	}
	tr := &fakeDropTransport{}

	require.NoError(t, applyPostDropInviteJoinIdentity(state, tr))
	require.True(t, tr.flipCalled.Load(), "FlipPresent must be called before Listen")
	require.True(t, tr.droppedCalled.Load(), "SetDropped must be called before Listen")
	require.NotNil(t, state.perNodeCert.Certificate)
	require.NotEqual(t, [32]byte{}, state.perNodeSPKI)
}

func TestApplyPostDropInviteJoinIdentity_LoadsKEKSealedNodeKeyWithoutStaticKey(t *testing.T) {
	dir := t.TempDir()
	kek := bytes.Repeat([]byte{0x42}, 32)
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, kek, tls.Certificate{PrivateKey: priv}))
	keysDir := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keysDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(keysDir, "7.key"), kek, 0o600))

	state := &bootState{
		cfg: Config{DataDir: dir},
		inviteJoin: &inviteJoinState{
			clusterKeyDropped: true,
			nodeKeyKEKGen:     7,
		},
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
	done            chan struct{}
	nodeID          string
	spki            [32]byte
	addr            string
	presentsPerNode bool
	nodeKeyKEKGen   uint32
	err             error
}

func newRecordingPresentRegistrar(err error) *recordingPresentRegistrar {
	return &recordingPresentRegistrar{done: make(chan struct{}), err: err}
}

func (r *recordingPresentRegistrar) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ProposeRegisterMember")
	}
}

func (r *recordingPresentRegistrar) ProposeRegisterMember(_ context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool, nodeKeyKEKGen uint32) error {
	r.called.Store(true)
	r.nodeID = nodeID
	r.spki = spki
	r.addr = addr
	r.presentsPerNode = presentsPerNode
	r.nodeKeyKEKGen = nodeKeyKEKGen
	close(r.done)
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
