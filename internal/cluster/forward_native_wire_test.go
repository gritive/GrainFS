package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestForwardNativeWire is the real-wire proof for the Phase 8 N7-2 native
// forward routes: two real HTTPTransports, a real leader-backed ForwardReceiver
// (newTestGroupBackend single-voter leader, same harness the streamed PutObject
// receiver test uses), and a ForwardSender wired with the SAME native dialer
// closures boot uses (forwardStreamDialer → ForwardWrite, forwardReadStreamDialer
// → ForwardRead). It proves, over the wire:
//
//  1. streamed-body write forward round-trip (PutObjectStream through HandleBody
//     into the GroupBackend);
//  2. streamed-response read forward round-trip for BOTH ForwardOpGetObject
//     (whole object) and ForwardOpReadAt (ranged — forwardRuntime.readAt routes
//     large range reads through the same dialer);
//  3. in-band application status passthrough: an unknown group yields an FB
//     reply decoding to ForwardStatusNotVoter, NOT a transport error (the native
//     wire preserves ForwardSender's hint-redirect protocol — HTTP status is
//     transport-only);
//  4. positive native dispatch (InboundNativeForwardWrites/Reads > 0);
//  5. neuter-verify (one-time): with the native handlers unregistered the calls
//     fail with the transport 503, proving the native route — not the tunnel —
//     served the calls above.
func TestForwardNativeWire(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	recvTr := transport.MustNewHTTPTransport("fwd-native-psk")
	cliTr := transport.MustNewHTTPTransport("fwd-native-psk")
	t.Cleanup(func() { recvTr.Close(); cliTr.Close() })
	require.NoError(t, recvTr.Listen(context.Background(), "127.0.0.1:0"))
	addr := recvTr.LocalAddr()

	recvTr.RegisterForwardWriteHandler(rcv.HandleBody)
	recvTr.RegisterForwardReadHandler(rcv.HandleRead)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// (3) In-band NotVoter passthrough — and server readiness gate. The frame
	// targets a group this receiver does not host; once the listener serves, the
	// call must succeed at the transport level (200) and the FB reply must decode
	// to NotVoter. Polled because Listen returns before Hertz accepts.
	unknownFrame := encodeForwardPayload("group-unknown", raftpb.ForwardOpPutObject,
		buildPutObjectArgs("bucket", "nv-key", "text/plain", nil))
	var nvReply []byte
	require.Eventually(t, func() bool {
		reply, err := cliTr.ForwardWrite(ctx, addr, unknownFrame, bytes.NewReader(nil))
		if err != nil {
			return false
		}
		nvReply = reply
		return true
	}, 10*time.Second, 50*time.Millisecond, "forward write to unknown group must succeed at transport level")
	require.Equal(t, raftpb.ForwardStatusNotVoter, raftpb.GetRootAsForwardReply(nvReply, 0).Status(),
		"unknown group must surface as in-band NotVoter, not a transport error")

	// Sender shaped exactly like boot (boot_phases_forwarders.go): the Call-shaped
	// propose dialer stays on the tunnel (out of scope here — it would need a
	// tunnel registration), only the two streaming dialers are native.
	proposeDialer := func(context.Context, string, []byte) ([]byte, error) {
		return nil, errors.New("propose forwards stay on the tunnel (not under test)")
	}
	streamDialer := func(callCtx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		return cliTr.ForwardWrite(callCtx, peer, payload, body)
	}
	readStreamDialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		return cliTr.ForwardRead(callCtx, peer, payload)
	}
	sender := NewForwardSender(proposeDialer).
		WithStreamDialer(streamDialer).
		WithReadStreamDialer(readStreamDialer)

	// (1) Write forward round-trip: streamed PutObject lands in the GroupBackend.
	const objBody = "native-forward-wire-body"
	putArgs := buildPutObjectArgs("bucket", "wire-key", "text/plain", nil)
	reply, err := sender.SendStream(ctx, []string{addr}, "group-1",
		raftpb.ForwardOpPutObject, putArgs, bytes.NewReader([]byte(objBody)))
	require.NoError(t, err)
	require.Equal(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(reply, 0).Status())
	head, err := gb.HeadObject(ctx, "bucket", "wire-key")
	require.NoError(t, err)
	require.Equal(t, int64(len(objBody)), head.Size)

	// (2a) Read forward round-trip — GetObject (whole object).
	reply, rc, err := sender.SendReadStream(ctx, []string{addr}, "group-1",
		raftpb.ForwardOpGetObject, buildGetObjectArgs("bucket", "wire-key", versioningStateUnknown))
	require.NoError(t, err)
	require.Equal(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(reply, 0).Status())
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, objBody, string(got))

	// (2b) Read forward round-trip — ReadAt (ranged; same dialer, same route).
	const raOff, raLen = 7, 12
	reply, rc, err = sender.SendReadStream(ctx, []string{addr}, "group-1",
		raftpb.ForwardOpReadAt, buildReadAtArgs("bucket", "wire-key", raOff, raLen))
	require.NoError(t, err)
	require.Equal(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(reply, 0).Status())
	got, err = io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, objBody[raOff:raOff+raLen], string(got))

	// (4) Positive dispatch: the native routes — not the tunnel — served these.
	require.Positive(t, recvTr.InboundNativeForwardWrites())
	require.Positive(t, recvTr.InboundNativeForwardReads())

	// (5) Neuter-verify: unregister the native handlers; the same calls must now
	// fail with the transport-level 503, proving the assertions above exercised
	// the native routes (no silent tunnel fallback exists for these dialers).
	recvTr.RegisterForwardWriteHandler(nil)
	recvTr.RegisterForwardReadHandler(nil)
	shortCtx, shortCancel := context.WithTimeout(ctx, 3*time.Second)
	defer shortCancel()
	_, err = cliTr.ForwardWrite(shortCtx, addr, unknownFrame, bytes.NewReader(nil))
	require.ErrorContains(t, err, "status 503")
	_, _, err = cliTr.ForwardRead(shortCtx, addr, unknownFrame)
	require.ErrorContains(t, err, "status 503")
}
