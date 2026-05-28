package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppliedIndexReqCodec_RoundTrip(t *testing.T) {
	req := AppliedIndexReq{TargetIndex: 0xDEADBEEF12345678}
	wire := encodeAppliedIndexReq(req)
	got, err := decodeAppliedIndexReq(wire)
	require.NoError(t, err)
	require.Equal(t, req, got)
}

func TestAppliedIndexReqCodec_BadMagic(t *testing.T) {
	bad := []byte("XXXBAD\x01" + "\x00\x00\x00\x00\x00\x00\x00\x01")
	_, err := decodeAppliedIndexReq(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad request magic")
}

func TestAppliedIndexRespCodec_RoundTrip(t *testing.T) {
	resp := AppliedIndexResp{NodeID: "node-A", LastApplied: 4242}
	wire := encodeAppliedIndexResp(resp)
	got, err := decodeAppliedIndexResp(wire)
	require.NoError(t, err)
	require.Equal(t, resp, got)
}

func TestAppliedIndexRespCodec_BadMagic(t *testing.T) {
	bad := []byte("XXXBAD\x02" + "\x00\x00")
	_, err := decodeAppliedIndexResp(bad)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad/old response magic")
}

func TestAppliedIndexRespCodec_Truncated(t *testing.T) {
	wire := encodeAppliedIndexResp(AppliedIndexResp{NodeID: "n", LastApplied: 1})
	_, err := decodeAppliedIndexResp(wire[:len(wire)-2])
	require.Error(t, err)
}

func TestHandleAppliedIndexProbe(t *testing.T) {
	resp, err := HandleAppliedIndexProbe(
		encodeAppliedIndexReq(AppliedIndexReq{TargetIndex: 100}),
		"node-X",
		func() uint64 { return 123 },
	)
	require.NoError(t, err)
	r, err := decodeAppliedIndexResp(resp)
	require.NoError(t, err)
	require.Equal(t, "node-X", r.NodeID)
	require.Equal(t, uint64(123), r.LastApplied)
}

func TestWaitVotersApplied_AllReady(t *testing.T) {
	dialer := func(_ context.Context, peer string, payload []byte) ([]byte, error) {
		return encodeAppliedIndexResp(AppliedIndexResp{NodeID: peer, LastApplied: 50}), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := WaitVotersApplied(ctx, 50, []string{"A", "B", "self"}, "self",
		func() uint64 { return 50 }, dialer, nil, 10*time.Millisecond)
	require.NoError(t, err)
}

func TestWaitVotersApplied_LaggingSelfShortcut(t *testing.T) {
	// Self lags initially; dialer should NEVER be called for self.
	var selfCalls int
	dialer := func(_ context.Context, peer string, _ []byte) ([]byte, error) {
		if peer == "self" {
			selfCalls++
		}
		return encodeAppliedIndexResp(AppliedIndexResp{NodeID: peer, LastApplied: 100}), nil
	}
	doneCh := make(chan struct{})
	localFn := func() uint64 {
		select {
		case <-doneCh:
			return 100
		default:
			return 40 // lagging
		}
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(doneCh)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := WaitVotersApplied(ctx, 100, []string{"A", "self"}, "self", localFn, dialer, nil, 20*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 0, selfCalls, "dialer must not be called for self")
}

func TestWaitVotersApplied_TimeoutOnLaggard(t *testing.T) {
	dialer := func(_ context.Context, peer string, _ []byte) ([]byte, error) {
		la := uint64(100)
		if peer == "slow" {
			la = 10
		}
		return encodeAppliedIndexResp(AppliedIndexResp{NodeID: peer, LastApplied: la}), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := WaitVotersApplied(ctx, 100, []string{"fast", "slow"}, "self",
		func() uint64 { return 100 }, dialer, nil, 20*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "slow")
}

func TestWaitVotersApplied_CheckNodeID_Mismatch(t *testing.T) {
	// checkNodeID returns error when the response nodeID doesn't match the
	// voter address mapping — simulates a wrong-peer response.
	dialer := func(_ context.Context, peer string, _ []byte) ([]byte, error) {
		return encodeAppliedIndexResp(AppliedIndexResp{NodeID: "wrong-node", LastApplied: 100}), nil
	}
	checker := func(voter, nodeID string) error {
		if nodeID == "wrong-node" {
			return fmt.Errorf("applied_index_probe: peer %s returned unexpected nodeID %q", voter, nodeID)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := WaitVotersApplied(ctx, 100, []string{"peer-addr"}, "self",
		func() uint64 { return 100 }, dialer, checker, 20*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong-node")
}
