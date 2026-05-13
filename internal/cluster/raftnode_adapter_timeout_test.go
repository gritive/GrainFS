package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestSendTimeoutNow_ErrNotImplemented_WhenNotWired verifies that
// raftTransportBridge.SendTimeoutNow returns ErrNotImplemented when
// SetTimeoutNowTransport has not been called (sendTN pointer is nil).
func TestSendTimeoutNow_ErrNotImplemented_WhenNotWired(t *testing.T) {
	b := &raftTransportBridge{}
	_, err := b.SendTimeoutNow("peer", &raft.TimeoutNowArgs{Term: 1})
	require.True(t, errors.Is(err, raft.ErrNotImplemented),
		"expected ErrNotImplemented, got %v", err)
}

// TestSetTimeoutNowTransport_BeforeSetTransport_NoPanic verifies that
// calling SetTimeoutNowTransport before SetTransport (bridge == nil) is
// a safe no-op and does not panic. The fn is silently dropped; this is
// acceptable because run.go always calls SetTransport first.
func TestSetTimeoutNowTransport_BeforeSetTransport_NoPanic(t *testing.T) {
	a := &raftNodeAdapter{} // bridge is nil
	require.NotPanics(t, func() {
		a.SetTimeoutNowTransport(func(_ string, _ *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
			return nil, nil
		})
	})
}
