package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// deadlineCapturingNode records the deadline the handler imposed on ProposeWait,
// then short-circuits with an error so WaitApplied/ApplyError are skipped
// (HandleGroupPropose calls them only when ProposeWait's err == nil). The
// embedded RaftNode is nil — never dereferenced because ProposeWait returns
// before any other method is reached.
type deadlineCapturingNode struct {
	RaftNode
	captured time.Duration
}

func (n *deadlineCapturingNode) ProposeWait(ctx context.Context, _ []byte) (uint64, error) {
	if d, ok := ctx.Deadline(); ok {
		n.captured = time.Until(d)
	}
	return 0, errors.New("captured")
}

// TestHandleGroupPropose_DeadlineHonorsForwardTimeout proves the receiver-side
// forward handler imposes the generous proposeForwardTimeout on the leader's
// raft commit, NOT the old hardcoded 5s. The 5s aborted commits the caller was
// still willing to wait for — the dominant (~77%) `context deadline exceeded`
// CompleteMultipartUpload-under-load failure mode.
func TestHandleGroupPropose_DeadlineHonorsForwardTimeout(t *testing.T) {
	rcv, mgr := setupReceiver(t, "self")
	node := &deadlineCapturingNode{}
	dist := &DistributedBackend{node: node}
	gb := WrapDistributedBackend("g1", dist)
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self", "peer-A"}, gb))

	payload := encodeGroupForwardPayload("g1", []byte("dummy-raft-cmd"))
	reply := rcv.HandleGroupPropose(&transport.Message{
		Type:    transport.StreamDataGroupProposeForward,
		Payload: payload,
	})
	require.NotNil(t, reply)

	// RED on the unfixed code: captured ≈ 5s. GREEN after the fix: ≈ 30s.
	require.Greater(t, node.captured, 10*time.Second,
		"HandleGroupPropose must impose proposeForwardTimeout, not the old hardcoded 5s")
}
