package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
)

var errFenceReadIndex = errors.New("test: ReadIndex unavailable")

// fenceErrNode is a RaftNode whose ReadIndex always fails (and is NOT
// ErrNotLeader, so DistributedBackend.ReadIndex returns it directly; Peers() is
// nil). Used to prove a forwarded read calls the read fence BEFORE resolving.
type fenceErrNode struct{ applyChCloseStubNode }

func (n *fenceErrNode) ReadIndex(context.Context) (uint64, error) { return 0, errFenceReadIndex }

// TestForwardedVersionReadsAreReadFenced proves every forwarded read handler that
// resolves through a read1 blob-authority authority branch (HeadObjectVersion,
// GetObjectVersion[+stream], GetObjectTags, ReadAt[+stream]) calls
// waitForwardReadFence BEFORE resolving — so a lagging receiver cannot read a
// stale local blob-authority state and take the wrong authority branch. With a healthy
// object present, WITHOUT the fence each handler resolves and returns OK; WITH the
// fence (ReadIndex fails) they return Internal before any resolution.
func TestForwardedVersionReadsAreReadFenced(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := t.Context()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	require.NoError(t, b.SetBucketVersioning("b", "Enabled"))
	vid := putVersioned(t, b, ctx, "b", "k", "data")

	// Swap in a ReadIndex-erroring node so the forward read fence fails before any
	// object resolution. Resolution does not use node.ReadIndex, so without the
	// fence these handlers still resolve the (real) object and return OK.
	b.node = &fenceErrNode{}

	rcv, _ := setupReceiver(t, "self")
	gb := WrapDistributedBackend("g1", b)
	dg := NewDataGroupWithBackend("g1", []string{"self"}, gb)

	statusOf := func(reply []byte) raftpb.ForwardStatus {
		return raftpb.GetRootAsForwardReply(reply, 0).Status()
	}

	// Non-streaming handlers.
	require.Equal(t, raftpb.ForwardStatusInternal,
		statusOf(rcv.handleHeadObjectVersion(dg, buildHeadObjectVersionArgs("b", "k", vid, 0))),
		"handleHeadObjectVersion must read-fence before resolving")
	require.Equal(t, raftpb.ForwardStatusInternal,
		statusOf(rcv.handleGetObjectVersion(dg, buildGetObjectVersionArgs("b", "k", vid, 0))),
		"handleGetObjectVersion must read-fence before resolving")
	require.Equal(t, raftpb.ForwardStatusInternal,
		statusOf(rcv.handleGetObjectTags(dg, buildGetObjectTagsArgs("b", "k", ""))),
		"handleGetObjectTags must read-fence before resolving")
	require.Equal(t, raftpb.ForwardStatusInternal,
		statusOf(rcv.handleReadAt(dg, buildReadAtArgs("b", "k", 0, 4))),
		"handleReadAt must read-fence before resolving")

	// Streaming tails: fence before returning the stream → Internal status + nil body.
	repl, rc := rcv.handleGetObjectVersionRead(dg, buildGetObjectVersionArgs("b", "k", vid, 0))
	require.Equal(t, raftpb.ForwardStatusInternal, statusOf(repl), "handleGetObjectVersionRead must read-fence")
	require.Nil(t, rc)

	repl, rc = rcv.handleReadAtRead(dg, buildReadAtArgs("b", "k", 0, 4))
	require.Equal(t, raftpb.ForwardStatusInternal, statusOf(repl), "handleReadAtRead must read-fence")
	require.Nil(t, rc)
}
