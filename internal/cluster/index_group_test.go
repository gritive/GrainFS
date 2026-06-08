package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putCmd(t *testing.T, bucket, key, versionID, group string) []byte {
	t.Helper()
	payload, err := encodeMetaPutObjectIndexCmd(ObjectIndexEntry{
		Bucket: bucket, Key: key, VersionID: versionID, PlacementGroupID: group, Size: 1, ModTime: 1,
	}, false)
	require.NoError(t, err)
	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, payload)
	require.NoError(t, err)
	return data
}

// mustPutPayload returns the MetaCmd PAYLOAD (cmd.DataBytes() equivalent) for a put.
func mustPutPayload(t *testing.T, bucket, key, versionID, group string) []byte {
	t.Helper()
	payload, err := encodeMetaPutObjectIndexCmd(ObjectIndexEntry{
		Bucket: bucket, Key: key, VersionID: versionID, PlacementGroupID: group, Size: 1, ModTime: 1,
	}, false)
	require.NoError(t, err)
	return payload
}

func TestIndexGroup_ApplyLoop_RoundTripAndWatermark(t *testing.T) {
	fsm := NewMetaFSM()
	ig := newIndexGroup(nil, fsm, nil)
	ch := make(chan raft.LogEntry, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ig.runApplyLoop(ctx, ch)

	ch <- raft.LogEntry{Index: 1, Term: 1, Type: raft.LogEntryCommand, Command: putCmd(t, "b", "k", "v1", "g0")}

	wctx, wcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wcancel()
	require.NoError(t, ig.waitAppliedResult(wctx, 1))

	got, ok := fsm.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	assert.Equal(t, "v1", got.VersionID)
}

func TestIndexGroup_ApplyLoop_GuardRejectsNonIndexAndMalformed(t *testing.T) {
	fsm := NewMetaFSM()
	ig := newIndexGroup(nil, fsm, nil)
	ch := make(chan raft.LogEntry, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ig.runApplyLoop(ctx, ch)

	// (a) non-object-index command (AddNode): guarded out, recorded as error,
	//     watermark still advances (entry consumed). FSM untouched.
	addNode, err := encodeMetaCmd(MetaCmdTypeAddNode, []byte{})
	require.NoError(t, err)
	ch <- raft.LogEntry{Index: 1, Term: 1, Type: raft.LogEntryCommand, Command: addNode}
	// (b) malformed bytes: must not panic; recorded as error; watermark advances.
	ch <- raft.LogEntry{Index: 2, Term: 1, Type: raft.LogEntryCommand, Command: []byte{0xde, 0xad}}
	// (c) empty bytes: recorded as error; watermark advances.
	ch <- raft.LogEntry{Index: 3, Term: 1, Type: raft.LogEntryCommand, Command: nil}

	wctx, wcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wcancel()
	for idx := uint64(1); idx <= 3; idx++ {
		err := ig.waitAppliedResult(wctx, idx)
		require.Error(t, err, "index %d should record a guard/decode error", idx)
		assert.Contains(t, err.Error(), "index group")
	}
	assert.False(t, fsm.HasUserData())
}

func TestIndexGroup_ApplyLoop_SnapshotEntryRestores(t *testing.T) {
	// The apply-loop LogEntrySnapshot branch must restore FSM state. Build snapshot
	// bytes from a populated FSM, then feed them as a LogEntrySnapshot to a fresh group.
	src := NewMetaFSM()
	wireTestKEK(t, src) // Snapshot() requires a KEK store wired
	require.NoError(t, src.applyPutObjectIndex(mustPutPayload(t, "b", "k", "v9", "g0")))
	snapData, err := src.Snapshot()
	require.NoError(t, err)

	fsm := NewMetaFSM()
	wireTestKEK(t, fsm) // Restore() requires a KEK store wired
	ig := newIndexGroup(nil, fsm, nil)
	ch := make(chan raft.LogEntry, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go ig.runApplyLoop(ctx, ch)

	ch <- raft.LogEntry{Index: 5, Term: 2, Type: raft.LogEntrySnapshot, Command: snapData}

	wctx, wcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer wcancel()
	require.NoError(t, ig.waitAppliedResult(wctx, 5))
	got, ok := fsm.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	assert.Equal(t, "v9", got.VersionID)
}
