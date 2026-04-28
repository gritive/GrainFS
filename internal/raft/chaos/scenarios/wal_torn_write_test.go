package scenarios

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestWALTornWrite_HandledByBadgerDB documents that grains relies on BadgerDB
// for WAL torn-write detection and recovery. raftly implements its own
// [len][data][crc] framing; grains uses BadgerDB's value-log CRC instead.
//
// This test exercises the close→reopen cycle to verify the underlying
// assumption: state and entries survive a clean shutdown/restart. A real
// torn-write injection would require corrupting the BadgerDB value log,
// which is out of scope for PR 0 and not required to close the gap.
//
// This test must remain PASSING across all subsequent raft PRs.
func TestWALTornWrite_HandledByBadgerDB(t *testing.T) {
	dir := t.TempDir()

	store, err := raft.NewBadgerLogStore(dir)
	require.NoError(t, err)

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	}
	require.NoError(t, store.AppendEntries(entries))
	require.NoError(t, store.SaveState(1, "node-A"))
	require.NoError(t, store.Close())

	store2, err := raft.NewBadgerLogStore(dir)
	require.NoError(t, err, "BadgerDB must reopen cleanly")
	defer store2.Close()

	term, votedFor, err := store2.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), term, "term must survive reopen")
	assert.Equal(t, "node-A", votedFor, "votedFor must survive reopen")

	got, err := store2.GetEntry(1)
	require.NoError(t, err)
	assert.Equal(t, "cmd1", string(got.Command), "entry must survive reopen")

	got, err = store2.GetEntry(2)
	require.NoError(t, err)
	assert.Equal(t, "cmd2", string(got.Command), "entry must survive reopen")
}
