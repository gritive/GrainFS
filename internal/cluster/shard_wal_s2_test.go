package cluster

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncRecorder records the ORDER of fsync calls (file vs dir) via the
// ShardService test seams. The hooks are no-ops on a tmpfs — we assert the
// CALL SEQUENCE, since real power-loss is untestable in a unit test.
type syncRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *syncRecorder) file(*os.File) error {
	r.mu.Lock()
	r.events = append(r.events, "file")
	r.mu.Unlock()
	return nil
}

func (r *syncRecorder) dir(string) error {
	r.mu.Lock()
	r.events = append(r.events, "dir")
	r.mu.Unlock()
	return nil
}

func (r *syncRecorder) seq() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

// dirCount returns how many dir-fsync events were recorded.
func (r *syncRecorder) dirCount() int {
	n := 0
	for _, e := range r.seq() {
		if e == "dir" {
			n++
		}
	}
	return n
}

// TestWriteEncryptedShardFile_LockedFsyncOrder proves the D2 locked durability
// order on a class that requires fsync (large no-redundancy, requireFsync=true):
// the shard file is fsynced FIRST, THEN the parent-directory chain is fsynced
// (one or more dir events: leaf shard dir + each newly-created ancestor up to
// the data dir), all before the write returns.
//
// RED before S2: no syncDir call exists → seq == ["file"] (no dir events).
// Mutation: drop the s.syncDirChain(...) call → dirCount == 0 → RED.
func TestWriteEncryptedShardFile_LockedFsyncOrder(t *testing.T) {
	backend, _, _, _ := newS1ShardSvc(t,
		ECConfig{DataShards: 1, ParityShards: 0}, []string{"self"},
		WithNoRedundancy(func() bool { return true }))
	rec := &syncRecorder{}
	backend.shardSvc.syncFileHook = rec.file
	backend.shardSvc.syncDirHook = rec.dir

	large := bytes.Repeat([]byte("s2-locked-order-"), 1<<17) // > 1MiB, requireFsync=true
	_, err := backend.PutObject(context.Background(), "b", "obj", bytes.NewReader(large), "application/octet-stream")
	require.NoError(t, err)

	seq := rec.seq()
	require.NotEmpty(t, seq)
	require.Equal(t, "file", seq[0], "shard file must be fsynced FIRST")
	require.GreaterOrEqual(t, rec.dirCount(), 1, "at least the leaf shard dir must be fsynced after the file")
	for _, e := range seq[1:] {
		require.Equal(t, "dir", e, "every fsync after the file fsync must be a dir fsync (locked order)")
	}
}
