package putpipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// captureSink records every chunk written through the shardSink seam and
// whether the shard was finalized/aborted. It proves the DriveActor streams the
// exact sealed bytes through the seam — the same bytes the S2 remote sink will
// ship to a peer's WriteSealedShard RPC. A nil DriveActor.newSink keeps today's
// local-file behavior (covered byte-identically by the other tests here).
type captureSink struct {
	mu        sync.Mutex
	written   []byte
	writeErr  error // when set, Write returns it (drives the failure dispatch)
	finalized bool
	aborted   bool
}

func (c *captureSink) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written = append(c.written, p...)
	return len(p), nil
}

func (c *captureSink) Finalize() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finalized = true
	return nil
}

func (c *captureSink) Abort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.aborted = true
}

// TestDriveActor_StreamsSealedBytesThroughSink proves the shardSink seam: with
// an injected sink the DriveActor writes the exact concatenated ciphertext and
// finalizes through the seam, without touching the local filesystem. This is
// the dormant S1 contract a remote-stream sink reuses in S2.
func TestDriveActor_StreamsSealedBytesThroughSink(t *testing.T) {
	in := make(chan EncryptedShardChunk, 4)
	commit := make(chan ShardWriteResult, 4)
	sink := &captureSink{}
	d := &DriveActor{
		in:       in,
		dataDir:  t.TempDir(),
		commitCh: commit,
		pending:  make(map[uint64]*shardWriteState),
		newSink: func(bucket, shardKey string, shardIdx int) (shardSink, error) {
			return sink, nil
		},
	}
	d.registerPut(1, "bucket", "key", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("hello"), LastInPut: false}
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte(" world"), LastInPut: true}

	select {
	case res := <-commit:
		require.NoError(t, res.Err)
		require.Equal(t, uint64(1), res.PutID)
		require.Equal(t, int64(11), res.Bytes)
	case <-time.After(2 * time.Second):
		t.Fatal("commit result not received")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Equal(t, "hello world", string(sink.written), "sink must receive the exact sealed bytes")
	require.True(t, sink.finalized, "shard must be finalized through the sink")
	require.False(t, sink.aborted, "successful write must not abort")
}

// TestDriveActor_AbortsSinkOnWriteError locks the seam-level failure dispatch
// the S2 remote sink depends on: a Write error must drive DriveActor to call
// Abort (and emit a failed result), independent of any filesystem behavior.
func TestDriveActor_AbortsSinkOnWriteError(t *testing.T) {
	in := make(chan EncryptedShardChunk, 1)
	commit := make(chan ShardWriteResult, 1)
	sink := &captureSink{writeErr: fmt.Errorf("injected write failure")}
	d := &DriveActor{
		in:       in,
		dataDir:  t.TempDir(),
		commitCh: commit,
		pending:  make(map[uint64]*shardWriteState),
		newSink: func(bucket, shardKey string, shardIdx int) (shardSink, error) {
			return sink, nil
		},
	}
	d.registerPut(1, "bucket", "key", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("data"), LastInPut: true}

	select {
	case res := <-commit:
		require.Error(t, res.Err, "write failure must surface as a failed result")
	case <-time.After(2 * time.Second):
		t.Fatal("commit result not received")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.True(t, sink.aborted, "a Write error must drive DriveActor to Abort the sink")
	require.False(t, sink.finalized, "a failed shard must not be finalized")
}

func TestDriveActor_WritesAndFinalizes(t *testing.T) {
	tmp := t.TempDir()
	in := make(chan EncryptedShardChunk, 4)
	commit := make(chan ShardWriteResult, 4)
	d := &DriveActor{
		in:       in,
		dataDir:  tmp,
		commitCh: commit,
		pending:  make(map[uint64]*shardWriteState),
	}
	d.registerPut(1, "bucket", "key", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("hello"), LastInPut: false}
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte(" world"), LastInPut: true}

	select {
	case res := <-commit:
		require.NoError(t, res.Err)
		require.Equal(t, uint64(1), res.PutID)
		require.Equal(t, 0, res.ShardIdx)
		require.Equal(t, int64(11), res.Bytes)
	case <-time.After(2 * time.Second):
		t.Fatal("commit result not received")
	}

	finalPath := filepath.Join(tmp, "bucket", "key", "shard_0")
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
}

func TestDriveActor_RemovesTmpOnWriteError(t *testing.T) {
	tmp := t.TempDir()
	bucketDir := filepath.Join(tmp, "bucket", "key")
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))
	// Pre-create the tmp path as a directory so os.OpenFile(O_CREATE|O_TRUNC)
	// fails with "is a directory" — this is reliable on all filesystems
	// regardless of privilege level.
	require.NoError(t, os.MkdirAll(filepath.Join(bucketDir, "shard_0.tmp"), 0o755))

	in := make(chan EncryptedShardChunk, 1)
	commit := make(chan ShardWriteResult, 1)
	d := &DriveActor{in: in, dataDir: tmp, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	d.registerPut(1, "bucket", "key", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("data"), LastInPut: true}
	select {
	case res := <-commit:
		require.Error(t, res.Err)
	case <-time.After(2 * time.Second):
		t.Fatal("no commit result")
	}
	// The "tmp file" was a directory we pre-created; it should still exist (we
	// don't remove directories), but no regular file was created.
	fi, err := os.Stat(filepath.Join(bucketDir, "shard_0.tmp"))
	if err == nil {
		require.True(t, fi.IsDir(), "shard_0.tmp should remain a directory, not a stray regular file")
	}
	// Final shard must not exist.
	_, err = os.Stat(filepath.Join(bucketDir, "shard_0"))
	require.True(t, os.IsNotExist(err), "final shard must not have been created")
}

func TestDriveActor_HandlesConcurrentPuts(t *testing.T) {
	tmp := t.TempDir()
	in := make(chan EncryptedShardChunk, 32)
	commit := make(chan ShardWriteResult, 32)
	d := &DriveActor{in: in, dataDir: tmp, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	for putID := uint64(1); putID <= 5; putID++ {
		d.registerPut(putID, "bucket", fmt.Sprintf("key%d", putID), 0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	for putID := uint64(1); putID <= 5; putID++ {
		in <- EncryptedShardChunk{PutID: putID, ShardIdx: 0, Ciphertext: []byte("a"), LastInPut: false}
		in <- EncryptedShardChunk{PutID: putID, ShardIdx: 0, Ciphertext: []byte("b"), LastInPut: true}
	}
	results := map[uint64]bool{}
	for len(results) < 5 {
		select {
		case res := <-commit:
			require.NoError(t, res.Err, "put %d failed", res.PutID)
			results[res.PutID] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only got %d results", len(results))
		}
	}
	for putID := uint64(1); putID <= 5; putID++ {
		require.True(t, results[putID])
		got, err := os.ReadFile(filepath.Join(tmp, "bucket", fmt.Sprintf("key%d", putID), "shard_0"))
		require.NoError(t, err)
		require.Equal(t, "ab", string(got))
	}
}

func TestDriveActor_RecoversFromPanic(t *testing.T) {
	tmp := t.TempDir()
	in := make(chan EncryptedShardChunk, 8)
	commit := make(chan ShardWriteResult, 8)
	d := &DriveActor{
		in:         in,
		dataDir:    tmp,
		commitCh:   commit,
		pending:    make(map[uint64]*shardWriteState),
		panicOnPut: 1,
	}
	d.registerPut(1, "bucket", "key1", 0)
	d.registerPut(2, "bucket", "key2", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	// PUT 1's first chunk triggers the injected panic.
	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("x"), LastInPut: true}
	select {
	case res := <-commit:
		require.Equal(t, uint64(1), res.PutID)
		require.Error(t, res.Err, "panicked PUT must report an error")
	case <-time.After(2 * time.Second):
		t.Fatal("no result for panicked PUT")
	}

	// The actor must still be alive: PUT 2 succeeds.
	in <- EncryptedShardChunk{PutID: 2, ShardIdx: 0, Ciphertext: []byte("ok"), LastInPut: true}
	select {
	case res := <-commit:
		require.Equal(t, uint64(2), res.PutID)
		require.NoError(t, res.Err, "actor must keep serving after a recovered panic")
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not survive the panic")
	}
	got, err := os.ReadFile(filepath.Join(tmp, "bucket", "key2", "shard_0"))
	require.NoError(t, err)
	require.Equal(t, "ok", string(got))
}

// TestDriveActor_RejectsPathTraversalShardKey ensures a shard key carrying
// ".." segments cannot escape the {dataDir}/{bucket} subtree. The shard key
// is ecObjectShardKey(objectKey, versionID); getKey does not normalize
// URL-encoded ".." in the S3 object key, so a crafted key could otherwise
// resolve a shard file outside the shard root once the pipeline is enabled.
func TestDriveActor_RejectsPathTraversalShardKey(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	in := make(chan EncryptedShardChunk, 1)
	commit := make(chan ShardWriteResult, 1)
	d := &DriveActor{in: in, dataDir: dataDir, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	// shardDir resolves to {dataDir}/bucket/../../escape == {root}/escape,
	// which is outside the {dataDir}/bucket containment root.
	d.registerPut(1, "bucket", "../../escape", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("pwned"), LastInPut: true}
	select {
	case res := <-commit:
		require.Error(t, res.Err, "shard key with .. traversal must be rejected")
	case <-time.After(2 * time.Second):
		t.Fatal("no commit result")
	}

	// Nothing may have been written outside the data dir.
	_, statErr := os.Stat(filepath.Join(root, "escape", "shard_0"))
	require.True(t, os.IsNotExist(statErr), "traversal shard must not have escaped the data dir")
}

// TestDriveActor_RejectsPathTraversalBucket covers the second escape vector:
// a bucket of ".." (or one carrying a separator) re-roots the containment
// check so the per-bucket Rel test alone would pass while the path physically
// escapes the data dir. The guard must reject the bucket as an unsafe segment.
func TestDriveActor_RejectsPathTraversalBucket(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	in := make(chan EncryptedShardChunk, 1)
	commit := make(chan ShardWriteResult, 1)
	d := &DriveActor{in: in, dataDir: dataDir, commitCh: commit, pending: make(map[uint64]*shardWriteState)}
	// bucket ".." moves the containment root up to {root}; shardDir resolves
	// to {root}/loot, escaping the {dataDir} the drive owns.
	d.registerPut(1, "..", "loot", 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)

	in <- EncryptedShardChunk{PutID: 1, ShardIdx: 0, Ciphertext: []byte("pwned"), LastInPut: true}
	select {
	case res := <-commit:
		require.Error(t, res.Err, "bucket with .. traversal must be rejected")
	case <-time.After(2 * time.Second):
		t.Fatal("no commit result")
	}

	_, statErr := os.Stat(filepath.Join(root, "loot", "shard_0"))
	require.True(t, os.IsNotExist(statErr), "traversal bucket must not have escaped the data dir")
}
