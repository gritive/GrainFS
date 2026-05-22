package putpipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
