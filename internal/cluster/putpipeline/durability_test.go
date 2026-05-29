package putpipeline

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/require"
)

// blockingWAL records when AppendBatch is entered and blocks there until
// release is closed. It lets a test assert that Put() does not return
// before the WAL fsync (group commit) has completed — i.e. shard
// durability precedes the handler's success return and the downstream
// metadata propose.
type blockingWAL struct {
	called  chan struct{}
	release chan struct{}
}

func (w *blockingWAL) AppendBatch(ctx context.Context, records []ShardWALRecord) (bool, error) {
	select {
	case w.called <- struct{}{}:
	default:
	}
	<-w.release
	return true, nil
}

// errWAL fails every AppendBatch. A WAL fsync failure must surface as a
// PUT error, not be swallowed.
type errWAL struct{}

func (errWAL) AppendBatch(ctx context.Context, records []ShardWALRecord) (bool, error) {
	return false, fmt.Errorf("injected wal fsync failure")
}

// TestPipeline_Put_WaitsForWALDurability asserts Put() does not return
// until the WAL group-commit fsync (AppendBatch) has completed. Returning
// on the K-shard early-ack alone would let the caller propose object
// metadata to raft before the shards are durable.
func TestPipeline_Put_WaitsForWALDurability(t *testing.T) {
	wal := &blockingWAL{called: make(chan struct{}, 1), release: make(chan struct{})}
	p := New(Config{
		DataDirs:  []string{t.TempDir()},
		DEKKeeper: testDEKKeeper(t),
		ClusterID: testClusterID(),
		ECConfig:  cluster.ECConfig{DataShards: 1, ParityShards: 0},
		WAL:       wal,
	})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	}()

	body := []byte("durable-or-not")
	size := int64(len(body))
	putDone := make(chan error, 1)
	go func() {
		_, err := p.Put(context.Background(), PutRequest{
			Bucket:   "external",
			Key:      "obj-durable",
			Body:     bytes.NewReader(body),
			SizeHint: &size,
		})
		putDone <- err
	}()

	// AppendBatch must be reached (all shards on disk, group-commit in flight).
	select {
	case <-wal.called:
	case <-time.After(3 * time.Second):
		t.Fatal("AppendBatch was never called")
	}

	// Put must still be blocked: durability (the WAL fsync) has not completed.
	select {
	case err := <-putDone:
		t.Fatalf("Put returned before WAL durability completed (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Release the fsync; Put must now complete successfully.
	close(wal.release)
	select {
	case err := <-putDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Put did not return after WAL durability completed")
	}
}

// TestPipeline_Put_PropagatesWALFsyncError asserts a WAL group-commit
// fsync failure makes Put() return an error instead of reporting success.
func TestPipeline_Put_PropagatesWALFsyncError(t *testing.T) {
	p := New(Config{
		DataDirs:  []string{t.TempDir()},
		DEKKeeper: testDEKKeeper(t),
		ClusterID: testClusterID(),
		ECConfig:  cluster.ECConfig{DataShards: 1, ParityShards: 0},
		WAL:       errWAL{},
	})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Shutdown(ctx)
	}()

	body := []byte("fsync-fails")
	size := int64(len(body))
	_, err := p.Put(context.Background(), PutRequest{
		Bucket:   "external",
		Key:      "obj-fsync-fail",
		Body:     bytes.NewReader(body),
		SizeHint: &size,
	})
	require.Error(t, err, "WAL fsync failure must fail the PUT")
}
