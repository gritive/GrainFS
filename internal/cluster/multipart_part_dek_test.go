package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// newDEKBackendAt builds a production-shaped DistributedBackend (WithShardDEKKeeper,
// static encryptor nil) rooted at dataDir and sealed by the supplied keeper. It
// returns an explicit teardown so the caller can fully close b1 (releasing the
// badger dir lock) before opening b2 over the same dataDir — a same-keeper
// "restart". Unlike newTestDistributedBackendDEK it does NOT auto-register
// t.Cleanup for the close, so the caller controls lifecycle sequencing.
func newDEKBackendAt(t *testing.T, dataDir string, keeper *encrypt.DEKKeeper, clusterID []byte) (*DistributedBackend, func()) {
	t.Helper()

	metaDir := dataDir + "/meta"
	db, err := badger.Open(badgerutil.SmallOptions(metaDir))
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeRaft, err := newRaftNode(cfg, dataDir)
	require.NoError(t, err)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.NoError(t, node.Bootstrap())
	for range 2000 {
		if node.IsLeader() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, node.IsLeader(), "no-peers node must become leader")

	backend, err := NewDistributedBackend(dataDir, db, node, nil, false)
	require.NoError(t, err)
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})

	svc := NewShardService(backend.root, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(t, keeper, clusterID))
	require.NotNil(t, svc.DEKKeeper(), "production shape: DEK keeper must be wired")
	backend.SetShardService(svc, []string{backend.selfAddr})

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	teardown := func() {
		if backend.coalesceCancel != nil {
			backend.coalesceCancel()
		}
		if backend.coalesce != nil {
			backend.coalesce.Stop()
		}
		if backend.shardSvc != nil {
			_ = backend.shardSvc.Close()
		}
		close(stopApply)
		node.Close()
		db.Close()
		if closeRaft != nil {
			_ = closeRaft()
		}
	}
	return backend, teardown
}

// TestMultipartPartSurvivesRestartUnderDEK proves the greenfield boundary for
// durable multipart part files is per-part AEAD: a part sealed under one DEK
// keeper is (a) ciphertext on disk and (b) decrypts back to its plaintext via a
// fresh backend over the SAME data dir + keeper (a same-keeper restart). The
// part read path (openMultipartPart) is purely filesystem + seam, so it does not
// depend on raft metadata recovery; completability across restart is a
// pre-existing durability property covered by the multipart suite.
func TestMultipartPartSurvivesRestartUnderDEK(t *testing.T) {
	dir := t.TempDir()
	clusterID := bytes.Repeat([]byte{0x78}, 16)
	kek := bytes.Repeat([]byte{0x77}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)

	b1, teardown1 := newDEKBackendAt(t, dir, keeper, clusterID)

	ctx := context.Background()
	require.NoError(t, b1.CreateBucket(ctx, "bkt"))
	up, err := b1.CreateMultipartUpload(ctx, "bkt", "obj", "")
	require.NoError(t, err)
	uploadID := up.UploadID

	partBody := bytes.Repeat([]byte("P"), 6<<20)
	_, err = b1.UploadPart(ctx, "bkt", "obj", uploadID, 1, bytes.NewReader(partBody))
	require.NoError(t, err)

	// On-disk part file is ciphertext.
	raw, err := os.ReadFile(b1.partPath(uploadID, 1))
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, partBody[:4096]), "part must be ciphertext on disk")

	// Fully close b1 (release the badger dir lock) before rebuilding.
	teardown1()

	// Rebuild over the SAME dir + keeper (simulated restart) and read the part
	// back through the seam-aware part reader.
	b2, teardown2 := newDEKBackendAt(t, dir, keeper, clusterID)
	defer teardown2()

	rc, err := b2.openMultipartPart(uploadID, 1)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, partBody, got)
}
