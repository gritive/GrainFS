package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupObjectECShards_NxMode verifies that LookupObjectECShards returns
// (0, 0, nil) when no ObjectMeta key exists (N× replication mode).
func TestLookupObjectECShards_NxMode(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	k, m, err := fsm.LookupObjectECShards("bkt", "key", "v1")
	require.NoError(t, err)
	assert.Equal(t, 0, k)
	assert.Equal(t, 0, m)
}

// TestLookupObjectECShards_ECMode verifies that LookupObjectECShards returns
// the correct (k, m) stored in ObjectMeta via CmdPutObjectMeta.
func TestLookupObjectECShards_ECMode(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	raw, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:    "bkt",
		Key:       "key",
		VersionID: "v1",
		Size:      1024,
		ECData:    2,
		ECParity:  1,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(raw))

	k, m, err := fsm.LookupObjectECShards("bkt", "key", "v1")
	require.NoError(t, err)
	assert.Equal(t, 2, k, "DataShards=2 기대")
	assert.Equal(t, 1, m, "ParityShards=1 기대")
}

// TestECCluster_Smoke_3Node verifies the in-process 3-holder EC path: a 2+1
// object is readable after one locally held shard disappears. Multi-process
// node failure is covered by tests/e2e/cluster_ec_test.go.
func TestECCluster_Smoke_3Node(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	require.NoError(t, backend.CreateBucket(context.Background(), "ec-smoke"))
	backend.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})

	enc := testEncryptor(t)
	svc := NewShardService(backend.root, nil, WithEncryptor(enc), withTestWALEnc(t, enc))
	backend.SetShardService(svc, []string{"self", "self", "self"})

	content := bytes.Repeat([]byte("ec-smoke-3node-"), 4096)
	obj, err := backend.PutObject(context.Background(), "ec-smoke", "obj", bytes.NewReader(content), "application/octet-stream")
	require.NoError(t, err)
	require.NotEmpty(t, obj.VersionID)

	shardKey := "obj/" + obj.VersionID
	require.NoError(t, os.Remove(backend.shardSvc.getShardPath("ec-smoke", shardKey, 0)))

	rc, _, err := backend.GetObject(context.Background(), "ec-smoke", "obj")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}
