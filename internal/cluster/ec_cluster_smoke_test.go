package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupObjectECShards_NxMode verifies that LookupObjectECShards returns
// (0, 0, nil) when no ObjectMeta key exists (N× replication mode).
func TestLookupObjectECShards_NxMode(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	k, m, err := fsm.LookupObjectECShards("bkt", "key", "v1")
	require.NoError(t, err)
	assert.Equal(t, 0, k)
	assert.Equal(t, 0, m)
}

// TestLookupObjectECShards_ECMode verifies that LookupObjectECShards returns
// the correct (k, m) stored in ObjectMeta via CmdPutObjectMeta.
func TestLookupObjectECShards_ECMode(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

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

// TestECCluster_Smoke_3Node는 3-node 클러스터에서 EC가 활성화되고
// 노드 1개 다운 후에도 읽기가 성공하는지 검증한다.
//
// TODO: 실제 DistributedBackend 다중 노드 테스트 인프라(newTestCluster, KillNode)
// 연결 후 t.Skip 제거. Phase 19 per-group QUIC 멀티플렉싱 구현 시 함께 완성.
func TestECCluster_Smoke_3Node(t *testing.T) {
	t.Skip("TODO: EC cluster 인프라 연결 필요 (multi-node DistributedBackend harness)")
}
