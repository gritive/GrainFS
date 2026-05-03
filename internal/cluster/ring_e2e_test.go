package cluster

// ring_e2e_test.go: 링 기반 EC 쓰기/읽기 경로 통합 테스트.
//
// 단일 노드(항상 리더)에서 CmdSetRing으로 링을 초기화한 뒤
// PutObject/GetObject가 RingVersion 기반 배치를 사용하는지 검증한다.

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putBytes(t *testing.T, b *DistributedBackend, bucket, key string, data []byte) {
	t.Helper()
	_, err := b.PutObject(context.Background(), bucket, key, bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err)
}

// proposeRing은 테스트 전용 — nodeIDs로 Ring v1을 FSM에 직접 propose한다.
func proposeRing(t *testing.T, b *DistributedBackend, nodeIDs []string) {
	t.Helper()
	ring := NewRing(1, nodeIDs, 10)
	err := b.propose(context.Background(), CmdSetRing, SetRingCmd{
		Version:  ring.Version,
		VNodes:   ring.VNodes,
		VPerNode: ring.VPerNode,
	})
	require.NoError(t, err)
}

func TestRing_PutObjectEC_UsesRingVersion(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	backend.SetShardService(svc, []string{selfAddr})
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1}) // 1+1 so 1 node works

	// 링 초기화
	proposeRing(t, backend, []string{selfAddr})

	// 링 버전이 FSM에 반영됐는지 확인
	rv := backend.CurrentRingVersion()
	assert.Equal(t, RingVersion(1), rv, "ring version should be 1 after init")

	// 버킷 생성 + 오브젝트 PUT
	require.NoError(t, backend.CreateBucket(context.Background(), "bucket"))
	content := []byte("hello ring ec world")
	putBytes(t, backend, "bucket", "key", content)

	// GET — RingVersion 기반 경로로 복구 가능해야 함
	rc, obj, err := backend.GetObject(context.Background(), "bucket", "key")
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, got), "content must round-trip via ring EC path")
}

func TestRing_GetObjectFallsBackWhenNoRing(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	backend.SetShardService(svc, []string{selfAddr})
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})

	// 링을 초기화하지 않은 상태: PlacementForNodes 경로
	require.NoError(t, backend.CreateBucket(context.Background(), "bucket"))
	content := []byte("no ring fallback")
	putBytes(t, backend, "bucket", "key", content)

	rc, obj, err := backend.GetObject(context.Background(), "bucket", "key")
	require.NoError(t, err)
	require.NotNil(t, obj)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, content, got)
}

func TestRing_CurrentRingVersion_ZeroBeforeInit(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	assert.Equal(t, RingVersion(0), backend.CurrentRingVersion())
}

func TestRing_CurrentRingVersion_AfterInit(t *testing.T) {
	backend := NewSingletonBackendForTest(t)
	proposeRing(t, backend, []string{"node-A", "node-B"})
	assert.Equal(t, RingVersion(1), backend.CurrentRingVersion())
}

func TestRing_ReshardToRing_NoopWhenSameVersion(t *testing.T) {
	backend := NewSingletonBackendForTest(t)

	const selfAddr = "self"
	shardDir := t.TempDir()
	svc := NewShardService(shardDir, nil)
	backend.SetShardService(svc, []string{selfAddr})
	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 1})

	proposeRing(t, backend, []string{selfAddr})
	require.NoError(t, backend.CreateBucket(context.Background(), "b"))
	putBytes(t, backend, "b", "k", []byte("data"))

	// ReshardToRing with current ring version should be a no-op
	err := backend.ReshardToRing(context.Background(), "b", "k", RingVersion(1))
	require.NoError(t, err)
}
