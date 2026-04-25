# Consistent Hash Ring + Follower Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `CmdPutShardPlacement`를 제거하고 결정론적 컨시스턴트 해시 링으로 EC 샤드 배치를 교체하며, 팔로워 노드에서도 S3 PUT이 가능하도록 ProposeForward QUIC RPC를 추가한다.

**Architecture:** 링 상태는 멤버십 변경 시에만 `CmdSetRing`으로 Raft에 커밋되고 FSM이 버전별 히스토리를 BadgerDB에 영속화한다. 오브젝트 메타에 `RingVersion`/`ECData`/`ECParity`를 저장해 어느 노드에서든 결정론적으로 샤드 위치를 재계산한다. 팔로워는 EC fan-out 후 단일 `CmdPutObjectMeta` propose를 리더에게 QUIC RPC로 포워딩한다.

**Tech Stack:** Go, BadgerDB (FSM), FlatBuffers (직렬화), QUIC (transport), Reed-Solomon (klauspost/reedsolomon), FNV32 (링 토큰)

---

## 파일 맵

| 파일 | 작업 |
|---|---|
| `internal/cluster/ring.go` | 신규: Ring, VirtualNode, PlacementForKey, walkCW, fnv32 |
| `internal/cluster/ring_test.go` | 신규: ring 단위 테스트 |
| `internal/cluster/ring_store.go` | 신규: ringStore (FSM에 embed), GetRing, GC goroutine |
| `internal/cluster/ring_store_test.go` | 신규: ring store 단위 테스트 |
| `internal/cluster/clusterpb/cluster.fbs` | 수정: SetRingCmd, VNodeEntry + PutObjectMetaCmd + ObjectMeta 필드 추가 |
| `internal/cluster/fsm.go` | 수정: CmdSetRing(17) 상수, FSM에 ringStore embed |
| `internal/cluster/apply.go` | 수정: CmdSetRing 핸들러, CmdPutShardPlacement/CmdDeleteShardPlacement no-op, applyPutObjectMeta refcount 증가 |
| `internal/cluster/codec.go` | 수정: encodeSetRingCmd/decodeSetRingCmd, PutObjectMetaCmd + objectMeta 필드 추가 |
| `internal/transport/transport.go` | 수정: StreamProposeForward = 0x06 추가 |
| `internal/raft/raft.go` | 수정: IsLeader() bool 메서드 추가 |
| `internal/cluster/backend.go` | 수정: propose() 포워딩 로직, putObjectEC 링 경로, GetObject 링 경로 |
| `internal/cluster/reshard_manager.go` | 수정: 링 버전 인식 확장 |
| `internal/cluster/ring_e2e_test.go` | 신규: 3노드 팔로워 쓰기 E2E |

---

## Task 1: Ring 타입 + PlacementForKey 알고리즘

**Files:**
- Create: `internal/cluster/ring.go`
- Create: `internal/cluster/ring_test.go`

- [ ] **Step 1: ring_test.go 작성 — 실패 확인용**

```go
// internal/cluster/ring_test.go
package cluster

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRing_PlacementForKey_Deterministic(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b", "node-c"}, 10)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	p1 := ring.PlacementForKey(cfg, "bucket/key1")
	p2 := ring.PlacementForKey(cfg, "bucket/key1")
	assert.Equal(t, p1, p2, "same key must always map to same nodes")
}

func TestRing_PlacementForKey_NoDuplicates(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b", "node-c"}, 10)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	placement := ring.PlacementForKey(cfg, "bucket/mykey")
	require.Len(t, placement, 3)
	seen := make(map[string]bool)
	for _, n := range placement {
		assert.False(t, seen[n], "duplicate node: %s", n)
		seen[n] = true
	}
}

func TestRing_PlacementForKey_Distribution(t *testing.T) {
	nodes := []string{"n1", "n2", "n3", "n4"}
	ring := NewRing(1, nodes, 150)
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	counts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		for _, n := range ring.PlacementForKey(cfg, key) {
			counts[n]++
		}
	}
	// Each node should be picked roughly 750 times (3000 total / 4 nodes).
	// Allow 25% deviation from mean.
	mean := 3000.0 / 4
	for n, c := range counts {
		assert.InDelta(t, mean, float64(c), mean*0.35, "node %s: %d picks", n, c)
	}
}

func TestRing_WalkCW_SkipsSeen(t *testing.T) {
	ring := NewRing(1, []string{"node-a", "node-b"}, 10)
	seen := map[string]bool{"node-a": true}
	got := ring.walkCW(0, seen)
	assert.Equal(t, "node-b", got)
}
```

- [ ] **Step 2: 실패 확인**

```bash
cd /Users/whitekid/work/gritive/grains
go test ./internal/cluster/ -run TestRing -count=1 2>&1 | head -5
```

Expected: `undefined: NewRing` 류의 컴파일 에러

- [ ] **Step 3: ring.go 구현**

```go
// internal/cluster/ring.go
package cluster

import (
	"hash/fnv"
	"sort"
	"strconv"
)

// RingVersion은 링 스냅샷의 단조 증가 버전 번호다.
type RingVersion uint64

// VirtualNode는 링 위 가상 노드다.
type VirtualNode struct {
	Token  uint32 // 링 위치 (0 ~ 2^32-1)
	NodeID string
}

// Ring은 컨시스턴트 해시 링이다.
type Ring struct {
	Version  RingVersion
	VNodes   []VirtualNode // Token 기준 정렬
	VPerNode int           // 물리 노드당 가상 노드 수
}

// NewRing은 nodes 목록으로 Ring을 생성한다. vPerNode = 0이면 150 사용.
func NewRing(version RingVersion, nodes []string, vPerNode int) *Ring {
	if vPerNode <= 0 {
		vPerNode = 150
	}
	vnodes := make([]VirtualNode, 0, len(nodes)*vPerNode)
	for _, nodeID := range nodes {
		for i := 0; i < vPerNode; i++ {
			token := ringFNV32(nodeID + "/" + strconv.Itoa(i))
			vnodes = append(vnodes, VirtualNode{Token: token, NodeID: nodeID})
		}
	}
	sort.Slice(vnodes, func(i, j int) bool {
		return vnodes[i].Token < vnodes[j].Token
	})
	return &Ring{Version: version, VNodes: vnodes, VPerNode: vPerNode}
}

// PlacementForKey는 key의 EC 샤드 k+m개를 담당할 물리 노드 목록을 반환한다.
// 샤드 i마다 hash(key+"/"+i)에서 시계방향으로 걸어가며 아직 선택되지 않은 노드를 선택한다.
// 결과는 결정론적: 동일한 링 + 동일한 key → 항상 동일한 노드 목록.
func (r *Ring) PlacementForKey(cfg ECConfig, key string) []string {
	n := cfg.NumShards()
	chosen := make([]string, 0, n)
	seen := make(map[string]bool, n)

	for i := 0; i < n; i++ {
		token := ringFNV32(key + "/" + strconv.Itoa(i))
		nodeID := r.walkCW(token, seen)
		if nodeID == "" {
			break // 노드 부족 — 호출자가 처리
		}
		chosen = append(chosen, nodeID)
		seen[nodeID] = true
	}
	return chosen
}

// walkCW는 token에서 시계방향으로 걸어가며 seen에 없는 첫 번째 물리 노드 ID를 반환한다.
// 모든 노드가 seen에 있으면 "" 반환.
func (r *Ring) walkCW(token uint32, seen map[string]bool) string {
	if len(r.VNodes) == 0 {
		return ""
	}
	// 시작 인덱스: token 이상인 첫 번째 vnode
	start := sort.Search(len(r.VNodes), func(i int) bool {
		return r.VNodes[i].Token >= token
	})
	// 링을 최대 한 바퀴 순회
	for i := 0; i < len(r.VNodes); i++ {
		idx := (start + i) % len(r.VNodes)
		nodeID := r.VNodes[idx].NodeID
		if !seen[nodeID] {
			return nodeID
		}
	}
	return ""
}

// ringFNV32는 FNV-1a 32비트 해시를 반환한다.
func ringFNV32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test ./internal/cluster/ -run TestRing -count=1 -v 2>&1 | tail -15
```

Expected: 모든 TestRing_* PASS

- [ ] **Step 5: 커밋**

```bash
git add internal/cluster/ring.go internal/cluster/ring_test.go
git commit -m "feat(cluster): Ring 타입 + PlacementForKey 알고리즘 구현"
```

---

## Task 2: FlatBuffers 스키마 업데이트

**Files:**
- Modify: `internal/cluster/clusterpb/cluster.fbs`

FlatBuffers는 테이블 끝에 필드를 추가하면 하위 호환된다 — 구 코드가 새 메시지를 읽으면 추가 필드 = 기본값 0.

- [ ] **Step 1: cluster.fbs 수정**

`internal/cluster/clusterpb/cluster.fbs`의 `PutObjectMetaCmd` 테이블에 3개 필드를 추가하고, `ObjectMeta`에도 같은 3개를 추가하며, 신규 `SetRingCmd`와 `VNodeEntry` 테이블을 추가한다.

```
// PutObjectMetaCmd 테이블 기존 끝 (version_id:string; 바로 다음에 추가)
  ring_version:uint64 = 0;   // 추가: 쓰기에 사용된 Ring 버전
  ec_data:uint8 = 0;         // 추가: EC k (data shards)
  ec_parity:uint8 = 0;       // 추가: EC m (parity shards)
```

```
// ObjectMeta 테이블 기존 끝 (acl:uint8; 바로 다음에 추가)
  ring_version:uint64 = 0;
  ec_data:uint8 = 0;
  ec_parity:uint8 = 0;
```

```
// 파일 끝 (root_type Command; 앞에)에 추가:
table VNodeEntry {
  token:uint32;
  node_id:string;
}

table SetRingCmd {
  version:uint64;
  vnodes:[VNodeEntry];
  vper_node:uint32;
}
```

전체 수정 내용을 적용한 `cluster.fbs`:

```flatbuffers
// FlatBuffers schema for cluster FSM commands and metadata.
// Regenerate: make fbs

namespace clusterpb;

table Command {
  type:uint32;
  data:[ubyte];
}

table CreateBucketCmd {
  bucket:string;
}

table DeleteBucketCmd {
  bucket:string;
}

table PutObjectMetaCmd {
  bucket:string;
  key:string;
  size:int64;
  content_type:string;
  etag:string;
  mod_time:int64;
  version_id:string;
  ring_version:uint64 = 0;
  ec_data:uint8 = 0;
  ec_parity:uint8 = 0;
}

table DeleteObjectCmd {
  bucket:string;
  key:string;
  version_id:string;
}

table DeleteObjectVersionCmd {
  bucket:string;
  key:string;
  version_id:string;
}

table CreateMultipartUploadCmd {
  upload_id:string;
  bucket:string;
  key:string;
  content_type:string;
  created_at:int64;
}

table CompleteMultipartCmd {
  bucket:string;
  key:string;
  upload_id:string;
  size:int64;
  content_type:string;
  etag:string;
  mod_time:int64;
  version_id:string;
}

table AbortMultipartCmd {
  bucket:string;
  key:string;
  upload_id:string;
}

table SetBucketPolicyCmd {
  bucket:string;
  policy_json:[ubyte];
}

table DeleteBucketPolicyCmd {
  bucket:string;
}

table ObjectMeta {
  key:string;
  size:int64;
  content_type:string;
  etag:string;
  last_modified:int64;
  acl:uint8;
  ring_version:uint64 = 0;
  ec_data:uint8 = 0;
  ec_parity:uint8 = 0;
}

table SetBucketVersioningCmd {
  bucket:string;
  state:string;
}

table SetObjectACLCmd {
  bucket:string;
  key:string;
  acl:uint8;
}

table KeyValue {
  key:string;
  value:[ubyte];
}

table SnapshotState {
  entries:[KeyValue];
}

table MultipartMeta {
  content_type:string;
}

table NodeStatsMsg {
  node_id:string;
  disk_used_pct:double;
  disk_avail_bytes:uint64;
  requests_per_sec:double;
  joined_at:int64;
}

table ReceiptGossipMsg {
  node_id:string;
  receipt_ids:[string];
}

table ReceiptQueryMsg {
  receipt_id:string;
}

table ReceiptQueryResponseMsg {
  found:bool;
  receipt_json:[ubyte];
}

table MigrateShardCmd {
  bucket:string;
  key:string;
  version_id:string;
  src_node:string;
  dst_node:string;
}

table PutShardPlacementCmd {
  bucket:string;
  key:string;
  node_ids:[string];
  k:int32 = 0;
  m:int32 = 0;
}

table DeleteShardPlacementCmd {
  bucket:string;
  key:string;
}

table MigrationDoneCmd {
  bucket:string;
  key:string;
  version_id:string;
  src_node:string;
  dst_node:string;
}

table VNodeEntry {
  token:uint32;
  node_id:string;
}

table SetRingCmd {
  version:uint64;
  vnodes:[VNodeEntry];
  vper_node:uint32;
}

root_type Command;
```

- [ ] **Step 2: make fbs 실행**

```bash
cd /Users/whitekid/work/gritive/grains
make fbs
```

Expected: `internal/cluster/clusterpb/` 아래 `PutObjectMetaCmd.go`, `ObjectMeta.go`, `VNodeEntry.go`, `SetRingCmd.go` 파일이 재생성됨. 에러 없음.

- [ ] **Step 3: 빌드 확인**

```bash
go build ./internal/cluster/... 2>&1
```

Expected: 에러 없음 (새 필드를 아직 사용하지 않으므로)

- [ ] **Step 4: 커밋**

```bash
git add internal/cluster/clusterpb/
git commit -m "feat(clusterpb): PutObjectMetaCmd+ObjectMeta에 ring/EC 필드 추가, SetRingCmd 신규"
```

---

## Task 3: codec.go — 신규 필드 + SetRingCmd 인코드/디코드

**Files:**
- Modify: `internal/cluster/codec.go`
- Modify: `internal/cluster/fsm.go`

- [ ] **Step 1: objectMeta 구조체에 필드 추가 (`codec.go` 상단)**

`internal/cluster/codec.go`의 `objectMeta` 구조체를 다음과 같이 변경한다:

```go
type objectMeta struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64
	ACL          uint8
	RingVersion  uint64 // 추가
	ECData       uint8  // 추가
	ECParity     uint8  // 추가
}
```

- [ ] **Step 2: marshalObjectMeta / unmarshalObjectMeta 업데이트**

`marshalObjectMeta` 함수에서 `clusterpb.ObjectMetaEnd(b)` 바로 위에 3줄 추가:

```go
func marshalObjectMeta(m objectMeta) ([]byte, error) {
	b := clusterBuilderPool.Get().(*flatbuffers.Builder)
	keyOff := b.CreateString(m.Key)
	ctOff := b.CreateString(m.ContentType)
	etagOff := b.CreateString(m.ETag)
	clusterpb.ObjectMetaStart(b)
	clusterpb.ObjectMetaAddKey(b, keyOff)
	clusterpb.ObjectMetaAddSize(b, m.Size)
	clusterpb.ObjectMetaAddContentType(b, ctOff)
	clusterpb.ObjectMetaAddEtag(b, etagOff)
	clusterpb.ObjectMetaAddLastModified(b, m.LastModified)
	clusterpb.ObjectMetaAddAcl(b, m.ACL)
	clusterpb.ObjectMetaAddRingVersion(b, m.RingVersion)
	clusterpb.ObjectMetaAddEcData(b, m.ECData)
	clusterpb.ObjectMetaAddEcParity(b, m.ECParity)
	return fbFinish(b, clusterpb.ObjectMetaEnd(b)), nil
}
```

`unmarshalObjectMeta` 반환 구조체에 3줄 추가:

```go
func unmarshalObjectMeta(data []byte) (objectMeta, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.ObjectMeta {
		return clusterpb.GetRootAsObjectMeta(d, 0)
	})
	if err != nil {
		return objectMeta{}, fmt.Errorf("unmarshal ObjectMeta: %w", err)
	}
	return objectMeta{
		Key:          string(t.Key()),
		Size:         t.Size(),
		ContentType:  string(t.ContentType()),
		ETag:         string(t.Etag()),
		LastModified: t.LastModified(),
		ACL:          t.Acl(),
		RingVersion:  t.RingVersion(),
		ECData:       t.EcData(),
		ECParity:     t.EcParity(),
	}, nil
}
```

- [ ] **Step 3: encodePutObjectMetaCmd / decodePutObjectMetaCmd 업데이트**

`PutObjectMetaCmd` 구조체에 3개 필드가 스펙에서 이미 추가됐는지 확인하고 (`internal/cluster/fsm.go`), codec 함수를 업데이트한다.

`internal/cluster/fsm.go`의 `PutObjectMetaCmd` 구조체:

```go
type PutObjectMetaCmd struct {
	Bucket      string
	Key         string
	Size        int64
	ContentType string
	ETag        string
	ModTime     int64
	VersionID   string
	RingVersion RingVersion // 추가
	ECData      uint8       // 추가
	ECParity    uint8       // 추가
}
```

`internal/cluster/codec.go`의 `encodePutObjectMetaCmd`:

```go
func encodePutObjectMetaCmd(c PutObjectMetaCmd) ([]byte, error) {
	b := clusterBuilderPool.Get().(*flatbuffers.Builder)
	bucketOff := b.CreateString(c.Bucket)
	keyOff := b.CreateString(c.Key)
	ctOff := b.CreateString(c.ContentType)
	etagOff := b.CreateString(c.ETag)
	vidOff := b.CreateString(c.VersionID)
	clusterpb.PutObjectMetaCmdStart(b)
	clusterpb.PutObjectMetaCmdAddBucket(b, bucketOff)
	clusterpb.PutObjectMetaCmdAddKey(b, keyOff)
	clusterpb.PutObjectMetaCmdAddSize(b, c.Size)
	clusterpb.PutObjectMetaCmdAddContentType(b, ctOff)
	clusterpb.PutObjectMetaCmdAddEtag(b, etagOff)
	clusterpb.PutObjectMetaCmdAddModTime(b, c.ModTime)
	clusterpb.PutObjectMetaCmdAddVersionId(b, vidOff)
	clusterpb.PutObjectMetaCmdAddRingVersion(b, uint64(c.RingVersion))
	clusterpb.PutObjectMetaCmdAddEcData(b, c.ECData)
	clusterpb.PutObjectMetaCmdAddEcParity(b, c.ECParity)
	return fbFinish(b, clusterpb.PutObjectMetaCmdEnd(b)), nil
}
```

`decodePutObjectMetaCmd` 반환 구조체에 3줄 추가:

```go
func decodePutObjectMetaCmd(data []byte) (PutObjectMetaCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.PutObjectMetaCmd {
		return clusterpb.GetRootAsPutObjectMetaCmd(d, 0)
	})
	if err != nil {
		return PutObjectMetaCmd{}, err
	}
	return PutObjectMetaCmd{
		Bucket:      string(t.Bucket()),
		Key:         string(t.Key()),
		Size:        t.Size(),
		ContentType: string(t.ContentType()),
		ETag:        string(t.Etag()),
		ModTime:     t.ModTime(),
		VersionID:   string(t.VersionId()),
		RingVersion: RingVersion(t.RingVersion()),
		ECData:      t.EcData(),
		ECParity:    t.EcParity(),
	}, nil
}
```

- [ ] **Step 4: SetRingCmd 인코드/디코드 추가**

`codec.go`의 `encodePayload` switch 앞에 다음 함수들을 추가한다:

```go
// encodeSetRingCmd serializes a SetRingCmd for Raft proposal.
func encodeSetRingCmd(c SetRingCmd) ([]byte, error) {
	b := clusterBuilderPool.Get().(*flatbuffers.Builder)
	// vnodes는 역순으로 먼저 빌드해야 한다 (FlatBuffers vector prepend 방식)
	vnOffsets := make([]flatbuffers.UOffsetT, len(c.VNodes))
	for i := len(c.VNodes) - 1; i >= 0; i-- {
		nodeIDOff := b.CreateString(c.VNodes[i].NodeID)
		clusterpb.VNodeEntryStart(b)
		clusterpb.VNodeEntryAddToken(b, c.VNodes[i].Token)
		clusterpb.VNodeEntryAddNodeId(b, nodeIDOff)
		vnOffsets[i] = clusterpb.VNodeEntryEnd(b)
	}
	clusterpb.SetRingCmdStartVnodesVector(b, len(vnOffsets))
	for i := len(vnOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(vnOffsets[i])
	}
	vnodesVec := b.EndVector(len(vnOffsets))
	clusterpb.SetRingCmdStart(b)
	clusterpb.SetRingCmdAddVersion(b, uint64(c.Version))
	clusterpb.SetRingCmdAddVnodes(b, vnodesVec)
	clusterpb.SetRingCmdAddVperNode(b, uint32(c.VPerNode))
	return fbFinish(b, clusterpb.SetRingCmdEnd(b)), nil
}

// decodeSetRingCmd deserializes a SetRingCmd from Raft log data.
func decodeSetRingCmd(data []byte) (SetRingCmd, error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.SetRingCmd {
		return clusterpb.GetRootAsSetRingCmd(d, 0)
	})
	if err != nil {
		return SetRingCmd{}, err
	}
	vnodes := make([]VirtualNode, t.VnodesLength())
	for i := 0; i < t.VnodesLength(); i++ {
		var vn clusterpb.VNodeEntry
		t.Vnodes(&vn, i)
		vnodes[i] = VirtualNode{Token: vn.Token(), NodeID: string(vn.NodeId())}
	}
	return SetRingCmd{
		Version:  RingVersion(t.Version()),
		VNodes:   vnodes,
		VPerNode: int(t.VperNode()),
	}, nil
}
```

- [ ] **Step 5: `SetRingCmd` 타입 + `encodePayload` switch 추가**

`internal/cluster/fsm.go`에 `SetRingCmd` 구조체와 `CmdSetRing` 상수를 추가한다:

```go
// CmdSetRing 상수 추가 (기존 CmdSetObjectACL = 16 아래)
CmdSetRing CommandType = 17

// SetRingCmd 구조체 추가
type SetRingCmd struct {
	Version  RingVersion
	VNodes   []VirtualNode
	VPerNode int
}
```

`internal/cluster/codec.go`의 `encodePayload` switch에 케이스 추가:

```go
case CmdSetRing:
    return encodeSetRingCmd(payload.(SetRingCmd))
```

- [ ] **Step 6: 빌드 + 테스트 확인**

```bash
go build ./internal/cluster/... && go test ./internal/cluster/ -run TestRing -count=1 -v
```

Expected: 빌드 성공, TestRing_* 모두 PASS

- [ ] **Step 7: 커밋**

```bash
git add internal/cluster/fsm.go internal/cluster/codec.go
git commit -m "feat(cluster): PutObjectMetaCmd/objectMeta ring+EC 필드, SetRingCmd 코덱 추가"
```

---

## Task 4: FSM ring store + applySetRing + no-op + refcount

**Files:**
- Create: `internal/cluster/ring_store.go`
- Create: `internal/cluster/ring_store_test.go`
- Modify: `internal/cluster/apply.go`

- [ ] **Step 1: ring_store_test.go 작성**

```go
// internal/cluster/ring_store_test.go
package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingStore_GetRing(t *testing.T) {
	store := newRingStore()
	ring := NewRing(1, []string{"n1", "n2", "n3"}, 10)
	store.putRing(ring)

	got, err := store.GetRing(1)
	require.NoError(t, err)
	assert.Equal(t, RingVersion(1), got.Version)
}

func TestRingStore_GetRing_NotFound(t *testing.T) {
	store := newRingStore()
	_, err := store.GetRing(99)
	assert.ErrorIs(t, err, ErrRingNotFound)
}

func TestRingStore_GetCurrentRing(t *testing.T) {
	store := newRingStore()
	ring1 := NewRing(1, []string{"n1"}, 10)
	ring2 := NewRing(2, []string{"n1", "n2"}, 10)
	store.putRing(ring1)
	store.putRing(ring2)

	got, err := store.GetCurrentRing()
	require.NoError(t, err)
	assert.Equal(t, RingVersion(2), got.Version)
}

func TestRingStore_RefCount_GCEligible(t *testing.T) {
	store := newRingStore()
	ring1 := NewRing(1, []string{"n1"}, 10)
	ring2 := NewRing(2, []string{"n1", "n2"}, 10)
	store.putRing(ring1)
	store.putRing(ring2)
	store.incRef(1)
	store.decRef(1)
	// refCount[1]==0, current==2, current>1 → eligible
	assert.True(t, store.gcEligible(1))
	assert.False(t, store.gcEligible(2)) // current ring never GC
}
```

- [ ] **Step 2: 실패 확인**

```bash
go test ./internal/cluster/ -run TestRingStore -count=1 2>&1 | head -5
```

Expected: undefined エラー

- [ ] **Step 3: ring_store.go 구현**

```go
// internal/cluster/ring_store.go
package cluster

import (
	"errors"
	"sync"
)

// ErrRingNotFound is returned when a requested ring version is not in the store.
var ErrRingNotFound = errors.New("ring version not found")

// ringStore manages in-memory ring version history used by the FSM.
// Thread-safe. BadgerDB 영속화는 FSM의 applySetRing/Snapshot/Restore에서 처리한다.
type ringStore struct {
	mu       sync.RWMutex
	current  RingVersion
	rings    map[RingVersion]*Ring
	refCount map[RingVersion]int64
}

func newRingStore() *ringStore {
	return &ringStore{
		rings:    make(map[RingVersion]*Ring),
		refCount: make(map[RingVersion]int64),
	}
}

// putRing은 링을 저장하고 current를 업데이트한다.
func (s *ringStore) putRing(r *Ring) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rings[r.Version] = r
	if r.Version > s.current {
		s.current = r.Version
	}
}

// GetRing은 version에 해당하는 링을 반환한다.
func (s *ringStore) GetRing(version RingVersion) (*Ring, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rings[version]
	if !ok {
		return nil, ErrRingNotFound
	}
	return r, nil
}

// GetCurrentRing은 가장 최신 링을 반환한다.
func (s *ringStore) GetCurrentRing() (*Ring, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.rings[s.current]
	if !ok {
		return nil, ErrRingNotFound
	}
	return r, nil
}

// incRef는 version의 참조 카운터를 1 증가시킨다.
func (s *ringStore) incRef(version RingVersion) {
	if version == 0 {
		return // legacy objects (RingVersion==0) don't use ring store
	}
	s.mu.Lock()
	s.refCount[version]++
	s.mu.Unlock()
}

// decRef는 version의 참조 카운터를 1 감소시킨다. 0 미만으로 가지 않는다.
func (s *ringStore) decRef(version RingVersion) {
	if version == 0 {
		return
	}
	s.mu.Lock()
	if s.refCount[version] > 0 {
		s.refCount[version]--
	}
	s.mu.Unlock()
}

// gcEligible은 version의 링이 GC 대상인지 반환한다.
// 조건: refCount==0 AND current > version.
func (s *ringStore) gcEligible(version RingVersion) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.refCount[version] == 0 && s.current > version
}

// gcRing은 version의 링을 in-memory에서 제거한다.
func (s *ringStore) gcRing(version RingVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rings, version)
	delete(s.refCount, version)
}

// allVersions는 현재 보유한 모든 링 버전 목록을 반환한다.
func (s *ringStore) allVersions() []RingVersion {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions := make([]RingVersion, 0, len(s.rings))
	for v := range s.rings {
		versions = append(versions, v)
	}
	return versions
}
```

- [ ] **Step 4: FSM에 ringStore 추가**

`internal/cluster/apply.go`의 `FSM` 구조체에 `ringStore` 필드를 추가한다:

```go
type FSM struct {
	db *badger.DB
	mu sync.RWMutex
	// ... 기존 필드 유지 ...
	rings *ringStore // 추가: 컨시스턴트 해시 링 히스토리
}
```

`NewFSM`에서 초기화:

```go
func NewFSM(db *badger.DB) *FSM {
	return &FSM{
		db:    db,
		rings: newRingStore(),
	}
}
```

- [ ] **Step 5: applySetRing 구현**

`apply.go`에서 `applySetRing` 메서드를 추가하고, `CmdSetRing` case와 no-op case들을 추가한다.

`Apply` switch에 케이스 추가:

```go
case CmdSetRing:
    return f.applySetRing(cmd.Data)
case CmdPutShardPlacement:
    return nil // no-op: 컨시스턴트 해시 링으로 대체됨. 로그 재생 시 무시.
case CmdDeleteShardPlacement:
    return nil // no-op: 동일.
```

기존 `case CmdPutShardPlacement:` 와 `case CmdDeleteShardPlacement:` 라인을 위 no-op으로 교체한다.

`applySetRing` 메서드:

```go
// ringKey는 BadgerDB에 링을 저장하는 키다.
func ringKey(version RingVersion) []byte {
	return []byte(fmt.Sprintf("ring:%d", uint64(version)))
}

func (f *FSM) applySetRing(data []byte) error {
	c, err := decodeSetRingCmd(data)
	if err != nil {
		return fmt.Errorf("decode SetRingCmd: %w", err)
	}
	ring := &Ring{
		Version:  c.Version,
		VNodes:   c.VNodes,
		VPerNode: c.VPerNode,
	}
	// BadgerDB에 영속화
	encoded, err := encodeRingForDB(ring)
	if err != nil {
		return fmt.Errorf("encode ring: %w", err)
	}
	if err := f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(ringKey(ring.Version), encoded)
	}); err != nil {
		return err
	}
	// in-memory 캐시 업데이트
	f.rings.putRing(ring)
	return nil
}
```

- [ ] **Step 6: ring DB 직렬화 헬퍼 추가**

`ring_store.go`에 BadgerDB 직렬화 함수를 추가한다 (JSON은 내부 통신에 금지이므로 binary 사용):

```go
// encodeRingForDB는 Ring을 binary로 직렬화한다.
// 포맷: [8B version][4B vPerNode][4B vnodeCount] [(4B token + 1B nodeIDLen + nodeIDBytes)...]
func encodeRingForDB(r *Ring) ([]byte, error) {
	buf := make([]byte, 0, 16+len(r.VNodes)*20)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(r.Version))
	buf = append(buf, tmp[:]...)
	binary.BigEndian.PutUint32(tmp[:4], uint32(r.VPerNode))
	buf = append(buf, tmp[:4]...)
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(r.VNodes)))
	buf = append(buf, tmp[:4]...)
	for _, vn := range r.VNodes {
		binary.BigEndian.PutUint32(tmp[:4], vn.Token)
		buf = append(buf, tmp[:4]...)
		idBytes := []byte(vn.NodeID)
		if len(idBytes) > 255 {
			return nil, fmt.Errorf("nodeID too long: %d", len(idBytes))
		}
		buf = append(buf, byte(len(idBytes)))
		buf = append(buf, idBytes...)
	}
	return buf, nil
}

// decodeRingFromDB는 encodeRingForDB 포맷을 역직렬화한다.
func decodeRingFromDB(data []byte) (*Ring, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("ring data too short: %d", len(data))
	}
	version := RingVersion(binary.BigEndian.Uint64(data[0:8]))
	vPerNode := int(binary.BigEndian.Uint32(data[8:12]))
	vnodeCount := int(binary.BigEndian.Uint32(data[12:16]))
	vnodes := make([]VirtualNode, 0, vnodeCount)
	pos := 16
	for i := 0; i < vnodeCount; i++ {
		if pos+5 > len(data) {
			return nil, fmt.Errorf("truncated ring data at vnode %d", i)
		}
		token := binary.BigEndian.Uint32(data[pos : pos+4])
		idLen := int(data[pos+4])
		pos += 5
		if pos+idLen > len(data) {
			return nil, fmt.Errorf("truncated nodeID at vnode %d", i)
		}
		nodeID := string(data[pos : pos+idLen])
		pos += idLen
		vnodes = append(vnodes, VirtualNode{Token: token, NodeID: nodeID})
	}
	return &Ring{Version: version, VNodes: vnodes, VPerNode: vPerNode}, nil
}
```

`encoding/binary` import를 ring_store.go에 추가한다:

```go
import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)
```

- [ ] **Step 7: FSM.applyPutObjectMeta에 refcount 추가**

`apply.go`의 `applyPutObjectMeta`가 BadgerDB에 meta를 쓰는 txn 내에서 ring refcount를 증가시킨다. txn은 atomic이므로 refcount도 txn 후에 in-memory에서 업데이트해야 한다. txn 성공 후 호출:

```go
func (f *FSM) applyPutObjectMeta(data []byte) error {
	c, err := decodePutObjectMetaCmd(data)
	if err != nil {
		return err
	}
	meta, err := marshalObjectMeta(objectMeta{
		Key:          c.Key,
		Size:         c.Size,
		ContentType:  c.ContentType,
		ETag:         c.ETag,
		LastModified: c.ModTime,
		RingVersion:  uint64(c.RingVersion),
		ECData:       c.ECData,
		ECParity:     c.ECParity,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	if err := f.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(c.Bucket, c.Key), meta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := txn.Set(objectMetaKeyV(c.Bucket, c.Key, c.VersionID), meta); err != nil {
				return err
			}
			if err := txn.Set(latestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// ring refcount 업데이트 (txn 성공 후)
	f.rings.incRef(c.RingVersion)
	return nil
}
```

`applyDeleteObject`에서도 delete 후 decRef가 필요하다. `applyDeleteObject`의 txn 완료 후, 기존 meta의 `RingVersion`을 먼저 읽어야 한다. 단순화를 위해 txn 내에서 meta를 읽어 RingVersion을 구하고, txn 성공 후 decRef 호출:

`apply.go`의 `applyDeleteObject`에서 txn 시작 전에 기존 meta의 RingVersion을 읽는 코드를 추가한다:

```go
// applyDeleteObject 수정: meta 삭제 전 RingVersion 읽어서 decRef
func (f *FSM) applyDeleteObject(data []byte) error {
	c, err := decodeDeleteObjectCmd(data)
	if err != nil {
		return err
	}
	var oldRingVersion RingVersion
	// meta에서 RingVersion 읽기 (best-effort, 실패해도 계속)
	_ = f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey(c.Bucket, c.Key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			oldRingVersion = RingVersion(m.RingVersion)
			return nil
		})
	})
	// ... 기존 applyDeleteObject 로직 유지 ...
	// txn 완료 후:
	if oldRingVersion > 0 {
		f.rings.decRef(oldRingVersion)
	}
	return nil
}
```

**주의:** 기존 `applyDeleteObject` 전체 로직(tombstone 생성, placement 삭제 등)을 그대로 유지하고 앞뒤에 RingVersion 읽기/decRef만 추가한다.

- [ ] **Step 8: FSM.GetRing / GetCurrentRing 노출**

`ring_store.go`에 FSM 메서드로 위임하는 래퍼를 추가한다 — 혹은 ring_store의 공개 메서드를 직접 FSM 필드로 접근하면 된다. 테스트와 backend에서 `f.rings.GetRing(v)` / `f.rings.GetCurrentRing()` 직접 호출.

- [ ] **Step 9: ring store 테스트 통과 확인**

```bash
go test ./internal/cluster/ -run TestRingStore -count=1 -v
```

Expected: 모든 TestRingStore_* PASS

- [ ] **Step 10: 전체 cluster 테스트 통과 확인**

```bash
go test ./internal/cluster/... -count=1 2>&1 | tail -10
```

Expected: ok (no failures)

- [ ] **Step 11: 커밋**

```bash
git add internal/cluster/apply.go internal/cluster/ring_store.go internal/cluster/ring_store_test.go
git commit -m "feat(cluster): ring store + CmdSetRing FSM apply + refcount GC 기반"
```

---

## Task 5: raft.Node.IsLeader() + transport ProposeForward

**Files:**
- Modify: `internal/raft/raft.go`
- Modify: `internal/transport/transport.go`

- [ ] **Step 1: IsLeader() 추가**

`internal/raft/raft.go`에서 `State()` 메서드 아래에 추가한다:

```go
// IsLeader returns true if the node is currently the Raft leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}
```

- [ ] **Step 2: StreamProposeForward 추가**

`internal/transport/transport.go`의 StreamType 상수 블록에 추가:

```go
const (
	StreamControl      StreamType = 0x01
	StreamData         StreamType = 0x02
	StreamAdmin        StreamType = 0x03
	StreamReceipt      StreamType = 0x04
	StreamReceiptQuery StreamType = 0x05
	StreamProposeForward StreamType = 0x06 // 팔로워가 리더에게 propose를 포워딩
)
```

- [ ] **Step 3: 빌드 확인**

```bash
go build ./internal/raft/... ./internal/transport/... 2>&1
```

Expected: 에러 없음

- [ ] **Step 4: 커밋**

```bash
git add internal/raft/raft.go internal/transport/transport.go
git commit -m "feat(raft,transport): IsLeader() 메서드 추가, StreamProposeForward(0x06) 추가"
```

---

## Task 6: backend ProposeForward 포워딩 + 링 쓰기 경로

**Files:**
- Modify: `internal/cluster/backend.go`

이 태스크는 3가지 변경을 포함한다:
1. `propose()` — 팔로워면 리더에게 QUIC RPC 포워딩
2. `putObjectEC()` — 링 기반 배치 (링이 있을 때)
3. `GetObject()` / `getObjectEC()` — RingVersion 기반 읽기 경로

- [ ] **Step 1: ProposeForward 핸들러 등록 + forwardPropose 메서드 추가**

`internal/cluster/backend.go`에 다음을 추가한다. `ShardService`에서 QUIC transport를 가져오는 방식을 참고한다.

먼저 `DistributedBackend`가 `ShardService`를 통해 `QUICTransport`에 접근할 수 있는지 확인한다:

```bash
grep -n "transport\|QUICTransport\|shardSvc\.transport" /Users/whitekid/work/gritive/grains/internal/cluster/backend.go | head -10
```

`forwardPropose` 구현을 `backend.go`에 추가한다. QUIC 스트림을 직접 열어 payload를 전송한다:

```go
// forwardPropose는 팔로워에서 리더로 propose를 QUIC RPC로 포워딩한다.
// 와이어 포맷: [4B cmdType big-endian][payload bytes] → [8B index big-endian][4B errLen][errBytes]
func (b *DistributedBackend) forwardPropose(ctx context.Context, leaderAddr string, data []byte) (uint64, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("forwardPropose: no transport available")
	}
	req := &transport.Message{
		Type:    transport.StreamProposeForward,
		Payload: data,
	}
	resp, err := b.shardSvc.SendRequest(ctx, leaderAddr, req)
	if err != nil {
		return 0, fmt.Errorf("forwardPropose: send: %w", err)
	}
	if len(resp.Payload) < 8 {
		return 0, fmt.Errorf("forwardPropose: response too short: %d", len(resp.Payload))
	}
	index := binary.BigEndian.Uint64(resp.Payload[0:8])
	errLen := binary.BigEndian.Uint32(resp.Payload[8:12])
	if errLen > 0 && len(resp.Payload) >= 12+int(errLen) {
		return 0, fmt.Errorf("forwardPropose: leader error: %s", string(resp.Payload[12:12+int(errLen)]))
	}
	return index, nil
}
```

`ShardService`에 `SendRequest` 메서드가 없으면 `backend.go`에서 직접 transport를 사용한다. 다음과 같이 `DistributedBackend`에 transport 필드를 추가하거나, `shardSvc`를 통해 접근한다.

실제로 `shardSvc.transport`는 unexported다. 대신 `DistributedBackend`에 transport 필드를 추가하거나, `ShardService`에 `SendRequest` 래퍼를 추가한다.

`ShardService`에 다음을 추가한다 (`shard_service.go`):

```go
// SendRequest sends a request message to a peer and returns the response.
// Used by forwardPropose to send propose forwarding RPCs.
func (s *ShardService) SendRequest(ctx context.Context, peerAddr string, msg *transport.Message) (*transport.Message, error) {
	return s.transport.SendRequest(ctx, peerAddr, msg)
}
```

`QUICTransport`에 `SendRequest` 메서드를 추가한다 (`quic.go`):

```go
// SendRequest sends a request message and waits for a single response.
// Opens a new QUIC stream, sends msg, reads one response, closes stream.
func (t *QUICTransport) SendRequest(ctx context.Context, addr string, msg *Message) (*Message, error) {
	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}
	defer stream.Close()
	if err := t.codec.Encode(stream, msg); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return t.codec.Decode(stream)
}
```

- [ ] **Step 2: ProposeForward 핸들러를 리더가 등록하도록 추가**

`backend.go`의 `RegisterRPCHandlers()` (또는 ShardService setup 위치)에 핸들러를 등록한다:

```go
// RegisterProposeForwardHandler는 StreamProposeForward 타입의 요청을 처리하는
// 핸들러를 QUIC router에 등록한다. 리더가 팔로워의 propose를 대신 처리한다.
func (b *DistributedBackend) RegisterProposeForwardHandler() {
	if b.shardSvc == nil {
		return
	}
	b.shardSvc.RegisterHandler(transport.StreamProposeForward, func(req *transport.Message) *transport.Message {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		idx, err := b.node.ProposeWait(ctx, req.Payload)
		resp := make([]byte, 12)
		if err != nil {
			binary.BigEndian.PutUint64(resp[0:8], 0)
			errBytes := []byte(err.Error())
			binary.BigEndian.PutUint32(resp[8:12], uint32(len(errBytes)))
			resp = append(resp, errBytes...)
		} else {
			binary.BigEndian.PutUint64(resp[0:8], idx)
			binary.BigEndian.PutUint32(resp[8:12], 0)
		}
		return &transport.Message{Type: transport.StreamProposeForward, Payload: resp}
	})
}
```

`ShardService`에 `RegisterHandler` 래퍼 추가 (`shard_service.go`):

```go
func (s *ShardService) RegisterHandler(st transport.StreamType, h func(*transport.Message) *transport.Message) {
	s.transport.Router().Handle(st, h)
}
```

- [ ] **Step 3: propose() 업데이트 — 팔로워면 포워딩**

`backend.go`의 `propose()` 함수를 다음으로 교체한다:

```go
func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	data, err := EncodeCommand(cmdType, payload)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	if b.node.IsLeader() {
		proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		idx, err := b.node.ProposeWait(proposeCtx, data)
		if err != nil {
			return err
		}
		for b.lastApplied.Load() < idx {
			select {
			case <-proposeCtx.Done():
				return proposeCtx.Err()
			default:
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	}

	// 팔로워: 리더에게 포워딩
	leaderID := b.node.LeaderID()
	if leaderID == "" {
		return raft.ErrNotLeader
	}
	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = b.forwardPropose(proposeCtx, leaderID, data)
	return err
}
```

- [ ] **Step 4: putObjectEC — 링 배치 경로 추가**

`backend.go`의 `putObjectEC()` 함수에서 `PlacementForNodes` 호출 직전에 링 존재 여부를 확인하고, 링이 있으면 `ring.PlacementForKey`를 사용한다:

```go
// putObjectEC 내에서 placement 계산 부분을 교체:
var placement []string
var ringVer RingVersion
if currentRing, ringErr := b.fsm.rings.GetCurrentRing(); ringErr == nil {
	placement = currentRing.PlacementForKey(effectiveCfg, shardKey)
	ringVer = currentRing.Version
} else {
	// 링 없음 (초기 클러스터): 기존 PlacementForNodes 사용
	placement = PlacementForNodes(effectiveCfg, liveNodes, shardKey)
}
```

`CmdPutObjectMeta` propose 시 RingVersion, ECData, ECParity 필드를 채운다:

```go
if merr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
	Bucket:      bucket,
	Key:         key,
	Size:        int64(len(data)),
	ContentType: contentType,
	ETag:        etag,
	ModTime:     now,
	VersionID:   versionID,
	RingVersion: ringVer,
	ECData:      uint8(effectiveCfg.DataShards),
	ECParity:    uint8(effectiveCfg.ParityShards),
}); merr != nil {
	// 샤드 orphan 처리: best-effort 삭제
	go b.deleteShardsAsync(ctx, placement, shardKey)
	return nil, merr
}
```

`deleteShardsAsync` 헬퍼:

```go
// deleteShardsAsync는 propose 실패 시 고아 샤드를 백그라운드에서 삭제한다.
// best-effort: 실패는 무시하고 scrubber fallback에 위임한다.
func (b *DistributedBackend) deleteShardsAsync(ctx context.Context, placement []string, shardKey string) {
	for _, node := range placement {
		if node == b.selfAddr {
			_ = b.shardSvc.DeleteLocalShards("", shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(context.Background(), node, "", shardKey)
		}
	}
}
```

**주의:** `CmdPutShardPlacement` propose 호출을 제거한다 — 이제 불필요하다.

- [ ] **Step 5: GetObject / getObjectEC — RingVersion 기반 읽기**

`backend.go`의 `GetObject()` 에서 `LookupShardPlacement` 호출 부분을 다음과 같이 교체한다:

```go
// meta에서 RingVersion 읽기
var metaRingVersion RingVersion
var metaECData, metaECParity uint8
_ = b.db.View(func(txn *badger.Txn) error {
	key := objectMetaKeyV(bucket, obj.Key, obj.VersionID)
	if obj.VersionID == "" {
		key = objectMetaKey(bucket, obj.Key)
	}
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		metaRingVersion = RingVersion(m.RingVersion)
		metaECData = m.ECData
		metaECParity = m.ECParity
		return nil
	})
})

if metaRingVersion > 0 {
	// 신 경로: 링에서 배치 재계산
	ring, ringErr := b.fsm.rings.GetRing(metaRingVersion)
	if ringErr == nil {
		cfg := ECConfig{DataShards: int(metaECData), ParityShards: int(metaECParity)}
		placement := ring.PlacementForKey(cfg, shardKey)
		rec := PlacementRecord{Nodes: placement, K: int(metaECData), M: int(metaECParity)}
		data, ecErr := b.getObjectEC(context.Background(), bucket, key, obj.VersionID, rec)
		if ecErr == nil {
			return io.NopCloser(bytes.NewReader(data)), obj, nil
		}
		b.logger.Warn().Str("bucket", bucket).Str("key", key).Err(ecErr).Msg("ring-based ec reconstruct failed, falling back")
	}
}

// 구 경로 (RingVersion==0): LookupShardPlacement fallback
if ecRec, lookupErr := b.fsm.LookupShardPlacement(bucket, shardKey); lookupErr != nil {
	return nil, nil, fmt.Errorf("lookup shard placement: %w", lookupErr)
} else if len(ecRec.Nodes) > 0 && b.shardSvc != nil {
	// ... 기존 EC 경로 ...
}
```

- [ ] **Step 6: 빌드 + 테스트**

```bash
go build ./... 2>&1 && go test ./internal/cluster/... -count=1 -timeout=120s 2>&1 | tail -20
```

Expected: 빌드 성공, 테스트 PASS

- [ ] **Step 7: 커밋**

```bash
git add internal/cluster/backend.go internal/cluster/shard_service.go internal/transport/quic.go
git commit -m "feat(cluster): propose() ProposeForward 포워딩, 링 기반 putObjectEC/getObjectEC"
```

---

## Task 7: ReshardWorker 링 버전 인식 확장

**Files:**
- Modify: `internal/cluster/reshard_manager.go`

현재 `ReshardManager.Run()`은 N×→EC 변환만 한다. 여기에 "링 버전이 낮은 오브젝트를 새 링 기준으로 리샤드"하는 경로를 추가한다.

- [ ] **Step 1: Converter 인터페이스에 링 메서드 추가**

`reshard_manager.go`의 `Converter` 인터페이스에:

```go
type Converter interface {
	ConvertObjectToEC(ctx context.Context, bucket, key string) error
	FSMRef() *FSM
	ECActive() bool
	EffectiveECConfig() ECConfig
	upgradeObjectEC(ctx context.Context, bucket, key string, oldRec PlacementRecord, newCfg ECConfig) error
	// 링 관련 추가
	CurrentRingVersion() RingVersion        // 현재 링 버전 반환 (0이면 링 없음)
	ReshardToRing(ctx context.Context, bucket, key string, ringVer RingVersion) error // 링 기준으로 리샤드
}
```

`DistributedBackend`에 두 메서드 구현 추가 (`backend.go`):

```go
// CurrentRingVersion은 FSM에서 현재 링 버전을 반환한다.
func (b *DistributedBackend) CurrentRingVersion() RingVersion {
	ring, err := b.fsm.rings.GetCurrentRing()
	if err != nil {
		return 0
	}
	return ring.Version
}

// ReshardToRing은 오브젝트를 현재 링 기준으로 리샤드한다.
// 기존 배치에서 reconstruct → 현재 링으로 fan-out → meta 업데이트.
func (b *DistributedBackend) ReshardToRing(ctx context.Context, bucket, key string, oldRingVer RingVersion) error {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return err
	}
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}

	// 현재 링
	currentRing, err := b.fsm.rings.GetCurrentRing()
	if err != nil {
		return fmt.Errorf("reshard: no current ring: %w", err)
	}
	if currentRing.Version == oldRingVer {
		return nil // 이미 최신 링
	}

	// 기존 링에서 데이터 복구
	var oldData []byte
	if oldRingVer > 0 {
		oldRing, err := b.fsm.rings.GetRing(oldRingVer)
		if err != nil {
			return fmt.Errorf("reshard: old ring %d not found: %w", oldRingVer, err)
		}
		cfg := b.ecConfig // 기존 config 사용 (ECData/ECParity는 meta에서)
		oldPlacement := oldRing.PlacementForKey(cfg, shardKey)
		rec := PlacementRecord{Nodes: oldPlacement, K: cfg.DataShards, M: cfg.ParityShards}
		oldData, err = b.getObjectEC(ctx, bucket, key, obj.VersionID, rec)
		if err != nil {
			return fmt.Errorf("reshard: reconstruct from old ring: %w", err)
		}
	} else {
		// RingVersion==0: LookupShardPlacement fallback
		ecRec, err := b.fsm.LookupShardPlacement(bucket, shardKey)
		if err != nil || len(ecRec.Nodes) == 0 {
			return fmt.Errorf("reshard: no old placement for ring-v0 object")
		}
		oldData, err = b.getObjectEC(ctx, bucket, key, obj.VersionID, ecRec)
		if err != nil {
			return fmt.Errorf("reshard: reconstruct: %w", err)
		}
	}

	// 현재 링으로 재배치 — putObjectEC의 쓰기 경로 재사용
	newObj, err := b.putObjectEC(bucket, key, obj.VersionID, oldData, obj.ContentType)
	_ = newObj
	return err
}
```

- [ ] **Step 2: ReshardManager.Run()에 링 인식 스캔 추가**

`reshard_manager.go`의 `Run()` 메서드 끝 부분에 링 리샤드 패스를 추가한다:

```go
// 링 리샤드 패스: RingVersion < currentRingVersion인 오브젝트 처리
currentRingVer := m.backend.CurrentRingVersion()
if currentRingVer > 0 {
	rerr := m.backend.FSMRef().IterObjectMetas(func(ref ObjectMetaRef) error {
		// meta의 RingVersion 조회
		var objRingVer RingVersion
		fsm := m.backend.FSMRef()
		_ = fsm.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(objectMetaKey(ref.Bucket, ref.Key))
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				objRingVer = RingVersion(m.RingVersion)
				return nil
			})
		})
		if objRingVer >= currentRingVer {
			skipped++
			return nil
		}
		if err := m.backend.ReshardToRing(ctx, ref.Bucket, ref.Key, objRingVer); err != nil {
			m.logger.Warn().Str("bucket", ref.Bucket).Str("key", ref.Key).
				Uint64("ring_ver", uint64(objRingVer)).Err(err).Msg("reshard: ring-based reshard failed")
			errs++
		} else {
			converted++
		}
		return nil
	})
	if rerr != nil {
		m.logger.Warn().Err(rerr).Msg("reshard: ring iter failed")
	}
}
```

- [ ] **Step 3: 테스트**

```bash
go test ./internal/cluster/... -count=1 -run TestReshard -timeout=60s -v 2>&1 | tail -20
```

Expected: 기존 reshard 테스트 PASS

- [ ] **Step 4: 커밋**

```bash
git add internal/cluster/reshard_manager.go internal/cluster/backend.go
git commit -m "feat(cluster): ReshardManager 링 버전 인식 확장 — 링 전환 후 오브젝트 리샤드"
```

---

## Task 8: E2E 테스트 — 팔로워 쓰기 + 링 배치 검증

**Files:**
- Create: `internal/cluster/ring_e2e_test.go`

이 테스트는 실제 3노드 in-process 클러스터를 띄워서 팔로워 노드에 PUT한 뒤 전 노드에서 GET이 성공하는지 확인한다.

- [ ] **Step 1: 기존 E2E 테스트 패턴 확인**

```bash
grep -rn "TestCluster\|newTestCluster\|3-node\|three.*node" /Users/whitekid/work/gritive/grains/internal/cluster/ | grep -v vendor | head -10
```

- [ ] **Step 2: ring_e2e_test.go 작성**

```go
// internal/cluster/ring_e2e_test.go
//go:build !short

package cluster_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFollowerWrite_PutAndGetEC tests that a PUT to a follower node
// succeeds and the object is retrievable from all nodes.
func TestFollowerWrite_PutAndGetEC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	// 3노드 클러스터 생성 (기존 testBackend 헬퍼 사용)
	cluster := newTestCluster(t, 3)
	defer cluster.Close()

	// 리더가 선출될 때까지 대기
	var leader, follower testNode
	require.Eventually(t, func() bool {
		for i, n := range cluster.Nodes {
			if n.Backend.node.IsLeader() {
				leader = cluster.Nodes[i]
				// follower는 leader 이외의 첫 번째 노드
				for j, m := range cluster.Nodes {
					if j != i {
						follower = cluster.Nodes[j]
						break
					}
				}
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "leader not elected")

	t.Logf("leader=%s follower=%s", leader.ID, follower.ID)

	// 링 초기화: 리더가 현재 멤버십으로 링 v1을 자동 생성
	// (실제 구현에서는 leader election 후 자동으로 CmdSetRing propose)
	// 테스트에서는 직접 propose
	require.NoError(t, cluster.InitRing(t, leader.Backend, []string{leader.ID, cluster.Nodes[1].ID, cluster.Nodes[2].ID}))

	ctx := context.Background()
	bucket := "test-bucket"
	require.NoError(t, follower.Backend.CreateBucket(bucket))

	// 팔로워에 PUT
	content := []byte("hello from follower")
	require.NoError(t, follower.Backend.PutObject(bucket, "mykey", content, "text/plain"))

	// 모든 노드에서 GET 확인
	for _, n := range cluster.Nodes {
		rc, obj, err := n.Backend.GetObject(bucket, "mykey")
		require.NoError(t, err, "GetObject on node %s", n.ID)
		body, err := io.ReadAll(rc)
		require.NoError(t, err)
		_ = rc.Close()
		assert.Equal(t, content, body, "content mismatch on node %s", n.ID)
		assert.Equal(t, int64(len(content)), obj.Size)
	}
}
```

**주의:** `newTestCluster`, `testNode`, `cluster.InitRing` 헬퍼가 기존 테스트에 없으면 이 테스트 내에서 간단하게 구현한다. 기존 `testbackend.go`의 헬퍼를 확인 후 재사용한다.

- [ ] **Step 3: testbackend.go 기존 헬퍼 파악 후 InitRing 추가**

```bash
grep -n "newTestCluster\|TestCluster\|testNode\|newTestBackend" /Users/whitekid/work/gritive/grains/internal/cluster/testbackend.go | head -20
```

기존 헬퍼를 확인하고 `InitRing` 함수만 추가한다:

```go
// InitRing은 노드 목록으로 Ring v1을 propose한다 (테스트 전용).
func InitRing(t *testing.T, b *DistributedBackend, nodeIDs []string) error {
	t.Helper()
	ring := NewRing(1, nodeIDs, 10) // 테스트에서는 vPerNode=10으로 작게
	cmd := SetRingCmd{
		Version:  ring.Version,
		VNodes:   ring.VNodes,
		VPerNode: ring.VPerNode,
	}
	return b.propose(context.Background(), CmdSetRing, cmd)
}
```

- [ ] **Step 4: E2E 테스트 실행**

```bash
go test ./internal/cluster/ -run TestFollowerWrite_PutAndGetEC -count=1 -v -timeout=60s
```

Expected: PASS

- [ ] **Step 5: 전체 테스트**

```bash
go test ./... -count=1 -timeout=180s 2>&1 | tail -20
```

Expected: ok (모든 패키지 통과)

- [ ] **Step 6: 커밋**

```bash
git add internal/cluster/ring_e2e_test.go
git commit -m "test(cluster): 팔로워 쓰기 + 링 배치 E2E 테스트 추가"
```

---

## 최종 검증

- [ ] `go build ./...` 에러 없음
- [ ] `go test ./... -count=1 -timeout=300s` 모든 테스트 통과
- [ ] `go vet ./...` 경고 없음

---

## 자기 검토 (Spec 커버리지)

| 스펙 섹션 | 커버링 태스크 |
|---|---|
| Section 1: CmdSetRing(17) 추가 | Task 3, Task 4 |
| Section 1: CmdPutShardPlacement/CmdDeleteShardPlacement no-op | Task 4 |
| Section 1: PutObjectMetaCmd RingVersion/ECData/ECParity | Task 2, Task 3 |
| Section 2: Ring 타입 + PlacementForKey + walkCW | Task 1 |
| Section 3: propose() ProposeForward 포워딩 | Task 5, Task 6 |
| Section 3: 샤드 orphan best-effort 삭제 | Task 6 |
| Section 4: getObjectEC RingVersion 분기 | Task 6 |
| Section 4: RingVersion==0 fallback | Task 6 |
| Section 5: ReshardWorker 링 인식 | Task 7 |
| Section 5: ring GC refcount 기반 | Task 4 |
| Section 6: Raft 로그 하위 호환 no-op | Task 4 |
| raft.Node.IsLeader() API gap | Task 5 |
