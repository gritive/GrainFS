# Consistent Hash Ring + Follower Write Design

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raft 제어 평면/데이터 평면 분리 — `CmdPutShardPlacement`를 컨시스턴트 해시 링으로 대체하고, 모든 노드(팔로워 포함)에서 S3/NFS/NBD 쓰기를 가능하게 한다.

**Architecture:** 컨시스턴트 해시 링을 클러스터 멤버십의 단일 진실로 사용한다. 링은 노드 가입/이탈 시에만 Raft에 커밋된다(`CmdSetRing`). 오브젝트 메타에 `RingVersion`만 저장하면 어느 노드에서든 샤드 위치를 결정론적으로 재계산할 수 있다. 팔로워는 EC 샤드 배포를 직접 수행한 뒤 단일 propose를 QUIC RPC로 리더에게 포워딩한다.

**Tech Stack:** Go, BadgerDB (FSM), QUIC (transport), Reed-Solomon EC (klauspost/reedsolomon), FNV32 (링 토큰 계산)

---

## Section 1: Raft 변경

### 제거되는 커맨드
- `CmdPutShardPlacement` (12) — 오브젝트 단위 배치 저장 불필요
- `CmdDeleteShardPlacement` (13) — 동일

### 추가되는 커맨드
- `CmdSetRing` (17) — 노드 가입/이탈 시 전체 링 스냅샷을 Raft에 커밋

`CmdSetRing`은 쓰기마다 발생하지 않으며, 3노드 클러스터에서 멤버십 변경은 수십 번 이하.

### 오브젝트 메타 변경

```go
type PutObjectMetaCmd struct {
    Bucket, Key, ETag string
    Size              int64
    RingVersion       RingVersion  // 추가: 쓰기 시 사용한 링 버전
    // ... 기존 필드
}
```

구버전 오브젝트는 `RingVersion == 0` → `LookupShardPlacement` fallback 경로 유지.

BadgerDB에 저장되는 `ObjectMeta` 구조체에도 `RingVersion uint64` 필드가 추가된다. 기존 직렬화 포맷 변경이 필요하며 `encodeObjectMeta` / `decodeObjectMeta`도 함께 수정한다.

---

## Section 2: 링 데이터 구조와 배치 알고리즘

### 링 구조

```go
type RingVersion uint64

type VirtualNode struct {
    Token  uint32 // 링 위의 위치 (0 ~ 2^32-1)
    NodeID string
}

type Ring struct {
    Version  RingVersion
    VNodes   []VirtualNode // Token 기준 정렬
    VPerNode int           // 물리 노드당 가상 노드 수 (기본값 150)
}
```

물리 노드 1개 → 150개의 가상 노드가 링에 균등 분포.  
`VNode_i`의 Token = `FNV32(nodeID + "/" + strconv.Itoa(i))`.

### EC 샤드 배치 알고리즘

```go
// PlacementForKey: key의 EC 샤드 k+m개를 담당할 물리 노드 목록 반환.
// 샤드 i마다 hash(key+"/"+i) 위치에서 시계방향으로 걸어가며
// 아직 선택되지 않은 물리 노드를 만나면 선택.
func (r *Ring) PlacementForKey(cfg ECConfig, key string) []string {
    n := cfg.NumShards()
    chosen := make([]string, 0, n)
    seen := make(map[string]bool)

    for i := 0; i < n; i++ {
        token := fnv32(key + "/" + strconv.Itoa(i))
        nodeID := r.walkCW(token, seen)
        chosen = append(chosen, nodeID)
        seen[nodeID] = true
    }
    return chosen
}
```

**결정론적 보장:** 동일한 링 + 동일한 key → 항상 동일한 노드 목록.  
**균등 분포:** 가상 노드 150개/물리 노드 → 노드 간 부하 편차 < 10%.  
**중복 없음:** `seen` 맵으로 k+m개 샤드가 반드시 서로 다른 물리 노드에 배치됨.

---

## Section 3: 쓰기 경로 + QUIC ProposeForward

### 어느 노드에서도 쓰기 가능 (팔로워 포함)

```
클라이언트 → 팔로워 노드 F

1. F: FSM에서 현재 링 조회 → PlacementForKey로 k+m 노드 계산
2. F: EC split → 각 노드에 샤드 fan-out (기존 QUIC 경로)
3. F: propose(CmdPutObjectMeta{..., RingVersion: ring.Version})
      ├─ F == 리더 → ProposeWait() 직접 호출
      └─ F == 팔로워 → ProposeForward QUIC RPC → 리더 L
4. L: Raft 커밋 → FSM 적용
5. F: 클라이언트에게 200 OK
```

### QUIC ProposeForward RPC

`internal/transport/quic.go`에 메시지 타입 추가:

```go
const MsgProposeForward = 0x10

type ProposeForwardReq struct {
    CmdType CommandType
    Payload []byte       // 인코딩된 커맨드 바이트
}

type ProposeForwardResp struct {
    Index uint64
    Err   string         // 비어있으면 성공
}
```

### propose() 변경

```go
func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
    data, _ := encodeCommand(cmdType, payload)

    if b.node.IsLeader() {
        idx, err := b.node.ProposeWait(ctx, data)
        // ... 기존 경로
        return err
    }

    // 팔로워: 리더에게 포워딩
    leaderID := b.node.LeaderID()
    if leaderID == "" {
        return ErrNoLeader
    }
    return b.transport.ForwardPropose(ctx, leaderID, cmdType, data)
}
```

### 일관성 보장

- 팔로워가 샤드 fan-out 후 propose 포워딩 실패 → 기존 rollback/cleanup 경로 동일
- propose 성공 후 팔로워 크래시 → Raft에 커밋됨, scrubber가 샤드 정합성 확인
- 리더 전환 중 포워딩 → `ErrNoLeader` → 클라이언트 재시도 (S3 표준 동작)

---

## Section 4: 읽기 경로 + Dual-read Fallback

### 읽기 경로

```
1. FSM에서 ObjectMeta 조회 → RingVersion 확인

   RingVersion == 0 (구버전)
   └→ LookupShardPlacement(bucket, shardKey) → 기존 Nodes 목록

   RingVersion > 0 (신버전)
   └→ FSM.GetRing(RingVersion) → ring.PlacementForKey(cfg, key) → 노드 목록

2. 노드 목록에서 샤드 fetch (기존 getObjectEC와 동일)
3. EC reconstruct → 응답
```

### FSM 링 히스토리

```go
type ringStore struct {
    current RingVersion
    rings   map[RingVersion]*Ring
}
```

오브젝트가 참조하는 `RingVersion`의 링이 항상 조회 가능해야 한다. 오래된 링은 해당 버전을 참조하는 오브젝트가 없을 때 GC 가능.

**링 영속성:** 링 데이터는 BadgerDB에 `ring:<version>` 키로 저장된다. `applySetRing`이 BadgerDB에 직렬화해 쓰며, `ringStore`의 in-memory map은 캐시 역할. FSM 재시작 시 BadgerDB에서 전체 링 히스토리를 로드한다.

### getObjectEC 변경

```go
func (b *DistributedBackend) getObjectEC(bucket, key string) ([]byte, error) {
    meta, _ := b.fsm.GetObjectMeta(bucket, key)
    shardKey := shardKeyFor(key, meta)

    var placement []string
    if meta.RingVersion == 0 {
        rec, _ := b.fsm.LookupShardPlacement(bucket, shardKey)
        placement = rec.Nodes
    } else {
        ring, _ := b.fsm.GetRing(meta.RingVersion)
        placement = ring.PlacementForKey(meta.ECConfig(), shardKey)
    }

    return b.fetchAndReconstruct(bucket, shardKey, placement, meta)
}
```

---

## Section 5: 링 전환 + 백그라운드 리샤드

### 노드 가입

```
1. 새 노드 조인 (기존 메커니즘)
2. 리더: 현재 링에 150개 가상 노드 추가 → Ring{Version: v+1}
3. propose(CmdSetRing) → 전체 클러스터 커밋
4. 이후 쓰기: RingVersion=v+1 사용
5. ReshardWorker: 영향받은 오브젝트 점진적 리샤드 (약 1/N)
```

### 노드 이탈 (graceful)

```
1. 리더: 노드 가상 노드 제거 → Ring{Version: v+1} 커밋
2. ReshardWorker: 해당 노드 담당 샤드를 새 링 기준으로 재배치 (우선순위 높음)
3. 재배치 완료 확인 후 노드 제거
```

### 노드 이탈 (crash)

```
1. scrubber: 샤드 누락 감지 (기존 heal 경로)
2. ReshardWorker: 누락 샤드를 새 링의 다른 노드로 복구
3. 복구 완료 후 관리자가 노드 제거 → 링 업데이트
```

### ReshardWorker

```go
// reshard_manager.go 확장
type ReshardWorker struct {
    fsm       *FSM
    transport ShardTransport
    batchSize int           // 한 번에 처리 (기본 100)
    interval  time.Duration // 기본 30s
}

// 처리 대상: RingVersion < current AND PlacementForKey(currentRing) != PlacementForKey(oldRing)
// 즉 링 변경으로 샤드 위치가 달라진 오브젝트만 리샤드
```

**이동 범위:** 컨시스턴트 해싱 특성상 노드 추가 시 전체 오브젝트의 `1/N`만 이동. 3→4노드 전환 시 약 25%.

### 링 GC

다음 조건 모두 충족 시 FSM에서 링 버전 제거 가능:
1. `current ring version > v`
2. `RingVersion == v`인 오브젝트가 0개 (reshard 완료)

---

## Section 6: 마이그레이션 / 범위

### 마이그레이션 전략

- **기존 오브젝트:** `RingVersion == 0` → `LookupShardPlacement` fallback, 코드 변경 없이 동작
- **초기 링:** 리더가 FSM에 Ring v1이 없음을 감지하면 현재 클러스터 멤버십(`allNodes`)으로 Ring v1을 자동 생성하고 `CmdSetRing` propose. 기존 노드 가입 시 리더 선출 직후 수행
- **무중단:** 롤링 업그레이드 가능. 신버전 노드는 두 경로 모두 처리

### 영향받는 파일

```
internal/cluster/
  ring.go                — Ring, VirtualNode 타입, PlacementForKey, walkCW (신규)
  ring_store.go          — FSM 내 링 히스토리 저장/조회 (신규)
  ec.go                  — PlacementForNodes deprecated 표시
  fsm.go                 — CmdSetRing 추가, applySetRing
  backend.go             — propose() 포워딩, putObjectEC/getObjectEC 신규 경로
  reshard_manager.go     — ReshardWorker 링 인식 확장
  shard_placement.go     — 유지 (fallback용)

internal/transport/
  quic.go                — MsgProposeForward, ForwardPropose()

internal/raft/
  raft.go                — IsLeader(), LeaderID() 확인/추가
```

### 테스트 전략

- `ring_test.go`: PlacementForKey 결정론성, 균등 분포, k+m 노드 중복 없음
- `ring_store_test.go`: 버전별 조회, GC 조건
- `backend_test.go`: 팔로워 쓰기 → propose 포워딩 경로
- `reshard_manager_test.go`: 링 전환 후 영향 오브젝트 리샤드 검증
- E2E: 3노드 클러스터에서 팔로워에 PUT → 모든 노드에서 GET 성공

### 구현 순서

```
1. Ring 타입 + PlacementForKey 알고리즘 + 단위 테스트
2. FSM: CmdSetRing + 링 히스토리 저장/조회
3. transport: ProposeForward QUIC RPC
4. backend: propose() 포워딩 + putObjectEC/getObjectEC 신규 경로
5. ReshardWorker 링 인식 확장
6. E2E 테스트
```
