# Thin Provisioning (Phase A) Implementation Design

**Goal:** Volume Manager에 allocated_blocks 추적 + NBD TRIM/DISCARD 지원 + 공간 리포트 추가. 논리 크기 > 물리 공간 오버프로비저닝은 현재도 동작하며 이를 명시적으로 지원.

**Architecture:** FlatBuffer 스키마 확장(backward-compat) + WriteAt 신규 블록 감지 + Manager.Discard() 추가 + NBD NBD_CMD_TRIM 처리.

**Tech Stack:** Go, FlatBuffers, NBD protocol, storage.Backend

---

## 현재 상태

`Volume` FlatBuffer: `name`, `size`, `block_size` — 논리 크기만 존재.

`WriteAt`: 블록 쓸 때 항상 `GetObject`(read-modify-write). 오브젝트가 없으면 zeros 취급. 할당 추적 없음.

`ReadAt`: 오브젝트 없으면 zeros 반환 → reads는 이미 thin.

NBD: `NBD_FLAG_SEND_FLUSH`만 협상. `NBD_CMD_TRIM`(4) 미구현.

물리 스토리지: 블록 오브젝트만 실제 공간 점유. 논리 크기 1TB 볼륨도 Create 시 0 bytes.

---

## Phase A 범위

### 포함
1. `allocated_blocks: int64` — FlatBuffer 필드 추가 (backward-compat, 기존 볼륨 = 0)
2. `WriteAt` 신규 블록 감지 → 배치 카운터 업데이트
3. `Manager.Discard(name, off, length)` — 블록 오브젝트 삭제 + 카운터 감소
4. `Volume.AllocatedBytes()` — 공간 리포트
5. NBD `NBD_CMD_TRIM=4` + `NBD_FLAG_SEND_TRIM` 협상
6. `GET /volumes/{name}` 응답에 `allocated_bytes` 추가
7. 오버프로비저닝: 현재 동작 유지 (논리 크기 > 물리 공간 허용) + 옵션 pool quota

### 미포함 (별도 TODO)
- 크로스볼륨 thin pool quota
- NFS 레이어 DISCARD passthrough
- allocated_blocks drift 복구 커맨드
- 블록 중복 제거(dedup)
- Phase B: CoW 스냅샷

---

## Architecture

### 1. FlatBuffer 스키마 변경

`internal/volume/volume.fbs`:
```fbs
table Volume {
  name:string;
  size:int64;
  block_size:int32;
  allocated_blocks:int64 = -1;  // -1=untracked(기존볼륨), 0=할당없음, >0=블록수
}
```

FlatBuffers는 새 필드를 테이블 끝에 추가하면 backward-compatible. **스키마 default를 `-1`로 명시해야 한다**: 기존 직렬화된 볼륨은 해당 필드가 없으므로 reader가 스키마 default(-1)를 반환 → "untracked" 시맨틱이 자연스럽게 적용됨. codegen 시 `PrependInt64Slot(N, value, -1)` 호출로 생성 확인 필수.

### 2. Volume 구조체 확장

`internal/volume/volume.go`:
```go
type Volume struct {
    Name            string
    Size            int64
    BlockSize       int
    AllocatedBlocks int64  // -1 = untracked (기존 볼륨), 0 = 할당 없음, >0 = 블록 수
}

func (v *Volume) AllocatedBytes() int64 {
    if v.AllocatedBlocks < 0 {
        return -1 // untracked
    }
    return v.AllocatedBlocks * int64(v.BlockSize)
}
```

### 3. Manager 구조 및 옵션

```go
type ManagerOptions struct {
    PoolQuota int64 // 0 = unlimited (default). 양수면 allocated_blocks * block_size 합계가 초과 시 쓰기 거부
}

// Manager는 인메모리 볼륨 캐시를 유지한다.
// volumes 맵은 mu 보호 하에 읽고 쓴다.
// S3 메타데이터를 매 호출마다 읽지 않으므로 WriteAt/Discard 핫패스에서 S3 왕복이 없다.
type Manager struct {
    backend storage.Backend
    mu      sync.RWMutex
    volumes map[string]*Volume  // 인메모리 캐시; nil = 미로드
    opts    ManagerOptions
}

func NewManagerWithOptions(backend storage.Backend, opts ManagerOptions) *Manager
func NewManager(backend storage.Backend) *Manager // ManagerOptions{} 기본값
```

**캐시 정책:** Create/Get 시 캐시 적재, Delete 시 캐시 제거, WriteAt/Discard 시 캐시 갱신(S3 PutObject 성공 후). 서버 재시작 시 캐시 비어있음 → 첫 Get/WriteAt에서 S3에서 로드.

**뮤텍스 설계 (Phase A):** 현재와 동일한 전역 `sync.RWMutex`. 볼륨별 Actor(goroutine+channel)는 Phase 17 Lock-free 검토 항목으로 이미 TODOS에 등록됨. 인메모리 캐시 추가로 핵심 S3 왕복 병목은 해소.

### 4. WriteAt — 신규 블록 감지

현재 `WriteAt`은 블록마다 `GetObject`를 수행(partial-block read-modify-write). 이 결과를 활용해 신규 블록 여부 판단.

변경 흐름:
```
WriteAt(name, p, off):
  m.mu.Lock() (기존 유지)
  vol = m.volumes[name]  // 인메모리 캐시에서 읽음
  
  // 1단계: PoolQuota 사전 검사 (루프 진입 전 원자적 거부)
  if opts.PoolQuota > 0:
    newBlocksNeeded = 각 blockKey에 대해 backend.HeadObject 실패 카운트 (pre-scan)
    //   HeadObject: 존재 여부만 확인, body 다운로드 없음
    currentAllocated = sum(v.AllocatedBlocks * v.BlockSize for v in m.volumes)
    //   인메모리 맵 O(N) 순회 — S3 왕복 없음
    if currentAllocated + newBlocksNeeded*blockSize > opts.PoolQuota:
      return 0, ErrPoolQuotaExceeded
  
  // 2단계: 실제 쓰기 루프
  newBlocks = 0
  for each block in range:
    rc, _, err = backend.GetObject(blockKey)
    isNew = (err != nil)        // 오브젝트 없음 = 신규 블록
    if isNew:
      blkData = zeros
    else:
      blkData = read existing
    
    copy p → blkData
    PutObject(blockKey, bytes.NewReader(blkData), "application/octet-stream")
    if isNew: newBlocks++
  
  if newBlocks > 0:
    if vol.AllocatedBlocks < 0 { vol.AllocatedBlocks = 0 }  // untracked → 추적 시작
    vol.AllocatedBlocks += newBlocks
    PutObject(metaKey, marshalVolume(vol))  // 캐시는 이미 갱신됨 (vol은 포인터)
  return totalWritten, nil
```

**메타데이터 일관성:** 블록 쓰기 성공 후 카운터 업데이트. 서버 크래시로 카운터만 누락되면 drift 발생 → 허용(space accounting). 복구 커맨드는 별도 TODO.

**PoolQuota 사전 검사 비용:** opts.PoolQuota == 0(기본값)이면 사전 검사 완전 생략. 활성화 시 O(blocks_in_range) HeadObject + O(volumes) 인메모리 순회. 옵션 기능이므로 허용.

### 5. Manager.Discard

```go
// Discard marks the byte range [off, off+length) as free.
// Blocks fully within the range are deleted from storage.
// Partially covered blocks are skipped (cannot partially discard a block).
func (m *Manager) Discard(name string, off, length int64) error
```

흐름:
```
Discard(name, off, length):
  m.mu.Lock()  // WriteAt과 동일한 write mutex
  defer m.mu.Unlock()
  vol = getVolUnlocked(name)
  bs = vol.BlockSize
  
  firstBlock = ceil(off / bs)          // 부분 커버 블록 제외
  lastBlock  = floor((off+length) / bs) - 1
  
  freed = 0
  for blkNum = firstBlock to lastBlock:
    err = backend.DeleteObject(blockKey(name, blkNum))
    if err == nil: freed++
  
  if freed > 0:
    if vol.AllocatedBlocks >= 0:  // untracked(-1)인 경우 카운터 수정하지 않음
      vol.AllocatedBlocks = max(0, vol.AllocatedBlocks - freed)
      PutObject(metaKey, marshalVolume(vol))
  return nil
```

### 6. NBD TRIM

`internal/nbd/nbd.go` 상수 추가:
```go
nbdCmdTrim          = uint32(4)
nbdFlagSendTrim     = uint16(1 << 5)  // NBD spec: bit 5
```

전송 플래그 협상(ExportName 및 OptGo 핸들러):
```go
// 기존:
nbdFlagHasFlags | nbdFlagSendFlush
// 변경:
nbdFlagHasFlags | nbdFlagSendFlush | nbdFlagSendTrim
```

커맨드 루프:
```go
case nbdCmdTrim:
    if err := s.mgr.Discard(s.volName, int64(offset), int64(length)); err != nil {
        return s.sendReply(conn, handle, 5, nil) // EIO
    }
    return s.sendReply(conn, handle, 0, nil)
```

NBD TRIM request는 data payload 없음. offset + length만 헤더에 있음.

### 7. API 응답 확장

`GET /volumes/{name}` JSON 응답:
```json
{
  "name": "myvolume",
  "size": 107374182400,
  "block_size": 4096,
  "allocated_bytes": 4194304,
  "allocated_blocks": 1024
}
```

**allocated_bytes 시맨틱:**
- `-1`: 미추적 (기존 볼륨이거나 아직 `recalculate` 미실행)
- `0`: 할당된 블록 없음
- `> 0`: 실제 할당 바이트

---

## 파일 구조

| 파일 | 변경 유형 | 내용 |
|------|-----------|------|
| `internal/volume/volume.fbs` | 수정 | `allocated_blocks:int64 = -1` 추가 |
| `internal/volume/volumepb/Volume.go` | 재생성 | `make fbs` + default -1 확인 |
| `internal/volume/volume.go` | 수정 | Manager.volumes 캐시, AllocatedBlocks 필드, WriteAt 감지 로직, Discard(), ManagerOptions |
| `internal/volume/codec.go` | 수정 | marshal/unmarshal allocated_blocks |
| `internal/nbd/nbd.go` | 수정 | NBD_CMD_TRIM, NBD_FLAG_SEND_TRIM, handleRequest case |
| `internal/volume/volume_test.go` | 수정 | Discard 테스트, AllocatedBlocks 추적 테스트 |
| `internal/nbd/nbd_test.go` | 수정 | TRIM E2E (Docker NBD) |
| `internal/server/volume_handlers.go` | 수정 | GET /volumes/{name} 응답에 allocated_bytes 추가 |

---

## 오버프로비저닝 모델

현재 GrainFS는 이미 thin-provisioned: 볼륨 Create 시 메타데이터 오브젝트만 생성, 물리 공간 0 소비. 논리 크기 합이 물리 용량을 초과해도 Create 가능. 이 동작을 그대로 유지.

`PoolQuota` 옵션이 0(기본)이면 무제한 오버프로비저닝. 양수로 설정 시 WriteAt에서 할당량 초과 방지.

CLI: `grainfs serve --volume-pool-quota 500GiB` (선택적)

---

## 에러 정의

```go
var (
    ErrPoolQuotaExceeded = errors.New("volume pool quota exceeded")
)
```

---

## 테스트 계획

### 단위 테스트 (`volume_test.go`)

| 케이스 | 검증 |
|--------|------|
| 신규 블록 쓰기 후 AllocatedBlocks == 1 | 카운터 증가 |
| 동일 블록 덮어쓰기 — AllocatedBlocks 불변 | 중복 카운트 방지 |
| Discard 후 AllocatedBlocks 감소 | 카운터 감소 |
| Discard 부분 커버 블록 — 해당 블록 보존 | 정렬 경계 처리 |
| Discard 미존재 블록 — 오류 없이 통과 | idempotent |
| PoolQuota 초과 시 ErrPoolQuotaExceeded | 쓰기 거부 |
| PoolQuota 정확히 한도에서 통과, 한 블록 초과 시 거부 | 경계값 검사 |
| Discard가 AllocatedBlocks보다 큰 범위 요청 — 0 이하로 내려가지 않음 | clamp 검사 |
| GET /volumes/{name} 응답에 allocated_bytes 필드 존재 | API 응답 스키마 |

### NBD TRIM 통합 테스트 (`nbd_test.go`, Docker)

| 케이스 | 검증 |
|--------|------|
| `mkfs.ext4` + 파일 쓰기 + 삭제 + `fstrim -v` | TRIM 커맨드 수신, 블록 오브젝트 삭제 확인 |
| TRIM 전/후 AllocatedBytes 비교 | 공간 회수 확인 |

---

## 주의사항

- **카운터 drift:** WriteAt 중 서버 크래시 시 `allocated_blocks`와 실제 오브젝트 수가 불일치 가능. Phase A에서는 허용. 복구는 `volume recalculate` 커맨드(별도 TODO)로.
- **부분 블록 DISCARD:** OS(Linux)는 실제로 block-aligned TRIM만 보내므로 부분 커버 스킵이 문제되지 않음. 단, `off+length`가 블록 경계에 정확히 정렬될 때 `lastBlock = floor((off+length)/bs) - 1`이 마지막 블록을 올바르게 제외함을 테스트로 검증.
- **FlatBuffer 재생성:** `make fbs` 실행 필수. `.fbs.stamp` 파일이 있으므로 변경 감지 자동. 재생성 후 `PrependInt64Slot` default 인자가 `-1`인지 반드시 확인.
- **클러스터 모드 범위 외:** Phase A는 단일 노드 전용. 멀티 노드에서 동일 볼륨에 동시 WriteAt 시 `allocated_blocks` 카운터가 LWW(last-write-wins)로 손실됨. 클러스터 안전성은 Phase A 범위 밖 — 볼륨 메타데이터를 Raft 로그를 통해 갱신하거나 S3 conditional put(if-match ETag) 방식으로 별도 구현 필요.
- **DeleteObject 멱등성:** `storage.Backend` 인터페이스 계약 — missing-key delete는 `nil` 반환(S3 호환: 204 반환). 이 계약이 보장되어야 Discard의 `freed` 카운트가 정확함. 구현체(local backend 등)에서 검증 필요.
- **인메모리 캐시 일관성:** `Manager.volumes` 맵은 서버 재시작 시 비어있음. 첫 접근 시 S3에서 로드. Create/Delete/WriteAt/Discard에서 항상 캐시를 갱신해야 함.
