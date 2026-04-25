# Thin Provisioning Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Volume Manager에 allocated_blocks 추적, NBD TRIM/DISCARD 지원, 공간 리포트를 추가해 Thin Provisioning Phase A를 완성한다.

**Architecture:** FlatBuffer 스키마에 `allocated_blocks:int64 = -1` 필드 추가 (backward-compat) → Manager에 인메모리 볼륨 캐시 추가 → WriteAt에서 신규 블록 감지·카운터 증가, Discard()로 블록 삭제·카운터 감소 → NBD `NBD_CMD_TRIM` 처리 추가 → API 응답에 `allocated_bytes` 노출.

**Tech Stack:** Go 1.26+, FlatBuffers (google/flatbuffers/go), storage.Backend (HeadObject/DeleteObject 멱등), NBD protocol

---

## 파일 구조

| 파일 | 변경 유형 | 내용 |
|------|-----------|------|
| `internal/volume/volume.fbs` | 수정 | `allocated_blocks:int64 = -1` 추가 |
| `internal/volume/volumepb/Volume.go` | 재생성 | `make fbs` (touch stamp 삭제 후) |
| `internal/volume/volume.go` | 수정 | Volume 구조체, Manager 캐시, WriteAt, Discard, ManagerOptions |
| `internal/volume/codec.go` | 수정 | marshalVolume/unmarshalVolume에 AllocatedBlocks 추가 |
| `internal/volume/volume_test.go` | 수정 | AllocatedBlocks 추적, Discard, PoolQuota 테스트 추가 |
| `internal/nbd/nbd.go` | 수정 | nbdCmdTrim, nbdFlagSendTrim, handleRequest case |
| `internal/server/volume_handlers.go` | 수정 | volumeResponse에 AllocatedBytes/AllocatedBlocks 추가 |
| `internal/volume/errors.go` | 생성 | ErrPoolQuotaExceeded 정의 |

---

## Task 1: FlatBuffer 스키마 수정 및 코드 재생성

**Files:**
- Modify: `internal/volume/volume.fbs`
- Regenerate: `internal/volume/volumepb/Volume.go`

- [ ] **Step 1: volume.fbs에 allocated_blocks 필드 추가**

`internal/volume/volume.fbs`를 아래와 같이 수정한다:

```fbs
// FlatBuffers schema for volume metadata.
// Regenerate: make fbs

namespace volumepb;

table Volume {
  name:string;
  size:int64;
  block_size:int32;
  allocated_blocks:int64 = -1;  // -1=untracked(기존 볼륨), 0=할당없음, >0=블록수
}

root_type Volume;
```

- [ ] **Step 2: stamp 파일 삭제 후 make fbs 실행**

```bash
rm -f internal/volume/volume.fbs.stamp
make fbs
```

Expected: `flatc --go --gen-all ...` 실행 후 에러 없음

- [ ] **Step 3: 재생성된 코드 검증**

```bash
grep -A6 "AllocatedBlocks" internal/volume/volumepb/Volume.go
```

Expected output — 아래 3가지를 모두 확인:
1. `return -1` 이 기본값으로 생성됨
2. `VolumeStart` 가 `StartObject(4)` 를 호출 (필드 4개)
3. `PrependInt64Slot(3, allocatedBlocks, -1)` 가 있음

```bash
grep "StartObject\|PrependInt64Slot(3" internal/volume/volumepb/Volume.go
```

Expected: `builder.StartObject(4)` 및 `builder.PrependInt64Slot(3, allocatedBlocks, -1)`

- [ ] **Step 4: 기존 빌드 통과 확인**

```bash
go build ./internal/volume/...
```

Expected: 에러 없음 (codec.go가 아직 AllocatedBlocks를 사용하지 않으므로 컴파일 통과)

- [ ] **Step 5: 커밋**

```bash
git add internal/volume/volume.fbs internal/volume/volumepb/Volume.go
git commit -m "feat(volume/fbs): allocated_blocks:int64 = -1 필드 추가"
```

---

## Task 2: errors.go 생성 + Volume 구조체 + codec 수정

**Files:**
- Create: `internal/volume/errors.go`
- Modify: `internal/volume/volume.go` (Volume struct, AllocatedBytes method)
- Modify: `internal/volume/codec.go`

- [ ] **Step 1: errors.go 생성**

```go
// internal/volume/errors.go
package volume

import "errors"

var ErrPoolQuotaExceeded = errors.New("volume pool quota exceeded")
```

- [ ] **Step 2: Volume 구조체에 AllocatedBlocks 필드 추가**

`internal/volume/volume.go`의 `Volume` 구조체를 수정한다:

```go
// Volume represents a virtual block device backed by object storage.
type Volume struct {
	Name            string
	Size            int64
	BlockSize       int
	AllocatedBlocks int64 // -1=untracked, 0=empty, >0=block count
}

// AllocatedBytes returns the number of bytes allocated in backing storage.
// Returns -1 if the volume was created before allocation tracking was introduced.
func (v *Volume) AllocatedBytes() int64 {
	if v.AllocatedBlocks < 0 {
		return -1
	}
	return v.AllocatedBlocks * int64(v.BlockSize)
}
```

- [ ] **Step 3: ManagerOptions + Manager 구조체 수정**

`internal/volume/volume.go`의 `Manager` 구조체를 아래와 같이 교체한다:

```go
// ManagerOptions configures optional Manager behaviour.
type ManagerOptions struct {
	// PoolQuota is the maximum total allocated bytes across all volumes.
	// 0 means unlimited (default).
	PoolQuota int64
}

// Manager manages volumes on top of a storage.Backend.
type Manager struct {
	backend storage.Backend
	mu      sync.Mutex         // 단일 뮤텍스: read-modify-write 원자성 보장
	volumes map[string]*Volume // 인메모리 캐시
	opts    ManagerOptions
}

// NewManager creates a new volume manager.
func NewManager(backend storage.Backend) *Manager {
	return NewManagerWithOptions(backend, ManagerOptions{})
}

// NewManagerWithOptions creates a new volume manager with the given options.
func NewManagerWithOptions(backend storage.Backend, opts ManagerOptions) *Manager {
	return &Manager{
		backend: backend,
		volumes: make(map[string]*Volume),
		opts:    opts,
	}
}
```

주의: `sync.RWMutex` → `sync.Mutex` 로 변경. WriteAt이 어차피 Lock()을 쓰므로 RLock()의 이득이 없음.

- [ ] **Step 4: codec.go의 marshalVolume/unmarshalVolume 수정**

`internal/volume/codec.go` 전체를 아래로 교체한다:

```go
package volume

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/volume/volumepb"
)

var volumeBuilderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(256) },
}

func marshalVolume(vol *Volume) ([]byte, error) {
	b := volumeBuilderPool.Get().(*flatbuffers.Builder)
	nameOff := b.CreateString(vol.Name)
	volumepb.VolumeStart(b)
	volumepb.VolumeAddName(b, nameOff)
	volumepb.VolumeAddSize(b, vol.Size)
	volumepb.VolumeAddBlockSize(b, int32(vol.BlockSize))
	volumepb.VolumeAddAllocatedBlocks(b, vol.AllocatedBlocks)
	root := volumepb.VolumeEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	volumeBuilderPool.Put(b)
	return out, nil
}

func unmarshalVolume(data []byte) (vol *Volume, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal volume: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal volume: invalid flatbuffer: %v", r)
		}
	}()
	t := volumepb.GetRootAsVolume(data, 0)
	return &Volume{
		Name:            string(t.Name()),
		Size:            t.Size(),
		BlockSize:       int(t.BlockSize()),
		AllocatedBlocks: t.AllocatedBlocks(), // -1 if field absent (old volumes)
	}, nil
}
```

- [ ] **Step 5: 빌드 통과 확인**

```bash
go build ./internal/volume/...
```

Expected: 에러 없음

- [ ] **Step 6: 기존 테스트가 여전히 통과하는지 확인**

```bash
go test ./internal/volume/... -run TestCreate -v
go test ./internal/volume/... -run TestGet -v
go test ./internal/volume/... -run TestWrite -v
```

Expected: 모든 기존 테스트 PASS (AllocatedBlocks = -1로 초기화됨)

- [ ] **Step 7: codec round-trip 테스트 추가**

`internal/volume/volume_test.go`에 추가:

```go
func TestVolumeCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name            string
		allocatedBlocks int64
	}{
		{"untracked", -1},
		{"empty", 0},
		{"with blocks", 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vol := &Volume{
				Name:            "test",
				Size:            1024,
				BlockSize:       4096,
				AllocatedBlocks: tt.allocatedBlocks,
			}
			data, err := marshalVolume(vol)
			require.NoError(t, err)

			got, err := unmarshalVolume(data)
			require.NoError(t, err)
			assert.Equal(t, tt.allocatedBlocks, got.AllocatedBlocks)
		})
	}
}

func TestVolumeAllocatedBytes(t *testing.T) {
	tests := []struct {
		allocatedBlocks int64
		blockSize       int
		want            int64
	}{
		{-1, 4096, -1},   // untracked
		{0, 4096, 0},     // empty
		{3, 4096, 12288}, // 3 blocks * 4096
	}
	for _, tt := range tests {
		vol := &Volume{AllocatedBlocks: tt.allocatedBlocks, BlockSize: tt.blockSize}
		assert.Equal(t, tt.want, vol.AllocatedBytes())
	}
}
```

- [ ] **Step 8: 테스트 실행**

```bash
go test ./internal/volume/... -run "TestVolumeCodec|TestVolumeAllocated" -v
```

Expected: PASS

- [ ] **Step 9: 커밋**

```bash
git add internal/volume/errors.go internal/volume/volume.go internal/volume/codec.go internal/volume/volume_test.go
git commit -m "feat(volume): Volume.AllocatedBlocks 필드 + ManagerOptions + 인메모리 캐시 구조"
```

---

## Task 3: Manager 메서드 캐시 통합

**Files:**
- Modify: `internal/volume/volume.go` (Create, Get, Delete, List, getVolUnlocked)

`getVolUnlocked`와 모든 외부 공개 메서드가 인메모리 캐시를 사용하도록 수정한다.

- [ ] **Step 1: getVolUnlocked 캐시 우선 읽기로 수정**

`internal/volume/volume.go`의 `getVolUnlocked` 함수를 아래로 교체한다:

```go
// getVolUnlocked returns the cached volume pointer (caller must hold m.mu).
// On cache miss, loads from storage and populates the cache.
func (m *Manager) getVolUnlocked(name string) (*Volume, error) {
	if vol, ok := m.volumes[name]; ok {
		return vol, nil
	}
	rc, _, err := m.backend.GetObject(volumeBucketName, metaKey(name))
	if err != nil {
		return nil, fmt.Errorf("volume %q not found", name)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read volume metadata: %w", err)
	}

	vol, err := unmarshalVolume(data)
	if err != nil {
		return nil, fmt.Errorf("unmarshal volume metadata: %w", err)
	}
	m.volumes[name] = vol
	return vol, nil
}
```

- [ ] **Step 2: Create 메서드 캐시 추가**

`Create` 메서드에서 `m.mu.Lock()` → `m.mu.Unlock()` 전에 캐시에 추가:

```go
func (m *Manager) Create(name string, sizeBytes int64) (*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.ensureBucket(); err != nil {
		return nil, fmt.Errorf("ensure volume bucket: %w", err)
	}

	if _, _, err := m.backend.GetObject(volumeBucketName, metaKey(name)); err == nil {
		return nil, fmt.Errorf("volume %q already exists", name)
	}

	vol := &Volume{
		Name:            name,
		Size:            sizeBytes,
		BlockSize:       DefaultBlockSize,
		AllocatedBlocks: -1, // untracked until first write
	}

	data, err := marshalVolume(vol)
	if err != nil {
		return nil, fmt.Errorf("marshal volume metadata: %w", err)
	}

	if _, err := m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf"); err != nil {
		return nil, fmt.Errorf("store volume metadata: %w", err)
	}

	m.volumes[name] = vol // cache
	copy := *vol
	return &copy, nil
}
```

- [ ] **Step 3: Get 메서드 수정 (캐시 기반, 복사본 반환)**

```go
func (m *Manager) Get(name string) (*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return nil, err
	}
	copy := *vol
	return &copy, nil
}
```

- [ ] **Step 4: Delete 메서드 캐시 제거 추가**

`Delete` 메서드 끝에 `m.backend.DeleteObject(volumeBucketName, metaKey(name))` 이후 캐시 제거:

```go
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, _, err := m.backend.GetObject(volumeBucketName, metaKey(name)); err != nil {
		return fmt.Errorf("volume %q not found", name)
	}

	objs, err := m.backend.ListObjects(volumeBucketName, blockPrefix(name), 100000)
	if err == nil {
		for _, obj := range objs {
			_ = m.backend.DeleteObject(volumeBucketName, obj.Key)
		}
	}

	if err := m.backend.DeleteObject(volumeBucketName, metaKey(name)); err != nil {
		return err
	}
	delete(m.volumes, name) // evict cache
	return nil
}
```

- [ ] **Step 5: List 메서드 캐시 갱신 추가**

`List` 메서드가 로드한 볼륨을 캐시에 등록하도록 수정. `volumes = append(volumes, vol)` 직전에 `m.volumes[vol.Name] = vol` 추가:

```go
func (m *Manager) List() ([]*Volume, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.backend.HeadBucket(volumeBucketName); err != nil {
		return nil, nil
	}

	objs, err := m.backend.ListObjects(volumeBucketName, metaPrefix, 10000)
	if err != nil {
		return nil, fmt.Errorf("list volume metadata: %w", err)
	}

	var volumes []*Volume
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, "/meta") {
			continue
		}
		rc, _, err := m.backend.GetObject(volumeBucketName, obj.Key)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}
		vol, err := unmarshalVolume(data)
		if err != nil {
			continue
		}
		m.volumes[vol.Name] = vol // update cache
		copy := *vol
		volumes = append(volumes, &copy)
	}
	return volumes, nil
}
```

- [ ] **Step 6: ReadAt 뮤텍스 변경 (RWMutex→Mutex)**

`ReadAt`의 `m.mu.RLock()` / `m.mu.RUnlock()` → `m.mu.Lock()` / `m.mu.Unlock()`:

```go
func (m *Manager) ReadAt(name string, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// ... 나머지 동일
```

- [ ] **Step 7: 전체 기존 테스트 통과 확인**

```bash
go test ./internal/volume/... -v
```

Expected: 모든 기존 테스트 PASS

- [ ] **Step 8: 커밋**

```bash
git add internal/volume/volume.go
git commit -m "feat(volume): Manager 인메모리 볼륨 캐시 통합 — S3 메타 왕복 제거"
```

---

## Task 4: WriteAt AllocatedBlocks 추적

**Files:**
- Modify: `internal/volume/volume.go` (WriteAt)
- Modify: `internal/volume/volume_test.go`

- [ ] **Step 1: 실패 테스트 작성**

`internal/volume/volume_test.go`에 추가:

```go
func TestAllocatedBlocksTracking(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.Create("track-test", 16384) // 4 blocks of 4096
	require.NoError(t, err)

	// 새 볼륨은 untracked (-1)
	vol, err := mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), vol.AllocatedBlocks)

	// 블록 0에 쓰기 → AllocatedBlocks = 1
	_, err = mgr.WriteAt("track-test", []byte("hello"), 0)
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks)
	assert.Equal(t, int64(4096), vol.AllocatedBytes())

	// 동일 블록 덮어쓰기 → AllocatedBlocks 변화 없음
	_, err = mgr.WriteAt("track-test", []byte("world"), 0)
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(1), vol.AllocatedBlocks, "overwrite should not increment counter")

	// 블록 1에 쓰기 → AllocatedBlocks = 2
	_, err = mgr.WriteAt("track-test", []byte("new block"), int64(DefaultBlockSize))
	require.NoError(t, err)

	vol, err = mgr.Get("track-test")
	require.NoError(t, err)
	assert.Equal(t, int64(2), vol.AllocatedBlocks)
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

```bash
go test ./internal/volume/... -run TestAllocatedBlocksTracking -v
```

Expected: FAIL (`AllocatedBlocks`가 여전히 -1이므로)

- [ ] **Step 3: WriteAt에 신규 블록 감지 + 카운터 업데이트 구현**

`internal/volume/volume.go`의 `WriteAt` 함수를 아래로 교체한다:

```go
func (m *Manager) WriteAt(name string, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, err
	}

	if off < 0 || off >= vol.Size {
		return 0, fmt.Errorf("offset %d out of range [0, %d)", off, vol.Size)
	}

	bs := int64(vol.BlockSize)
	totalWritten := 0
	newBlocks := 0

	for totalWritten < len(p) && off+int64(totalWritten) < vol.Size {
		pos := off + int64(totalWritten)
		blkNum := pos / bs
		blkOff := pos % bs

		// Read existing block (or zeros); isNew tracks if this is a new allocation
		blkData := make([]byte, vol.BlockSize)
		rc, _, err := m.backend.GetObject(volumeBucketName, blockKey(name, blkNum))
		isNew := err != nil
		if !isNew {
			io.ReadFull(rc, blkData)
			rc.Close()
		}

		canWrite := int(bs - blkOff)
		remaining := len(p) - totalWritten
		if canWrite > remaining {
			canWrite = remaining
		}
		if endPos := off + int64(totalWritten) + int64(canWrite); endPos > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		copy(blkData[blkOff:blkOff+int64(canWrite)], p[totalWritten:totalWritten+canWrite])

		if _, err := m.backend.PutObject(volumeBucketName, blockKey(name, blkNum),
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return totalWritten, fmt.Errorf("write block %d: %w", blkNum, err)
		}

		if isNew {
			newBlocks++
		}
		totalWritten += canWrite
	}

	if newBlocks > 0 {
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0 // untracked → start tracking
		}
		vol.AllocatedBlocks += int64(newBlocks)
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
		}
		// vol은 캐시 포인터이므로 캐시도 이미 갱신됨
	}

	return totalWritten, nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test ./internal/volume/... -run TestAllocatedBlocksTracking -v
```

Expected: PASS

- [ ] **Step 5: 전체 테스트 통과 확인**

```bash
go test ./internal/volume/... -v
```

Expected: 모든 테스트 PASS (`strings` 패키지 import 제거 필요할 수 있음 — `strings.NewReader` 삭제)

gofmt 적용:
```bash
gofmt -w internal/volume/volume.go
```

- [ ] **Step 6: 커밋**

```bash
git add internal/volume/volume.go internal/volume/volume_test.go
git commit -m "feat(volume): WriteAt AllocatedBlocks 추적 — 신규 블록 감지 + 카운터 증가"
```

---

## Task 5: Manager.Discard 구현

**Files:**
- Modify: `internal/volume/volume.go`
- Modify: `internal/volume/volume_test.go`

- [ ] **Step 1: Discard 실패 테스트 작성**

`internal/volume/volume_test.go`에 추가:

```go
func TestDiscard(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("discard-test", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 0, 1, 2에 쓰기
	_, err = mgr.WriteAt("discard-test", make([]byte, blockSize*3), 0)
	require.NoError(t, err)

	vol, _ := mgr.Get("discard-test")
	require.Equal(t, int64(3), vol.AllocatedBlocks, "should have 3 blocks after write")

	// 블록 1만 discard (off=4096, length=4096)
	err = mgr.Discard("discard-test", int64(blockSize), int64(blockSize))
	require.NoError(t, err)

	vol, _ = mgr.Get("discard-test")
	assert.Equal(t, int64(2), vol.AllocatedBlocks, "counter should decrease by 1")

	// 블록 0 읽기는 여전히 데이터 있음
	buf := make([]byte, blockSize)
	_, err = mgr.ReadAt("discard-test", buf, 0)
	require.NoError(t, err)

	// 블록 1 읽기는 zeros
	_, err = mgr.ReadAt("discard-test", buf, int64(blockSize))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, blockSize), buf, "discarded block should read as zeros")
}

func TestDiscardPartialBlock(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("partial-discard", int64(blockSize*4))
	require.NoError(t, err)

	_, err = mgr.WriteAt("partial-discard", make([]byte, blockSize*2), 0)
	require.NoError(t, err)

	// 부분 커버 discard — 블록 0의 일부와 블록 1의 일부만 포함
	// off=1, length=blockSize-1 → firstBlock=ceil(1/bs)=1, lastBlock=floor(bs/bs)-1=0 → 범위 없음
	err = mgr.Discard("partial-discard", 1, int64(blockSize-1))
	require.NoError(t, err)

	vol, _ := mgr.Get("partial-discard")
	assert.Equal(t, int64(2), vol.AllocatedBlocks, "partial discard should not free any block")
}

func TestDiscardNonExistentBlock(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("no-block-discard", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 쓰기 없이 discard → 에러 없이 통과
	err = mgr.Discard("no-block-discard", 0, int64(blockSize))
	require.NoError(t, err, "discard of non-existent block should be idempotent")
}

func TestDiscardCounterClamp(t *testing.T) {
	const blockSize = DefaultBlockSize
	mgr := setupManager(t)
	_, err := mgr.Create("clamp-test", int64(blockSize*4))
	require.NoError(t, err)

	// 블록 1개 쓰기 후 2블록 범위 discard
	_, err = mgr.WriteAt("clamp-test", make([]byte, blockSize), 0)
	require.NoError(t, err)

	vol, _ := mgr.Get("clamp-test")
	require.Equal(t, int64(1), vol.AllocatedBlocks)

	// 블록 0, 1 discard (블록 1은 미존재)
	err = mgr.Discard("clamp-test", 0, int64(blockSize*2))
	require.NoError(t, err)

	vol, _ = mgr.Get("clamp-test")
	assert.Equal(t, int64(0), vol.AllocatedBlocks, "counter should not go below 0")
}
```

- [ ] **Step 2: 테스트가 컴파일 오류인지 확인 (Discard 미구현)**

```bash
go test ./internal/volume/... -run TestDiscard 2>&1 | head -5
```

Expected: `mgr.Discard undefined` 컴파일 오류

- [ ] **Step 3: Discard 구현**

`internal/volume/volume.go`에 WriteAt 함수 뒤에 추가:

```go
// Discard marks the byte range [off, off+length) as free.
// Only blocks fully within the range are deleted; partially covered blocks are skipped.
// This operation is idempotent — deleting non-existent blocks is not an error.
func (m *Manager) Discard(name string, off, length int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return err
	}

	bs := int64(vol.BlockSize)
	firstBlock := (off + bs - 1) / bs         // ceil(off/bs)
	lastBlock := (off+length)/bs - 1           // floor((off+length)/bs) - 1

	if lastBlock < firstBlock {
		return nil // no fully-covered blocks
	}

	freed := int64(0)
	for blkNum := firstBlock; blkNum <= lastBlock; blkNum++ {
		if err := m.backend.DeleteObject(volumeBucketName, blockKey(name, blkNum)); err == nil {
			freed++
		}
	}

	if freed > 0 && vol.AllocatedBlocks >= 0 {
		if vol.AllocatedBlocks-freed < 0 {
			vol.AllocatedBlocks = 0
		} else {
			vol.AllocatedBlocks -= freed
		}
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
		}
	}
	return nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test ./internal/volume/... -run "TestDiscard" -v
```

Expected: 모든 Discard 테스트 PASS

- [ ] **Step 5: 전체 테스트 통과 확인**

```bash
go test ./internal/volume/... -v
```

Expected: 모든 테스트 PASS

```bash
gofmt -w internal/volume/volume.go
```

- [ ] **Step 6: 커밋**

```bash
git add internal/volume/volume.go internal/volume/volume_test.go
git commit -m "feat(volume): Manager.Discard 구현 — 블록 삭제 + AllocatedBlocks 카운터 감소"
```

---

## Task 6: PoolQuota 사전 검사

**Files:**
- Modify: `internal/volume/volume.go` (WriteAt PoolQuota 분기)
- Modify: `internal/volume/volume_test.go`

- [ ] **Step 1: PoolQuota 실패 테스트 작성**

`internal/volume/volume_test.go`에 추가:

```go
func setupManagerWithQuota(t *testing.T, quota int64) *Manager {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	return NewManagerWithOptions(backend, ManagerOptions{PoolQuota: quota})
}

func TestPoolQuotaExceeded(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 2 blocks
	mgr := setupManagerWithQuota(t, int64(blockSize*2))
	_, err := mgr.Create("quota-test", int64(blockSize*4))
	require.NoError(t, err)

	// 2블록 쓰기 → 성공
	_, err = mgr.WriteAt("quota-test", make([]byte, blockSize*2), 0)
	require.NoError(t, err)

	// 1블록 더 쓰기 → quota 초과
	_, err = mgr.WriteAt("quota-test", make([]byte, blockSize), int64(blockSize*2))
	assert.ErrorIs(t, err, ErrPoolQuotaExceeded)
}

func TestPoolQuotaBoundary(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 2 blocks 정확히
	mgr := setupManagerWithQuota(t, int64(blockSize*2))
	_, err := mgr.Create("boundary-test", int64(blockSize*4))
	require.NoError(t, err)

	// 한 번에 2블록 쓰기 → 한도에 정확히 맞음 (통과)
	_, err = mgr.WriteAt("boundary-test", make([]byte, blockSize*2), 0)
	require.NoError(t, err, "write at exact quota limit should succeed")

	// 1바이트라도 새 블록 → 거부
	_, err = mgr.WriteAt("boundary-test", []byte{0x01}, int64(blockSize*2))
	assert.ErrorIs(t, err, ErrPoolQuotaExceeded, "write beyond quota limit should fail")
}

func TestPoolQuotaOverwriteDoesNotCount(t *testing.T) {
	const blockSize = DefaultBlockSize
	// quota = 1 block
	mgr := setupManagerWithQuota(t, int64(blockSize))
	_, err := mgr.Create("overwrite-quota", int64(blockSize*4))
	require.NoError(t, err)

	// 첫 쓰기 → 1블록 할당 (quota 소진)
	_, err = mgr.WriteAt("overwrite-quota", make([]byte, blockSize), 0)
	require.NoError(t, err)

	// 같은 블록 덮어쓰기 → 새 블록 없으므로 quota 검사 통과
	_, err = mgr.WriteAt("overwrite-quota", make([]byte, blockSize), 0)
	require.NoError(t, err, "overwrite of existing block should not be quota-limited")
}
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

```bash
go test ./internal/volume/... -run "TestPoolQuota" -v
```

Expected: FAIL (quota 검사 없으므로 모두 통과해버림)

- [ ] **Step 3: WriteAt에 PoolQuota 사전 검사 추가**

`WriteAt` 함수에서 `vol` 조회 직후, 쓰기 루프 진입 전에 아래 블록을 삽입한다:

```go
// Stage 1: PoolQuota pre-scan (only when quota is configured)
if m.opts.PoolQuota > 0 {
    // Count blocks in write range that don't yet exist (new allocations)
    newBlocksNeeded := int64(0)
    firstBlk := (off) / bs
    // lastBlk: largest block number that overlaps [off, off+len(p))
    lastBlk := (off + int64(len(p)) - 1) / bs
    if lastBlk >= vol.Size/bs {
        lastBlk = vol.Size/bs - 1
    }
    for blkNum := firstBlk; blkNum <= lastBlk; blkNum++ {
        if _, err := m.backend.HeadObject(volumeBucketName, blockKey(name, blkNum)); err != nil {
            newBlocksNeeded++
        }
    }

    // Sum allocated bytes across all cached volumes
    currentAllocated := int64(0)
    for _, v := range m.volumes {
        if v.AllocatedBlocks > 0 {
            currentAllocated += v.AllocatedBlocks * int64(v.BlockSize)
        }
    }

    if currentAllocated+newBlocksNeeded*bs > m.opts.PoolQuota {
        return 0, ErrPoolQuotaExceeded
    }
}
```

이 블록을 삽입할 위치는 `if off < 0 ...` 경계 검사 이후, `totalWritten := 0` 선언 이전.

완성된 WriteAt 흐름:

```go
func (m *Manager) WriteAt(name string, p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	vol, err := m.getVolUnlocked(name)
	if err != nil {
		return 0, err
	}

	if off < 0 || off >= vol.Size {
		return 0, fmt.Errorf("offset %d out of range [0, %d)", off, vol.Size)
	}

	bs := int64(vol.BlockSize)

	// Stage 1: PoolQuota pre-scan
	if m.opts.PoolQuota > 0 {
		newBlocksNeeded := int64(0)
		firstBlk := off / bs
		lastBlk := (off + int64(len(p)) - 1) / bs
		if lastBlk >= vol.Size/bs {
			lastBlk = vol.Size/bs - 1
		}
		for blkNum := firstBlk; blkNum <= lastBlk; blkNum++ {
			if _, err := m.backend.HeadObject(volumeBucketName, blockKey(name, blkNum)); err != nil {
				newBlocksNeeded++
			}
		}

		currentAllocated := int64(0)
		for _, v := range m.volumes {
			if v.AllocatedBlocks > 0 {
				currentAllocated += v.AllocatedBlocks * int64(v.BlockSize)
			}
		}

		if currentAllocated+newBlocksNeeded*bs > m.opts.PoolQuota {
			return 0, ErrPoolQuotaExceeded
		}
	}

	// Stage 2: write loop (existing logic with isNew tracking)
	totalWritten := 0
	newBlocks := 0

	for totalWritten < len(p) && off+int64(totalWritten) < vol.Size {
		pos := off + int64(totalWritten)
		blkNum := pos / bs
		blkOff := pos % bs

		blkData := make([]byte, vol.BlockSize)
		rc, _, err := m.backend.GetObject(volumeBucketName, blockKey(name, blkNum))
		isNew := err != nil
		if !isNew {
			io.ReadFull(rc, blkData)
			rc.Close()
		}

		canWrite := int(bs - blkOff)
		remaining := len(p) - totalWritten
		if canWrite > remaining {
			canWrite = remaining
		}
		if endPos := off + int64(totalWritten) + int64(canWrite); endPos > vol.Size {
			canWrite = int(vol.Size - pos)
		}

		copy(blkData[blkOff:blkOff+int64(canWrite)], p[totalWritten:totalWritten+canWrite])

		if _, err := m.backend.PutObject(volumeBucketName, blockKey(name, blkNum),
			bytes.NewReader(blkData), "application/octet-stream"); err != nil {
			return totalWritten, fmt.Errorf("write block %d: %w", blkNum, err)
		}

		if isNew {
			newBlocks++
		}
		totalWritten += canWrite
	}

	if newBlocks > 0 {
		if vol.AllocatedBlocks < 0 {
			vol.AllocatedBlocks = 0
		}
		vol.AllocatedBlocks += int64(newBlocks)
		data, err := marshalVolume(vol)
		if err == nil {
			m.backend.PutObject(volumeBucketName, metaKey(name), bytes.NewReader(data), "application/protobuf")
		}
	}

	return totalWritten, nil
}
```

- [ ] **Step 4: 테스트 통과 확인**

```bash
go test ./internal/volume/... -run "TestPoolQuota" -v
```

Expected: 모든 PoolQuota 테스트 PASS

- [ ] **Step 5: 전체 테스트 통과 확인**

```bash
go test ./internal/volume/... -v
```

Expected: 모든 테스트 PASS

```bash
gofmt -w internal/volume/volume.go
```

- [ ] **Step 6: 커밋**

```bash
git add internal/volume/volume.go internal/volume/volume_test.go
git commit -m "feat(volume): PoolQuota 사전 검사 — HeadObject 기반 pre-scan + ErrPoolQuotaExceeded"
```

---

## Task 7: NBD TRIM 지원

**Files:**
- Modify: `internal/nbd/nbd.go`

- [ ] **Step 1: NBD_CMD_TRIM 및 NBD_FLAG_SEND_TRIM 상수 추가**

`internal/nbd/nbd.go`의 상수 블록에서 `nbdCmdFlush = uint32(3)` 다음에 추가:

```go
nbdCmdTrim  = uint32(4)

// NBD_FLAG_SEND_TRIM: bit 5 (NBD protocol spec)
nbdFlagSendTrim = uint16(1 << 5)
```

- [ ] **Step 2: sendExportData에서 TRIM 플래그 협상**

`sendExportData` 함수의 플래그 라인을 수정:

```go
// 변경 전:
binary.BigEndian.PutUint16(buf[8:10], nbdFlagHasFlags|nbdFlagSendFlush)

// 변경 후:
binary.BigEndian.PutUint16(buf[8:10], nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim)
```

- [ ] **Step 3: handleOptGo에서 TRIM 플래그 협상**

`handleOptGo` 함수의 플래그 라인을 수정:

```go
// 변경 전:
binary.BigEndian.PutUint16(info[10:12], nbdFlagHasFlags|nbdFlagSendFlush)

// 변경 후:
binary.BigEndian.PutUint16(info[10:12], nbdFlagHasFlags|nbdFlagSendFlush|nbdFlagSendTrim)
```

- [ ] **Step 4: handleRequest에 NBD_CMD_TRIM case 추가**

`handleRequest` 함수의 switch문에서 `case nbdCmdFlush:` 다음에 추가:

```go
case nbdCmdTrim:
    if err := s.mgr.Discard(s.volName, int64(offset), int64(length)); err != nil {
        return s.sendReply(conn, handle, 5, nil) // EIO
    }
    return s.sendReply(conn, handle, 0, nil)
```

- [ ] **Step 5: Manager 인터페이스 확인 (nbd → volume 의존성)**

nbd 패키지가 `volume.Manager`를 직접 사용하므로 `Discard` 메서드가 노출됐는지 확인:

```bash
grep -n "Discard" internal/volume/volume.go
```

Expected: `func (m *Manager) Discard(` 라인 존재

- [ ] **Step 6: 빌드 확인**

```bash
go build ./internal/nbd/...
```

Expected: 에러 없음 (Linux 태그이므로 macOS에서 빌드 안 될 수 있음)

macOS에서:
```bash
GOOS=linux go build ./internal/nbd/...
```

Expected: 에러 없음

- [ ] **Step 7: 커밋**

```bash
git add internal/nbd/nbd.go
git commit -m "feat(nbd): NBD_CMD_TRIM(4) + NBD_FLAG_SEND_TRIM 협상 — Discard 연동"
```

---

## Task 8: API 응답 확장

**Files:**
- Modify: `internal/server/volume_handlers.go`

- [ ] **Step 1: volumeResponse에 allocated_bytes/allocated_blocks 필드 추가**

`internal/server/volume_handlers.go`의 `volumeResponse` 구조체를 수정:

```go
type volumeResponse struct {
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	BlockSize       int    `json:"block_size"`
	AllocatedBytes  int64  `json:"allocated_bytes"`  // -1=untracked, 0=empty, >0=bytes
	AllocatedBlocks int64  `json:"allocated_blocks"` // -1=untracked, 0=empty, >0=count
}
```

- [ ] **Step 2: 모든 volumeResponse 생성 위치를 AllocatedBytes/AllocatedBlocks 포함으로 수정**

`listVolumes`, `createVolume`, `getVolume` 함수에서 `volumeResponse{...}` 생성 부분을 수정.

`listVolumes`:
```go
result = append(result, volumeResponse{
    Name:            v.Name,
    Size:            v.Size,
    BlockSize:       v.BlockSize,
    AllocatedBytes:  v.AllocatedBytes(),
    AllocatedBlocks: v.AllocatedBlocks,
})
```

`createVolume`:
```go
c.JSON(consts.StatusCreated, volumeResponse{
    Name:            vol.Name,
    Size:            vol.Size,
    BlockSize:       vol.BlockSize,
    AllocatedBytes:  vol.AllocatedBytes(),
    AllocatedBlocks: vol.AllocatedBlocks,
})
```

`getVolume`:
```go
c.JSON(consts.StatusOK, volumeResponse{
    Name:            vol.Name,
    Size:            vol.Size,
    BlockSize:       vol.BlockSize,
    AllocatedBytes:  vol.AllocatedBytes(),
    AllocatedBlocks: vol.AllocatedBlocks,
})
```

- [ ] **Step 3: API 응답 테스트 추가**

`internal/volume/volume_test.go`에 추가:

```go
func TestAllocatedBytesInAPIResponse(t *testing.T) {
	mgr := setupManager(t)
	_, err := mgr.Create("api-test", int64(DefaultBlockSize*4))
	require.NoError(t, err)

	// 새 볼륨: untracked
	vol, err := mgr.Get("api-test")
	require.NoError(t, err)
	assert.Equal(t, int64(-1), vol.AllocatedBytes(), "new volume should be untracked")

	// 쓰기 후: AllocatedBytes = 1 block
	_, err = mgr.WriteAt("api-test", []byte("data"), 0)
	require.NoError(t, err)

	vol, err = mgr.Get("api-test")
	require.NoError(t, err)
	assert.Equal(t, int64(DefaultBlockSize), vol.AllocatedBytes())
	assert.Equal(t, int64(1), vol.AllocatedBlocks)
}
```

- [ ] **Step 4: 테스트 실행**

```bash
go test ./internal/volume/... -run TestAllocatedBytesInAPIResponse -v
```

Expected: PASS

- [ ] **Step 5: 빌드 확인**

```bash
go build ./internal/server/...
```

Expected: 에러 없음

- [ ] **Step 6: gofmt**

```bash
gofmt -w internal/server/volume_handlers.go
```

- [ ] **Step 7: 커밋**

```bash
git add internal/server/volume_handlers.go internal/volume/volume_test.go
git commit -m "feat(server): GET/POST /volumes 응답에 allocated_bytes, allocated_blocks 추가"
```

---

## Task 9: 전체 통합 검증 및 정리

**Files:**
- Read: `internal/volume/volume.go` (import 정리 확인)

- [ ] **Step 1: 전체 테스트 실행**

```bash
go test ./internal/volume/... ./internal/server/... -v -count=1
```

Expected: 모든 테스트 PASS

- [ ] **Step 2: 빌드 확인**

```bash
make build
```

Expected: 에러 없음

- [ ] **Step 3: strings import 제거 확인 (WriteAt에서 strings.NewReader 삭제됨)**

```bash
grep '"strings"' internal/volume/volume.go
```

Expected: 없음 (strings.NewReader를 bytes.NewReader로 교체했으므로)

만약 있다면 사용처를 확인하고 제거:
```bash
goimports -w internal/volume/volume.go
```

- [ ] **Step 4: lint 통과 확인**

```bash
go vet ./internal/volume/... ./internal/server/... ./internal/nbd/...
make lint
```

Expected: 경고/에러 없음

- [ ] **Step 5: TODOS.md 완료 처리**

`TODOS.md`에서 아래 항목을 제거한다 (완료됨):
```
- [ ] Thin provisioning (Phase A: TRIM + allocated_blocks + 공간 리포트) — spec: `docs/superpowers/specs/2026-04-25-thin-provisioning-design.md`
```

- [ ] **Step 6: 최종 커밋**

```bash
git add TODOS.md
git commit -m "docs: TODOS thin provisioning Phase A 항목 완료 처리"
```

---

## 부록: NBD TRIM Docker 통합 테스트 (선택)

> NBD 패키지는 Linux 전용. Docker로 실행해야 함.

`internal/nbd/nbd_test.go`에 추가:

```go
// TestNBDTrimIntegration은 Docker 환경에서 실행 (//go:build nbd_docker)
// fstrim -v로 TRIM 명령을 보내고 AllocatedBytes 변화를 확인한다.
// 실행: make test-nbd-docker
```

실제 Docker 테스트는 기존 `make test-nbd-docker` 인프라를 활용한다. 이 태스크는 Phase A 구현 완료 후 별도 PR로 추가.
