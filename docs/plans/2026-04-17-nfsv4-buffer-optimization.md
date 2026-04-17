# NFSv4 Buffer Optimization for Large Files

**Date:** 2026-04-17
**Status:** Design Approved
**Priority:** High - Performance Optimization

## Problem Statement

Current NFSv4 READ/WRITE operations use default `io.Copy` with 32KB buffers. For large files (>10MB), this results in excessive syscalls and buffer allocations:
- 100MB file = ~3,000 syscalls
- 100MB file = ~3,000 buffer allocations
- High GC pressure from repeated allocations
- Suboptimal throughput (typically 30-50MB/s)

## Solution Overview

Implement adaptive buffer sizing with buffer pool reuse for NFSv4 operations:
- Small files (<1MB): 32KB buffers (current behavior)
- Medium files (1-10MB): 256KB buffers
- Large files (>10MB): 1MB buffers

**Expected improvement:**
- Throughput: 2-3x improvement for large files (target: >100MB/s read, >80MB/s write)
- Latency: 40-60% reduction in P99 for large file operations
- Syscalls: 30x reduction for 100MB files (3,000 → ~100)
- Memory: Minimal increase (buffer pool amortizes cost)

## Architecture

### Current State (Large File Read)

```
Client NFS READ request
  → compound.go (READ handler)
  → io.Copy with 32KB buffer
  → ~3,000 iterations [allocate → read → send → GC]
  → Response to client
```

### Optimized State (Large File Read)

```
Client NFS READ request
  → compound.go (READ handler)
  → bufferedCopy with 1MB buffer from pool
  → ~100 iterations [get buffer → read → send → return buffer]
  → Response to client (buffer reused, no GC)
```

## Components

### 1. Buffer Pool (`internal/nfs4server/buffer.go`)

New file implementing buffer management:

```go
type bufferPool struct {
    small *sync.Pool // 32KB buffers
    medium *sync.Pool // 256KB buffers
    large *sync.Pool // 1MB buffers
}

func (p *bufferPool) getBuffer(size int64) []byte
func (p *bufferPool) putBuffer(buf []byte)
func bufferedCopy(dst io.Writer, src io.Reader, size int64) error
```

**Pool configuration:**
- 3 pools: 32KB, 256KB, 1MB
- Max 10 buffers per pool (30MB total overhead)
- sync.Pool for GC-friendly reuse

### 2. NFSv4 Compound Handler (`internal/nfs4server/compound.go`)

Modify READ/WRITE operation handlers:

**Current:**
```go
io.Copy(nfsFile, payload)
```

**Optimized:**
```go
size := getContentLength() // or file size
buf := bufferPool.getBuffer(size)
defer bufferPool.putBuffer(buf)
bufferedCopy(dst, src, size)
```

### 3. VFS Layer (`internal/vfs/vfs.go`)

Ensure VFS ReadAt/WriteAt efficiently handle large buffers:
- No artificial limits on buffer sizes
- Add benchmarks for 10MB/100MB operations

## Data Flow

### Large File Read (>10MB)

1. Client sends NFSv4 READ request
2. compound.go detects large read (offset + count > 10MB)
3. Buffer pool provides 1MB buffer
4. Loop: read 1MB chunk → send → repeat
5. Return buffer to pool
6. Send final response

**Key optimization:** 30x fewer iterations (3,000 → 100)

### Large File Write (>10MB)

1. Client sends NFSv4 WRITE with payload
2. compound.go detects large payload
3. Buffer pool provides 256KB buffer
4. Loop: read 256KB chunk → WriteAt → next chunk
5. Flush and return buffer to pool
6. Send final response

**Key optimization:** Chunked writes prevent blocking on single large I/O

## Error Handling

### Buffer Pool Exhaustion

**Scenario:** All 10 buffers in use (e.g., 10 concurrent 100MB transfers)

**Fallback:**
```go
buf := bufferPool.getBuffer(size)
if buf == nil {
    // Pool exhausted, allocate temporary buffer
    buf = make([]byte, size)
    // Log warning once per minute
}
```

**Impact:** Slightly slower (no reuse), but no functional failure

### Memory Pressure

**Scenario:** System low on memory, allocation fails

**Fallback:**
```go
if allocErr != nil {
    // Reduce buffer size by 50%
    size = size / 2
    retryAllocation(size)
}
```

**Recovery:** Return to normal sizing when memory available

### Interrupted Operations

**Scenario:** Client disconnects during transfer

**Handling:**
1. Detect disconnect (read/write error)
2. Immediately return buffer to pool
3. VFS layer handles partial writes safely
4. Debug log: "Transfer interrupted at X MB"

### Oversized Requests

**Scenario:** Malformed 10GB READ request

**Validation:**
```go
const maxReadSize = 1 * 1024 * 1024 // NFSv4 spec limit
if count > maxReadSize {
    return NFS4ERR_TOO_BIG
}
```

**Safety:** Never allocate buffer larger than 1MB

### Concurrent Transfers

**Monitoring:**
- `nfs4_buffer_pool_gets_total` - counter
- `nfs4_buffer_pool_misses_total` - counter (pool exhausted)
- `nfs4_buffer_size_bytes` - gauge (current buffer sizes in use)

**Expected behavior:**
- 10 concurrent 100MB transfers = 10 buffers × 1MB = 10MB pool overhead
- If >10 concurrent: temporary allocations, higher miss rate

## Testing Strategy

### Unit Tests

**`internal/nfs4server/buffer_test.go`**

```go
func TestBufferPoolGetPutCycle(t *testing.T)
func TestBufferPoolExhaustionFallback(t *testing.T)
func TestAdaptiveBufferSizing(t *testing.T)
func TestConcurrentBufferAccess(t *testing.T)
```

**`internal/nfs4server/copy_bench_test.go`**

```go
func BenchmarkRead1MB(b *testing.B)
func BenchmarkRead100MB(b *testing.B)
func BenchmarkConcurrentReads100MB(b *testing.B)
```

### E2E Tests

**`tests/e2e/nfs4_largefile_test.go`**

```go
func TestNFSv4LargeFileRead(t *testing.T) {
    sizes := []int64{10*MB, 50*MB, 100*MB, 500*MB}
    for _, size := range sizes {
        // Upload file via S3
        // Read via NFSv4
        // Verify checksum
        // Measure throughput (target: >100MB/s)
    }
}

func TestNFSv4LargeFileWrite(t *testing.T) {
    sizes := []int64{10*MB, 50*MB, 100*MB}
    for _, size := range sizes {
        // Write via NFSv4
        // Read via S3
        // Verify checksum
        // Measure throughput (target: >80MB/s)
    }
}

func TestNFSv4ConcurrentLargeFiles(t *testing.T) {
    // 10 concurrent clients writing 100MB files
    // Verify no data corruption
    // Verify buffer pool doesn't leak
}

func TestNFSv4BufferPoolNoLeaks(t *testing.T) {
    // Perform 100 transfers of various sizes
    // Verify pool size returns to baseline
}
```

### Performance Validation

**Baseline measurement (before optimization):**
```bash
go test -bench=BenchmarkRead100MB -benchmem ./internal/nfs4server/
```

**After optimization:**
```bash
go test -bench=BenchmarkRead100MB -benchmem ./internal/nfs4server/
# Expect: 2-3x throughput improvement
# Expect: 30x fewer allocations
```

### Acceptance Criteria

- ✅ 100MB file read throughput: >100MB/s
- ✅ 100MB file write throughput: >80MB/s
- ✅ Buffer pool hit rate: >90% for consecutive transfers
- ✅ No buffer leaks (pool size stable after 100 transfers)
- ✅ Small file performance: within 10% of baseline (no regression)
- ✅ Concurrent transfers: handle 10+ simultaneous large files
- ✅ Error recovery: graceful degradation under memory pressure

## Implementation Checklist

- [ ] Create `internal/nfs4server/buffer.go` with buffer pool
- [ ] Add unit tests for buffer pool
- [ ] Modify READ handler in `compound.go` to use adaptive buffers
- [ ] Modify WRITE handler in `compound.go` to use adaptive buffers
- [ ] Add Prometheus metrics for buffer pool
- [ ] Create `internal/nfs4server/copy_bench_test.go` benchmarks
- [ ] Create E2E tests in `tests/e2e/nfs4_largefile_test.go`
- [ ] Run E2E tests and validate performance improvements
- [ ] Update ROADMAP.md with Phase 11: Performance Optimization
- [ ] Update TODOS.md (remove completed items, add remaining work)

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Buffer pool grows unbounded | Low | High (memory leak) | Unit tests + E2E leak tests, sync.Pool auto-cleanup |
| Small files slow down | Medium | Medium (regression) | Benchmark small files, keep 32KB for <1MB |
| Memory overhead too high | Low | Medium | Limit pool to 10 buffers per size (30MB total) |
| Concurrent contention | Medium | Low | Pool exhaustion fallback to temp allocation |
| Platform-specific issues | Low | Low | Pure Go implementation, no syscalls directly |

## Success Metrics

**Performance:**
- Large file throughput: 2-3x improvement
- Large file P99 latency: 40-60% reduction
- Syscall count: 30x reduction for 100MB files

**Reliability:**
- No buffer leaks in E2E tests
- Graceful degradation under memory pressure
- No regression in small file performance

**Observability:**
- Prometheus metrics for buffer pool health
- Clear logs for fallback conditions
- Benchmark comparisons in CI/CD

## Open Questions

None - design is complete and ready for implementation.

## References

- Current NFSv4 implementation: `internal/nfs4server/compound.go`
- VFS layer: `internal/vfs/vfs.go`
- Similar optimization: Phase 8 cache performance work (showed 120x GetObject improvement)
- NFSv4 spec: RFC 5661 (https://tools.ietf.org/html/rfc5661)
