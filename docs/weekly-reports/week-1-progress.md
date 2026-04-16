# Week 1 Progress - Protocol-Agnostic Storage Implementation

**Iteration 1 Status:** In Progress

## Completed Tasks

### 1. TDD Test Written ✅
- **File:** `tests/e2e/cross_protocol_test.go`
- **Test:** `TestCrossProtocolS3PutNFSStat`
- **Status:** Written and skipped (NFS client not integrated)
- **Purpose:** Documents expected behavior for S3 PUT → NFS stat cache invalidation

### 2. Architecture Design Documented ✅
- **File:** `docs/architecture/cache-invalidation-flow.md`
- **Decisions:**
  - VFS instances register with `DistributedBackend` via `CacheInvalidator` interface
  - S3 bucket/key → VFS file path mapping (strip `__grainfs_vfs_` prefix)
  - Eventual consistency window: 50-100ms Raft latency
  - Deleted file tracking via BadgerDB for ESTALE error propagation

### 3. NFS Client Spike Research ✅
- **File:** `docs/architecture/nfs-client-spike.md`
- **Approach:** Shell wrapper to `mount.nfs` CLI (recommended)
- **Fallback:** Unit test VFS directly if mounting proves complex
- **Platform considerations:** Linux vs macOS mount differences

### 4. Performance Baseline Script Created ✅
- **File:** `benchmarks/run-baseline.sh`
- **Tool:** k6 (existing benchmark suite)
- **Status:** Script ready, k6 not installed on current system
- **Action:** Skip for now, measure baseline in Week 2 during technical spike

## Remaining Week 1 Tasks

### 1. User Interviews (BLOCKED - User's Responsibility)
- **Task:** Interview 3 ML researchers
- **Questions:** "What's annoying about managing training data? Ever wished you could upload once and access as both S3 and filesystem?"
- **Decision Gate:** If ≥1 says "yes", continue. If all say "no", pivot.

### 2. Code Implementation (Week 3-6)
- **Week 3:** VFS cache invalidation methods
- **Week 4:** NFS ESTALE error propagation
- **Week 5:** NBD cache coherency
- **Week 6:** Cross-protocol integration tests

## Technical Debt / TODOs

1. **Install k6** for performance benchmarking
2. **Implement NFS mount wrapper** for E2E tests
3. **Add `CacheInvalidator` interface** to cluster package
4. **Implement VFS registration** mechanism
5. **Add VFS `Invalidate()` and `MarkDeleted()` methods**

## Next Steps (Iteration 2)

1. **Wait for user interviews** (Week 1 Day 1-2)
2. **If validated:** Start Week 3 implementation
   - Add `CacheInvalidator` interface to `internal/cluster/`
   - Implement VFS `Invalidate(bucket, key)` method
   - Wire up OnApply callback to iterate VFS instances
3. **If not validated:** Pivot to differentiating feature

## Files Created/Modified

**Created:**
- `tests/e2e/cross_protocol_test.go` - TDD test for cross-protocol cache invalidation
- `docs/architecture/cache-invalidation-flow.md` - Architecture design decisions
- `docs/architecture/nfs-client-spike.md` - NFS testing approach research
- `benchmarks/run-baseline.sh` - Performance baseline script

**Modified:**
- None yet (implementation starts Week 3)

## Commit Status

Current commit: `abceb34` - "feat: approve protocol-agnostic storage implementation plan"

Next commit should include:
- TDD test file
- Architecture documents
- Research documents
