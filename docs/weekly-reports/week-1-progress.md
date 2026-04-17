# Week 1 Progress - Protocol-Agnostic Storage Implementation

**Iteration 2 Status:** Cache Invalidation Infrastructure Complete ✅

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

**Iteration 1 Commit:** `a599a69` - "feat: week 1 protocol-agnostic storage progress"
- TDD test file
- Architecture documents
- Research documents

**Iteration 2 Commit:** `fdc6b51` - "feat: implement cache invalidation infrastructure"
- CacheInvalidator interface & Registry
- VFS Invalidate() method
- TDD tests for VFS and Registry
- NFS server registration

Next commits:
- Week 4: NFS ESTALE error propagation
- Week 5: NBD cache coherency
- Week 6: Cross-protocol integration tests

---

## Iteration 2 Summary (Completed)

**Achievement:** Cache invalidation infrastructure implemented and tested

**Components Built:**
1. **CacheInvalidator Interface** (`internal/cluster/invalidator.go`)
   - Defines contract for cache invalidation
   - Implemented by VFS (stat/dir caches)

2. **Registry** (`internal/cluster/invalidator.go`)
   - Manages multiple cache invalidators (VFS instances)
   - `Register(volumeID, invalidator)` method
   - `InvalidateAll(bucket, key)` broadcasts to all invalidators

3. **VFS Invalidate() Method** (`internal/vfs/vfs.go`)
   - Maps S3 bucket/key to VFS file path
   - Clears stat cache for specific file
   - Clears parent directory cache (so ReadDir reflects changes)
   - Respects bucket matching (no-ops for different buckets)

4. **DistributedBackend Integration** (`internal/cluster/backend.go`)
   - Added registry field
   - Modified `notifyOnApply()` to call `registry.InvalidateAll()`
   - Keeps legacy callback for CachedBackend

5. **NFS Server Registration** (`internal/nfsserver/nfsserver.go`)
   - Accepts registry parameter in constructor
   - Registers VFS instance on startup in `ListenAndServe()`

**Tests Written (TDD):**
- `TestVFSInvalidate`: Verifies stat cache cleared
- `TestVFSInvalidateDifferentBucket`: Verifies bucket filtering
- `TestRegistryRegister`: Verifies single invalidator
- `TestRegistryMultipleInvalidators`: Verifies broadcast to all
- `TestRegistryEmpty`: Verifies no-op with no invalidators

**Test Results:** All tests pass ✅

**Blocker:** E2E cross-protocol test still skipped (NFS client not integrated)

---

## Next Steps (Iteration 3)

1. **Option A:** Implement NFS client wrapper (mount.nfs approach)
   - Requires sudo/root
   - Platform-specific (Linux vs macOS)
   - Complex test setup

2. **Option B:** Unit test cross-protocol flow directly
   - Skip NFS mount complexity
   - Test VFS + backend interaction only
   - Faster iteration

3. **Option C:** Move to Week 4 (ESTALE implementation)
   - Defer NFS client integration
   - Focus on deleted file tracking
   - Implement ESTALE error propagation

**Recommendation:** Option C - move to ESTALE implementation while NFS client approach matures. Can circle back to integration testing later.
