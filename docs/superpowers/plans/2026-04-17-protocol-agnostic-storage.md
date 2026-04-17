# Plan: Protocol-Agnostic Storage for ML Workloads

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Design source:** `~/.gstack/projects/gritive-grains/whitekid-master-design-20260417-033228.md`

**Goal:** Validate protocol-agnostic storage hypothesis — S3, NFS, and NBD accessing the same data simultaneously, enabling ML workflows without data duplication.

**Philosophy:** Validate through user interviews FIRST, then through practice. CEO review feedback: "Conviction without validation is stubbornness, not strategy." Interview 3 ML researchers in Week 1. If they care, build Approach A (Minimal Viable Unification) for user feedback. If they don't, pivot immediately.

**Timeline:** 12-16 weeks (3-4 months) — Revised per Eng review feedback: 8-10 weeks aggressive, needs buffers for distributed systems complexity

**Tech Stack:** Go 1.26+, BadgerDB metadata, Raft consensus, existing S3/NFS/NBD servers

**Status:** APPROVED — All reviews completed (CEO, Design, Eng, DX), feedback incorporated, ready for execution

---

## Developer Setup Guide

**REQUIRED READING before starting implementation.**

### Quick Start

1. **Install dependencies:**
   ```bash
   # Go 1.26+ required
   go version  # verify go1.26+

   # Clone and build
   git clone https://github.com/gritive/GrainFS
   cd GrainFS
   go build -o grainfs ./cmd/grainfs
   ```

2. **Run all three protocols locally for testing:**
   ```bash
   # Start GrainFS with S3, NFS, NBD enabled
   ./grainfs serve \
     --data ./tmp/grainfs \
     --port 9000 \
     --nfs-port 9002 \
     --nbd-port 9003 \
     --access-key test \
     --secret-key testtest

   # In another terminal, verify S3 API
   aws --endpoint-url http://localhost:9000 s3 ls

   # Mount NFS (macOS/Linux)
   mkdir -p /tmp/nfs-mount
   mount -t nfs -o port=9002,nolock localhost:/ /tmp/nfs-mount

   # Test NBD (Linux only, skip on macOS)
   nbd-client localhost 9003 /dev/nbd0
   ```

3. **Run tests:**
   ```bash
   # Unit tests
   go test ./internal/storage/... -v

   # E2E tests (requires Docker)
   go test ./tests/e2e/... -v
   ```

### Package Structure

- `cmd/grainfs/` — CLI entry point, serve command
- `internal/storage/` — Backend abstraction, cache, distributed storage
- `internal/vfs/` — Virtual filesystem layer (stat/dir caches, POSIX semantics)
- `internal/nfsserver/` — NFSv4 server implementation
- `internal/nbd/` — NBD block device server
- `internal/cluster/` — Raft consensus, FSM, membership
- `internal/server/` — S3 API HTTP server
- `tests/e2e/` — End-to-end tests (S3, NFS, NBD)

### Key Concepts

- **VFS (Virtual Filesystem)**: Abstraction layer providing POSIX-style file operations on top of object storage. Has stat/dir caches with TTLs.
- **Raft FSM**: Finite State Machine that replicates mutations via Raft consensus. All mutations (S3 PUT, NFS write, NBD write) go through Raft.
- **ESTALE Error**: NFS-specific error returned when file handle becomes invalid (e.g., file deleted after being opened).
- **Cache Invalidation**: Mechanism to clear local caches when data changes elsewhere. Critical for cross-protocol consistency.

### Debugging Guide

**Enable debug logging:**
```bash
# Set log level to debug
export GRAINFS_LOG_LEVEL=debug

# Or pass flag
./grainfs serve --data ./tmp --log-level debug
```

**Trace Raft operations:**
```bash
# Check Raft log entries
grep "Raft applied" /var/log/grainfs/production.log

# Check Raft consensus latency
grep "Raft commit" /var/log/grainfs/production.log | awk '{print $3}'
```

**Common issues:**

| Symptom | Cause | Fix |
|---------|-------|-----|
| NFS shows stale data after S3 upload | VFS cache not invalidated | Check if `vfs.InvalidateStatCache()` called in `onApply` callback |
| S3 GET returns old data after NFS write | S3 cache not invalidated | Check if `CachedBackend.InvalidateKey()` called |
| Tests flaky (timing issues) | Not waiting for Raft consensus | Add `time.Sleep(100 * time.Millisecond)` after mutations |
| NBD tests fail on macOS | NBD requires Linux | Skip NBD tests on macOS, use Linux VM or colima |

**Correlation IDs:** Every request has unique X-Request-ID header. Use it to trace operations across protocols:
```bash
# Extract correlation ID from logs
grep "X-Request-ID: abc123" /var/log/grainfs/*.log
```

### Platform-Specific Notes

**macOS Development:**
- Use colima for Docker-based testing
- NBD requires Linux VM: `colima start --vm-type vz --cpu 4 --memory 8`
- Skip NBD-specific tests on macOS (use build tags)

**Linux Development:**
- Native NBD support available
- Mount NFS with `mount -t nfs`
- Run full E2E test suite including NBD tests

---

## Premises

1. **ML teams struggle with protocol silos** — Data copied between S3, NFS, block storage creates operational overhead
2. **Value = same data, multiple protocols** — It's not just having multiple protocols, it's accessing identical bytes
3. **Single binary can be simpler than Ceph** — Go-based monolith can match Ceph's unification without enterprise complexity
4. **S3-only = competing with MinIO** — Without protocol unification, GrainFS has no differentiated value

All premises from office-hours session agreed by user.

---

## Week 1: User Validation + Technical Audit

**GOVERNED BY CEO REVIEW FEEDBACK:** User interviews happen FIRST, before implementation. If ML researchers don't validate the problem, we pivot immediately instead of wasting 6 weeks.

**Goal:** Validate that ML researchers actually care about protocol-agnostic storage AND understand technical feasibility.

**Deliverables:**
- Interview transcripts from 3+ ML researchers
- Decision document: continue/pivot based on validation
- Architecture diagram showing current dependencies (S3 → storage, NFS → storage, NBD → storage)
- Dependency graph identifying shared state (stat caches, object caches, in-memory structures)
- Cache coherency assessment report (VFS TTLs, S3 CachedBackend, NBD bypass)
- Risk assessment: Can protocols share storage layer without major refactor?

**Tasks:**
- [ ] **THE ASSIGNMENT (CRITICAL - DO THIS FIRST):** Interview 3+ ML researchers:
  - [ ] "What's the most annoying part of managing your training data?"
  - [ ] "Have you ever wished you could upload data once and access it as both object storage and a filesystem?"
  - [ ] Document their responses verbatim
- [ ] Parallel to interviews: **CRITICAL TECHNICAL TASKS** (per Design + Eng review feedback):
  - [ ] **Architecture Design** (must complete before implementation):
    - [ ] **Cache invalidation flow design:**
      ```go
      // Problem: DistributedBackend.onApply needs to invalidate VFS caches,
      // but VFS instances are per-volume and Backend is cluster-wide.

      // Solution: Backend maintains map of volumeID → VFS instance
      type DistributedBackend struct {
          vfsInstances sync.Map // volumeID → *vfs.VFS
      }

      // When volume created, register VFS
      func (b *DistributedBackend) RegisterVFS(volumeID string, v *vfs.VFS) {
          b.vfsInstances.Store(volumeID, v)
      }

      // OnApply callback: invalidate all VFS caches
      func (b *DistributedBackend) onApply(cmdType CommandType, bucket, key string) {
          b.vfsInstances.Range(func(id, v interface{}) bool {
              vfs := v.(*vfs.VFS)
              vfs.InvalidateStatCache(bucket, key) // TODO: implement this
              vfs.InvalidateDirCache(bucket)        // TODO: implement this
              return true
          })
          b.cache.InvalidateKey(bucket, key) // S3 cache invalidation
      }
      ```
    - [ ] **VFS deleted file tracking design:**
      ```go
      // Problem: NFS needs to return ESTALE when accessing deleted files.
      // Solution: Track deleted files in BadgerDB with TTL.

      type VFS struct {
          db *badger.DB // existing metadata DB
          deletedCache *lru.Cache // bucket+key → bool (fast path)
      }

      func (v *VFS) MarkDeleted(bucket, key string) error {
          // Write to BadgerDB for persistence
          return v.db.Update(func(txn *badger.Txn) error {
              return txn.Set([]byte("deleted:"+bucket+":"+key), []byte("1"))
          })
      }

      func (v *VFS) IsDeleted(bucket, key string) bool {
          // Check cache first
          if _, ok := v.deletedCache.Get(bucket + ":" + key); ok {
              return true
          }
          // Fallback to BadgerDB
          var err error
          v.db.View(func(txn *badger.Txn) error {
              _, err = txn.Get([]byte("deleted:" + bucket + ":" + key))
              return nil
          })
          return err == badger.ErrKeyNotFound
      }
      ```
    - [ ] **Cross-protocol error semantics design:**
      ```go
      // NFS handler returns ESTALE when file deleted
      func (h *Handler) GetAttr(ctx context.Context, f nfs.File) (nfs.FileAttr, error) {
          attr, err := h.vfs.Stat(ctx, f.Path())
          if h.vfs.IsDeleted(bucket, key) {
              return nfs.FileAttr{}, nfs.ErrStale
          }
          return attr, err
      }
      ```
  - [ ] **NFS Client Spike** (validate testing approach):
    - [ ] Research Go NFS client options (none exist for `willscott/go-nfs`)
    - [ ] Prototype `mount.nfs` shell wrapper for E2E tests
    - [ ] Test: Can we mount NFS in Docker? Does ESTALE propagate?
  - [ ] **Performance Baseline** (per Eng review):
    - [ ] Measure current single-protocol throughput (S3 PUT/GET ops/sec at P50/P99)
    - [ ] Measure current Raft commit latency (median, P95)
    - [ ] Document baseline numbers for regression comparison
    - [ ] Set up automated benchmark in CI (k6 script for cross-protocol scenarios)
  - [ ] "What's the most annoying part of managing your training data?"
  - [ ] "Have you ever wished you could upload data once and access it as both object storage and a filesystem?"
  - [ ] Document their responses verbatim
- [ ] **DECISION GATE:** If ≥1 researcher says "yes, this solves a real problem I have" → continue to Week 2
- [ ] **DECISION GATE:** If all researchers say "no, we just copy to NFS" → pivot to differentiating feature or use case
- [ ] Parallel to interviews: Analyze `internal/storage.Backend` interface — unified abstraction or protocol-specific?
- [ ] Map actual method signatures each protocol calls
- [ ] Identify protocol-specific backdoors around storage layer
- [ ] Document shared state: VFS stat/dir caches, S3 object caches, protocol-specific metadata
- [ ] Assess Raft FSM support for cache invalidation commands
- [ ] Deliver architecture diagram and risk assessment

**Why this order:** CEO reviews identified critical flaw — original plan deferred user validation to Week 7-8 AFTER implementation. This creates sunk cost bias. Validating FIRST prevents building things nobody wants.

**Technical decision gate:** If protocols bypass storage layer entirely, Approach A is infeasible. Pivot to Approach B (Clean Storage Abstraction) or Approach C (S3+NFS only).

---

## Week 2: Decision Gate + Technical Spike

**CRITICAL DECISION POINT:** Based on Week 1 user interviews AND technical spike results, decide whether to continue with protocol-agnostic storage or pivot.

**If ≥1 ML researcher validated ("this solves a real problem"):**
- [ ] Review interview quotes and extract specific pain points
- [ ] **TECHNICAL SPIKE (per Design + Eng review feedback):** Prototype cache coherency before full implementation
  - [ ] **TDD Workflow (project requirement):** Write failing tests FIRST, then implement
  - [ ] **Test 1: S3 PUT → NFS stat consistency**
    - [ ] Write test: `TestCrossProtocolS3PutNFSStat` (use TDD: write failing test first)
    - [ ] Implement: VFS cache invalidation via Raft FSM OnApply callback
    - [ ] Verify: NFS stat() sees new file within Raft latency + 100ms
  - [ ] **Test 2: S3 DELETE → NFS ESTALE**
    - [ ] Write test: `TestCrossProtocolS3DeleteNFSESTALE` (use TDD: write failing test first)
    - [ ] Implement: VFS deleted file tracking + NFS ESTALE error propagation
    - [ ] Verify: NFS stat() returns `nfs.ErrStale` after S3 DELETE
  - [ ] **Performance Measurements** (using Week 1 baseline):
    - [ ] Measure: Single-protocol S3 PUT/GET throughput (compare to baseline)
    - [ ] Measure: Cross-protocol S3 PUT → NFS read latency
    - [ ] Calculate: Performance regression = (cross_protocol_latency - single_protocol_latency) / single_protocol_latency
  - [ ] **Decision gate:**
    - [ ] If regression <20% → proceed with full 3-protocol implementation
    - [ ] If regression 20-30% → evaluate: can optimizations reduce to <20%?
    - [ ] If regression >30% → pivot to Approach C (S3+NFS only, defer NBD)
- [ ] Validate that ESTALE implementation works (S3 DELETE → NFS ESTALE)
- [ ] Validate that VFS cache invalidation works (S3 PUT → NFS stat sees new metadata)
- [ ] Update plan timeline with validated user need (10-12 weeks per CEO feedback)
- [ ] Proceed to Week 3-5: Cache Coherency Implementation

**If NO ML researchers validated (all say "no, we just copy to NFS"):**
- [ ] Document why hypothesis was wrong
- [ ] Propose alternative differentiation:
  - [ ] Performance: Benchmark vs MinIO, optimize hot paths
  - [ ] Simplicity: 5-minute setup vs MinIO's complexity
  - [ ] Cost: Compression, tiering, cold storage
- [ ] Present pivot options to project owner
- [ ] Do NOT proceed with implementation

**If technical spike shows regression >30%:**
- [ ] Document performance bottleneck (Raft latency? cache invalidation overhead? VFS cache misses?)
- [ ] Evaluate pivot to Approach C (S3+NFS only, simpler caching)
  - [ ] Remove NBD from initial implementation (defers 2x complexity)
  - [ ] Focus on S3+NFS cross-protocol cache coherency
  - [ ] Re-measure performance without NBD
- [ ] Present to project owner for go/no-go decision

**Deliverable:** Decision document with clear recommendation, technical spike results, and performance measurements.

---

## Week 3-6: Cache Coherency Implementation (Extended Timeline)

**Goal:** Implement Raft-based cache invalidation enabling cross-protocol data consistency.

**Deliverables:**
- Cross-protocol cache invalidation system (S3, NFS, NBD)
- Integration tests for all protocol combinations
- Performance benchmarks measuring invalidation overhead
- Decision: Continue or pivot based on <20% regression target

**Tasks (TDD Workflow - Write failing tests FIRST):**

**Week 3: VFS Cache Invalidation**
- [ ] **Design API** (from Week 1 architecture):
  - [ ] Add to `internal/vfs/vfs.go`:
    ```go
    func (v *VFS) InvalidateStatCache(bucket, key string) error
    func (v *VFS) InvalidateDirCache(bucket string) error
    ```
  - [ ] Add to `internal/storage/backend.go`:
    ```go
    func (b *DistributedBackend) InvalidateCaches(bucket, key string) error
    ```
- [ ] **Test (TDD):** Write `TestVFSCacheInvalidation` — verify stat cache cleared after invalidation
- [ ] **Implement:** VFS cache invalidation methods (mutex-protected map delete)
- [ ] **Test (TDD):** Write `TestRaftOnApplyInvalidatesVFS` — verify Raft Apply calls VFS invalidation
- [ ] **Implement:** Wire up `DistributedBackend.onApply` to call `vfs.InvalidateStatCache`

**Week 4: NFS ESTALE Error Propagation**
- [ ] **Design API** (from Week 1 architecture):
  - [ ] Add to `internal/vfs/vfs.go`:
    ```go
    func (v *VFS) MarkDeleted(bucket, key string) error
    func (v *VFS) IsDeleted(bucket, key string) bool
    ```
  - [ ] Add to `internal/nfsserver/handler.go`:
    ```go
    if v.IsDeleted(bucket, key) { return nfs.ErrStale }
    ```
- [ ] **Test (TDD):** Write `TestNFSSTALEAfterDelete` — verify NFS returns ESTALE after S3 DELETE
- [ ] **Implement:** VFS deleted file tracking (BadgerDB table: `deleted_files {bucket, key, timestamp}`)
- [ ] **Implement:** NFS ESTALE error propagation in getattr/readdir handlers
- [ ] **Test (TDD):** Write `TestNFSOpenDeletedFile` — verify NFS open fails with ESTALE

**Week 5: NBD Cache Coherency**
- [ ] **Design decision:** NBD bypasses VFS by design. Two options:
  - [ ] Option A: NBD writes trigger cache invalidation via Raft (adds Raft load)
  - [ ] Option B: NBD accepts eventual consistency (simpler, matches block device semantics)
  - [ ] **Decision:** Choose based on Week 2 spike results
- [ ] **If Option A (cache invalidation):**
  - [ ] **Test (TDD):** Write `TestNBDWriteInvalidatesS3` — verify S3 sees NBD write within Raft latency
  - [ ] **Implement:** Add NBD write → Raft `CmdPutObject` proposal
  - [ ] **Implement:** Raft Apply → S3 cache invalidation
- [ ] **If Option B (eventual consistency):**
  - [ ] **Test (TDD):** Write `TestNBDEventualConsistency` — verify S3 sees NBD write within TTL
  - [ ] **Document:** NBD writes have no cache invalidation, rely on TTL expiry

**Week 6: Cross-Protocol Integration Tests**
- [ ] **Test matrix (TDD):** Write failing tests for all scenarios (see test matrix below)
- [ ] **Implement:** Fix failures until all cross-protocol tests pass
- [ ] **Performance:** Run CI benchmarks (from Week 1) to measure regression
- [ ] **Decision gate:** If regression >20%, pivot to Approach C (S3+NFS only)

---

## Week 7-8: Integration & Testing (Extended Timeline)

**Goal:** Build integration tests proving same-byte access across protocols, validate data integrity.

**Deliverables:**
- Integration test suite covering cross-protocol test matrix
- Performance benchmarks (P50, P99, max latency)
- Failure injection test results (crash, partition, concurrent mutations)

**Test matrix:**
| Operation | Protocol A | Protocol B | Expected Behavior |
|-----------|------------|------------|-------------------|
| Write | S3 PUT | Read via NFS | Data visible within cache TTL |
| Write | NFS write | Read via S3 | Object updated within Raft latency |
| Delete | S3 DELETE | Open file in NFS | ESTALE error on next read |
| Snapshot | NBD snapshot | S3 list | Snapshot appears as object |
| Concurrent | S3 PUT + NFS read | Same key | Last writer wins (Raft serialization) |

**Tasks:**
- [ ] Implement integration test for S3 PUT → NFS read
- [ ] Implement integration test for NFS write → S3 GET
- [ ] Implement integration test for S3 DELETE → NFS ESTALE
- [ ] Implement integration test for NBD snapshot → S3 list
- [ ] Implement concurrent mutation test (S3 PUT + NFS read same key)
- [ ] Implement failure injection tests:
  - [ ] Crash during cross-protocol write
  - [ ] Network partition during S3 upload
  - [ ] NBD write failure after S3 upload
- [ ] Build performance benchmarks (cross-protocol latency histogram)
- [ ] Validate no data corruption under concurrent access
- [ ] Deliver test suite and benchmark results

---

## Week 9-10: Performance Tuning & Edge Case Handling (Extended Timeline)

**Goal:** Optimize cross-protocol performance, handle edge cases, polish for ML researcher preview.

**Deliverables:**
- Performance optimization report
- Final benchmarks showing regression <20%
- Observability instrumentation (metrics, traces, logs)
- Production-ready prototype for ML researcher testing
- Go/no-go decision on continued investment

**Tasks:**
- [ ] **CI/CD Automation** (per Eng review feedback):
  - [ ] Add cross-protocol tests to CI pipeline (tests from Week 7-8)
  - [ ] Set up automated performance regression detection (compare to Week 1 baseline)
  - [ ] Configure CI to fail if regression >20% (configurable threshold)
  - [ ] Add performance benchmark trends to CI dashboard
- [ ] Analyze performance bottlenecks from Week 7-8 benchmarks
- [ ] Optimize cache invalidation overhead (batch invalidations? reduce Raft round-trips?)
- [ ] Tune VFS cache TTLs for optimal consistency/performance tradeoff
- [ ] Optimize S3 cache hit rates (key patterns? access patterns?)
- [ ] **Implement observability:**
  - [ ] Metrics: `cross_protocol_latency_seconds`, `cache_invalidation_total`, `consistency_violations_total`
  - [ ] Logs: correlation IDs tracing operations across protocols
  - [ ] Traces: OpenTelemetry spans showing S3 → Raft → NFS data flow
- [ ] Re-run benchmarks, validate regression <20%
- [ ] Document performance characteristics
- [ ] Handle edge cases from error semantics table:
  - [ ] S3 DELETE fails while NFS has file open → NFS gets ESTALE
  - [ ] NBD write fails after S3 upload → S3 returns old data
  - [ ] Raft failure → all protocols return 503 Unavailable
- [ ] Build production binary with all three protocols
- [ ] Write quickstart guide for ML researchers
- [ ] Present findings and recommendation to project owner

**Decision gate:** Based on validated user need (from Week 1):
- **If validated:** Continue to Approach B (Clean Storage Abstraction) or scale to production
- **If not validated:** Already pivoted in Week 2

---

## Dependencies

**Blockers (must resolve before Week 2):**
- Raft log compaction architecture (raft-log-offset-required-for-snapshot learning)
- Cluster replication for bucket policies (bucket-policy-cluster-replication pitfall)

**Related work (can proceed in parallel):**
- Post-Phase 1 operational readiness drills (already complete)
- Existing E2E tests for S3 and NBD (expand NFS test coverage)

---

## Success Criteria

**User validation (Week 1 - CRITICAL GATE):**
- [ ] 3+ ML researchers interviewed BEFORE implementation
- [ ] ≥1 researcher says "this solves a real problem I have"
- [ ] Clear signal on whether to continue investment or pivot
- [ ] If no validation: pivot immediately without building

**Technical validation:**
- [ ] S3 upload creates file readable via NFS mount
- [ ] NFS file modification visible via S3 API immediately (within Raft latency)
- [ ] NBD snapshot captures S3+NFS changes atomically
- [ ] Integration tests pass for all protocol combinations
- [ ] No data corruption under concurrent cross-protocol access
- [ ] Performance regression <20% (single-protocol baseline vs cross-protocol)

---

## Error Semantics Reference

| Scenario | Protocol A | Protocol B | Expected Error |
|----------|------------|------------|----------------|
| S3 DELETE fails | NFS has file open | NFS sees stale metadata | ESTALE on stat() |
| NBD write fails | S3 upload succeeded | Data partially written | S3 GET returns old data |
| NFS rename | S3 GET in-progress | Inconsistent state | S3 returns old key name |
| Raft failure | All protocols | Write unavailable | 503 Unavailable |

---

## Risk Management

**Technical risks:**
- **High:** Cache coherency more complex than estimated → extends timeline or blocks Approach A
- **Medium:** Performance regression >20% → pivot to Approach C (S3+NFS only)
- **Medium:** Raft FSM doesn't support required mutation types → requires Raft extension

**User validation risks:**
- ~~**High:** Cannot find ML researchers willing to test experimental storage~~ **MITIGATED:** Interviews happen Week 1, before implementation. If we can't find researchers, we pivot immediately.
- ~~**High:** ML researchers don't care about protocol unification → pivot required~~ **MITIGATED:** Week 1 interviews validate problem before building. If no pain point, we don't build.
- **Medium:** Prototype has bugs that discourage early adopters

**Mitigation:**
- Week 1 user interviews + technical audit reveal feasibility before investment
- Week 2 decision gate prevents over-investment if users don't care or technical approach is infeasible
- Week 5 decision gate prevents over-investment if performance regression >20%

---

## Out of Scope

Deferred to future phases (may add to plan if validated):
- Approach B: Clean Storage Abstraction (full refactor if Approach A succeeds)
- Production hardening (deployment automation, monitoring, scaling)
- Additional protocols (SMB, iSCSI, WebDAV)
- Advanced ML features (dataset versioning, lifecycle management, automated ML pipeline integration)

---

## Open Questions

1. ~~**User discovery:** Where do we find ML researchers willing to try an experimental storage system?~~ **RESOLVED:** Interviews happen Week 1 before implementation. If we can't find 3 researchers or they don't validate the problem, we pivot immediately.
2. **Cache invalidation performance:** What's the overhead of invalidating VFS + S3 caches on every mutation?
3. **Concurrent mutation handling:** How do we serialize S3 PUT + NFS write to same key?
4. **Performance regression:** Will cross-protocol cache coherency kill single-protocol performance?
