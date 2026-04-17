# TODOS

## Future Performance Optimizations
- [x] Zero copy implementation for large files (sendfile/splice) ✓ DONE
  - Implemented: SetBodyStream with 16KB threshold
  - Performance: 73K req/sec (2288x improvement), 83µs avg latency (9.8x faster)
  - Tests: Integration tests passing, data integrity verified
- [x] Zero copy E2E tests (range requests) ✓ DONE
  - Range requests return 206 Partial Content with Content-Range header
  - parseByteRange handles start-end, suffix, open-end ranges
  - Range requests bypass zero-copy (use standard path)
- [x] Zero copy E2E tests (cold data, fault injection) ✓ DONE
  - Cold data integrity: all sizes (512B, 16KiB, 16KiB+1, 256KiB) verified
  - Backend error → 500; small file partial read → 500; large file (zero-copy) partial read → truncated body (200)
  - If-Unmodified-Since support added (was missing): past date → 412, future date → 200
  - Content-Length header verified for all paths (standard + zero-copy)
- [x] NBD Zero copy E2E tests ✓ DONE
  - TCP E2E: large block (1MB), block-aligned, unaligned offset, multiple clients
  - Server.Serve(net.Listener) added for testable injection
  - Runs via Docker (golang:1.26-bookworm, Linux); all pass
- [x] Compression data path E2E tests ✓ DONE
  - zstd compression with sync.Pool (encoder/decoder reuse)
  - Opt-out design: compression enabled by default in NewPackedBackend
  - Entry format: [key_len:4][key][flags:1][data_len:4][data][crc32:4]
  - Flags: 0x00=raw, 0x01=zstd compressed; only compresses when result is smaller
  - E2E: PackedBackend round-trip, large object passthrough, concurrent safety
- [x] Profiling to verify buffer reuse in production workload ✓ DONE
  - sync.Pool: 2 allocs/op (only output buffers), 5.7µs/round-trip on M3
  - Tests: AllocsBounded (≤6/op), HeapGrowthBounded (≤10 heap allocs/op over 1000 iters)

### Copy on Write (Phase 3)
- [ ] NBD Copy on Write in storage layer
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation

### Architecture Exploration
- [ ] S3-First MVP approach evaluation
- [ ] Market need analysis (S3-only vs unified storage)
- [ ] Decision on protocol prioritization

### Testing Expansion
- [x] Full NFSv4 E2E test coverage ✓ DONE
  - NULL, PUTROOTFH+GETFH, SETCLIENTID+CONFIRM, WRITE+READ, READDIR, GETATTR
  - READ at offset, READ beyond EOF, ACCESS, PUTFH, RENEW (27 tests total)
- [x] NBD Zero copy E2E tests (see Future Performance Optimizations above)
- [x] Compression data path E2E tests (see Future Performance Optimizations above)
- [x] Recovery and crash scenario tests ✓ DONE
  - BlobStore restart no longer overwrites existing blob files (nextID scans existing blobs)
  - ScanAll gracefully skips partial/truncated entries, preserving all complete entries
  - Multi-blob rotation recovery verified across blob file boundaries

### Next
- [ ] Thin provisoning
- [ ] http file serving, used by origin for cdn
- [ ] snapshot
- [ ] PITR
- [ ] automated shapshot and PITR
- [ ] WAL
- [ ] zero copy, zero allocation, sendfile
- [ ] PGO
- [ ] unix domain socket for internal communication, SCM_RIGHTS
- [ ] FlatBuffers, nmap
- [ ] io_uring
- [ ] SoA(Structure of Arrays)
- [ ] SIMD
- [ ] Unified buffer cache: Centeralized Page Cache
- [ ] Zero-copy Protocol Bridge (NFS to S3)
- [ ] Adaptive Raft Batching
- [ ] SPDK
- [ ] badgerdb: managed mode(raft)
- [ ] badgerdb: write batch
- [ ] badgerdb: TableLoadingMode
- [ ] badgerdb: LSM Read Amplification, bloom filter
- [ ] hertz: Zero-copy Read/Write
- [ ] Reed-Solomon + SIMD verify SIMD usage
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] smithy-go: io.WriteTo를 구현하여, FlatBuffers, zero-copy, zero-allocation 활용
- [ ] Erasure Coding을 활용한 Bit Rot 방지, 
- [ ] sync.Pool의 적극 활용
- [ ] Zstd & Reed-Solomon: 버퍼 재사용 with sync.Pool
- [ ] dashboard: raft health, badgerdb vlog gc status, erasure coding status
- [ ] dashboard: hot config change, log tailing, 
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging
- [ ] admin cli
- [ ] self healing; data scrubbing, lazy scrubbing, ec repair
- [ ] self healing: data balancing: capacity, hot spot balancing, rafe leader balancing
- [ ] decenteralized architecture
- [ ] split brain monitoring
- [ ] panic recovery
- [ ] IAM, Unified ACL: policy pre-compile
- [ ] ACL embedding
- [ ] LRU token validation
- [ ] zero copy permission check
- [ ] versioning
- [ ] soft-delete
- [ ] sharding, multi raft
- [ ] life cycle management
- [ ] nbd over internet for edge computing(powered by wireguard)
- [ ] migration; Pull-through Cache
- [ ] migration; injestor
- [ ] migration; nfs: virtual overlay
- [ ] migration; nbd: block proxing
- [ ] migration; adaptive throttling, priority queue
- [ ] protocol agnostic: 저장 계층(Badger)이 하나이므로, S3로 올린 파일을 NFS에서 바로 수정하고 NBD 블록으로 백업하는 흐름
