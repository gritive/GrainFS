# TODOS

## Future Performance Optimizations
- [ ] Zero copy implementation for large files (sendfile/splice)
- [ ] NBD Zero copy E2E tests
- [ ] Compression data path E2E tests
- [ ] Profiling to verify buffer reuse in production workload

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
- [ ] Full NFSv4 E2E test coverage
- [ ] NBD Zero copy E2E tests
- [ ] Compression data path E2E tests
- [ ] Recovery and crash scenario tests

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
