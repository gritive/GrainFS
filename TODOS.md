# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

- [ ] **Thin pool quota (cross-volume)** — 여러 볼륨이 공유하는 물리 용량 예산 풀. 볼륨별 `PoolQuota` 옵션(Phase A)보다 정교한 전체 클러스터 수준 quota 관리. Phase A 완료 이후.
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] **단일 블롭 손상 격리** — *zero ops* — 손상된 블롭 객체만 격리해 read-only로 표시; 동일 볼륨의 다른 객체는 정상 서비스 유지.

## Phase 17: Scale-Out

- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
- [ ] **Blame Mode v2 — shard-level 시각적 replay** — Phase 16은 텍스트 타임라인 + JSON download만, v2에서 shard 재생 UI
- [ ] **PagerDuty 네이티브 webhook 매핑** — Phase 16은 Slack-compatible JSON + docs 매핑만
- [ ] Sharding, multi raft
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] **NBD 인증 및 TLS 지원** — 현재 NBD 서버(`internal/nbd/nbd.go`)는 인증/암호화 없이 평문 TCP 익명 접속을 허용한다. 누구나 NBD 포트에 붙으면 read/write 가능 (cluster-key/access-key는 S3에만 적용). 표준 `NBD_OPT_STARTTLS` 옵션 처리 + TLS server cert + 선택적 client cert 인증 또는 사전 공유 키(PSK) 추가. 우선 TLS만이라도 활성화해 wire eavesdropping 차단. NBD 서버는 platform-agnostic이므로 Linux 외에서도 동일 적용.
- [ ] **Rolling upgrade safety** — *zero ops* — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)

## Phase 19: Performance

- [ ] **PUT handler latency breakdown 측정** — 2026-04-28 MinIO 비교 측정에서 grainfs PUT이 single 4KB 4x, cluster 64KB 5.3x 느림 (p50). pprof CPU 84% syscall이라 알고리즘 문제 아님 — PUT마다 발생하는 (1) BadgerDB metadata fsync, (2) raft ProposeWait, (3) EC shard fsync 직렬화 비용 추정. `handlePut` 안에 단계별 trace span 추가하고 metadata fsync 비중이 30%+면 BadgerDB WriteBatch group commit 또는 metadata-EC parallel write 검토. 결과 [`benchmarks/results-20260428-064151/ANALYSIS.md`](benchmarks/results-20260428-064151/ANALYSIS.md). 재오픈 조건은 이 측정 자체.
- [ ] **PUT crypto sync.Pool화** — 같은 측정에서 매 요청 `crypto/internal/fips140/sha256.New` 33MB, `hmac.New` 21MB allocs/30s. SigV4 검증 + ETag md5가 매 요청 객체 생성. sync.Pool로 재사용 → GC pause p99 감소 기대. 절대 throughput 영향은 작음 (CPU bound 아님). 트리거: GC pause p99 5ms 초과 시.
- [ ] **cluster 내부 QUIC PQ TLS handshake 검증** — `mlkem.NewDecapsulationKey768` + `cryptobyte.Builder` allocs 26MB+/30s 관찰. raft RPC connection은 long-lived여야 정상이나 매 RPC reconnect라면 큰 낭비. quic connection idle timeout / pooling 확인.
- [ ] **bench_compare.sh 1MB+ 시나리오 fix** — 현재 `s3_bench.js`의 `randomString(N)`을 매 iteration 호출하는데 N=1MB일 때 goja JS 엔진이 너무 오래 걸려 ramp-up 안에 첫 PUT조차 못 끝냄 (2026-04-28 측정에서 모든 시스템 1024KB가 0 ops). setup phase에 1회만 buffer 생성 후 재사용하도록 수정. → grainfs/minio 큰 객체 비교 가능하게.
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] **EC shard cache 사이즈 튜닝** — 본구현 완료 v0.0.4.42 (E2E 85.7% hit). 운영 telemetry(`grainfs_ec_shard_cache_hit_rate`)로 working set 측정 후 default 256 MB 적정성 검증. 큰 객체 백업 워크로드면 GB 단위까지, 작은 객체 위주면 비활성화 권장.
- [ ] **SPDK** — Phase 21+. NVMe direct user-space I/O로 io_uring 대비 1.5–3x 추가 throughput 가능. 구현 비용 큼(C library + cgo + kernel module 의존성), Linux 전용. 트리거: 운영 telemetry에서 io_uring path가 NVMe queue depth 포화 + p99 latency 한계 도달 시 재평가.
- [ ] **Predictive resource warnings — BadgerDB / goroutine / FD** — *zero ops* — BadgerDB value log 크기, goroutine 수, open FD 추세를 추적하고 임계 도달 전 경고. 디스크 사용률 경고와 동일 패턴(transition-only firing).

### Phase 21+ 후보 (측정 후 결정 / 현재 상태)

- **~~SIMD~~** — **NOT PURSUE (2026-04-28 측정)**. pprof 10s GET load 측정 결과 top 30 중 **98%가 `syscall.RawSyscall` + `runtime.kevent` + `runtime.pthread_cond_*`** (전부 syscall + scheduler). Reed-Solomon / AES / MD5 / CRC32 등 SIMD 대상 함수는 단 한 개도 sample에 등장 안 함 — GrainFS는 CPU compute bound가 아니라 I/O syscall bound. transitively-SIMD 라이브러리(klauspost/reedsolomon AVX-512, Go stdlib AES-NI, hardware CRC32)가 이미 모든 hot path 커버함이 측정으로 확인. **재오픈 조건**: 아키텍처 크게 달라지거나 (예: SPDK/io_uring 도입으로 syscall 비중 급감) 운영 telemetry에서 EC encode/decode가 5%+ CPU 차지 시.
- **SoA (Structure of Arrays)** — Go GC + pointer-heavy 코드베이스에서 cache benefit marginal. 후보(scrubber bulk metadata scan, placement records 배치 순회)도 sequential hot loop인지 확인 필요. 트리거: scrubber single-pass 30s+ 또는 BadgerDB iteration이 GC pressure 일으킬 때.
- **~~Raft leader 부담 경감 (commit-cycle 효율화)~~** — **NOT PURSUE (2026-04-28 측정)**. 3-node 클러스터에서 32-concurrent PUT 부하 (64 KB × 12s) 동안 leader/follower pprof 비교: Leader CPU 27.6% vs Follower 24.4% (**차이 3%, idle 70%+**). 둘 다 95%가 syscall + scheduler. propose / forwardPropose / ProposeWait 함수는 top 25에 한 번도 등장 안 함 — Raft commit이 hot path가 아님. Leader-only 추가 부하는 ETag md5(1.4%) + BadgerDB fsync madvise(1.2%) 정도로 작음. 코드 측에서도 `propose()` 12개 호출 전부 합의 필수(versioning/ETag/ring 일관성), bypass 가능 path 없음. read/list/shard-fetch/gossip/SSE/status는 이미 모두 leader 안 거치는 구조. **재오픈 조건**: 100+ node 클러스터 운영, PUT throughput 현재 5x 목표(10k+ PUT/s), 또는 propose-to-apply p99 SLO 위반 측정 시. batch propose / async commit / Raft pipelining / forwardPropose batching 후보는 그때 재평가.

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Hot reload drift detection** — *zero ops* — config 파일 시스템 도입 후, 런타임 reload 시 디스크 config와 메모리 상태 불일치 감지 + 명확한 에러. config 파일 시스템 자체가 선행 조건.
