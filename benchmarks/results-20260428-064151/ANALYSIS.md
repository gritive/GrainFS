# GrainFS vs MinIO 벤치마크 분석 (2026-04-28)

## TL;DR

- **GET은 동급 또는 grainfs 우위** (4KB single grainfs p50 0.44ms vs minio 0.77ms — 0.57x).
- **PUT은 grainfs가 일관되게 느림**: single 4KB 4.0x, cluster 64KB 5.3x slower in p50.
- 격차의 주 원인은 **CPU 알고리즘이 아니라 I/O syscall**. CPU profile 84%가 syscall + scheduler, grainfs 비즈니스 로직은 거의 보이지 않음.
- 따라서 SIMD나 Reed-Solomon 최적화는 의미 없음 (이미 검증된 NOT_PURSUE 결론과 동일).
- 진짜 병목은 (1) PUT path의 metadata fsync 횟수, (2) 매 요청 crypto allocation, (3) 1MB+ 큰 객체에서 측정 도구 자체의 한계.

## 환경

- darwin/arm64, single machine (Mac), 4-node cluster은 4 process × 1 drive
- grainfs 5148b69, EC 2+2, encryption off, rate limit 사실상 무한
- minio community RELEASE.2025-09-07, 4-drive auto erasure, encryption off
- k6: VUs 20, 30s steady (45s total ramp 포함), `s3_bench.js` mixed PUT→GET→50% DELETE

## 결과 요약

### Latency p50 (ms) — grainfs / minio ratio (작을수록 grainfs 우위)

| op | mode | 4KB | 64KB | 1024KB |
|----|------|-----|------|--------|
| PUT | single  | 4.01x | 1.48x | (도구 한계) |
| PUT | cluster | 3.81x | 5.29x | (도구 한계) |
| GET | single  | **0.57x** | 1.10x | (도구 한계) |
| GET | cluster | **0.44x** | 1.09x | (도구 한계) |
| DEL | single  | 4.72x | 1.57x | (도구 한계) |
| DEL | cluster | 2.77x | 0.71x | (도구 한계) |

### 1024KB가 0 ops로 나오는 이유

모든 시스템에서 동일하게 0 ops + failed 1. grainfs/minio 둘 다이므로 **k6 측정 도구의 한계**:
- `s3_bench.js`의 `randomString(1024*1024)`가 매 iteration마다 1MB JS string 생성 → goja JS 엔진에서 100ms+ 소요 추정
- ramp-up 10s 동안 첫 PUT조차 끝나지 못함

**Fix 후속**: 큰 사이즈는 setup 시점에 1회만 buffer 생성 후 매 iteration 재사용. → TODOS 항목 추가

## pprof 분석 (cluster mode, 첫 size = 4KB, 25s CPU profile)

### CPU (8.15s sample, 32% utilization)
```
57.91%  syscall.rawsyscalln              ← fsync, kqueue, mmap madvise 등
 7.48%  runtime.pthread_cond_wait
 6.38%  runtime.kevent
 5.77%  runtime.pthread_cond_signal
 3.56%  runtime.madvise                  ← BadgerDB value log
 3.44%  syscall.RawSyscall
─────────────────────────────────────────
 84%    syscall + scheduler
```

PutObject path만 focus 시 cum 2.38s 중 96%가 syscall. **CPU가 idle하면서 fsync를 기다림** — 전형적인 I/O bound 패턴.

### Allocs (1.05GB, 30s)

```
332MB  badger/v4/skl.newArena         ← BadgerDB MemTable arena (정상)
160MB  io.ReadAll                      ← request body
 33MB  crypto/internal/fips140/sha256.New     ← SigV4 매 요청 (개선 가능)
 32MB  raft.fbFinishRPC                ← Raft RPC FlatBuffer encode
 21MB  crypto/internal/fips140/hmac.New       ← SigV4 매 요청
 18MB  crypto/internal/fips140/aes.New
 15MB  badger.Txn.Get
 14MB  crypto/internal/fips140/mlkem.NewDecapsulationKey768  ← QUIC PQ TLS 1.3 hybrid
 12MB  cryptobyte.(*Builder).add       ← TLS handshake
 11MB  fips140/aes/gcm.New
 10MB  fips140/aes/gcm.NewGCMForTLS13
```

크립토 객체 매 요청 새로 만드는 합계 약 100MB+. `sync.Pool`로 재사용 시 GC pressure 감소.

### Mutex (1714ms total delay)

```
1687ms  sync.(*Mutex).Unlock  (98.44%)
  24ms  runtime.unlock         (1.41%)
```

30s 부하 중 1.7s = 5.6% 평균. 큰 contention 아니지만 어디서 오는지 추적 가치 있음 (cum tree 분석으로 BadgerDB Txn locking 추정).

## 격차 원인 진단

### 1. Single 모드 4KB PUT — grainfs 4x 느림 (6.10ms vs 1.52ms)

같은 머신, 1-node, EC 없음. raft 없음. 단순 PutObject path.

격차 후보:
- **BadgerDB metadata write + fsync**: grainfs는 모든 PUT마다 metadata KV 쓰기 (object meta, version, ETag) → BadgerDB SST + value log fsync. minio는 erasure encoded blob 안에 metadata inline.
- **Crypto pipeline**: SigV4 sha256/hmac 검증 + ETag md5 + (optional) at-rest AES — 매 요청 객체 생성.
- **Hertz/netpoll vs minio 자체 HTTP**: 가능성 있지만 benchmark 1KB scale에서 차이 ~ms 수준.

### 2. Cluster 모드 64KB PUT — grainfs 5.3x 느림 (23.74ms vs 4.49ms)

- **Raft propose 동기화**: 매 PUT은 ProposeWait → leader가 majority commit + apply 대기.
- **4-shard parallel write**: EC 2+2이므로 4개 노드가 각자 shard를 fsync.
- **gossip overhead**: heal-receipt gossip + ring update.
- **PQ TLS handshake** (mlkem allocs): cluster 내부 quic 통신이 매 새 connection마다 hybrid post-quantum key exchange.

minio도 distributed에서 N/2+1 write quorum이지만 metadata가 별도 layer 없음. propose-then-apply 같은 동기화 단계 없음.

### 3. GET은 grainfs 우위 (4KB cluster 0.44x)

GrainFS의 narrow Unified Buffer Cache + EC shard cache가 효과적으로 동작 중 (Phase 2 #3 작업 결과). minio는 매번 erasure decode가 필요할 수 있음 (작은 객체에선 cache hit 안 되면 비쌈).

## 개선 방향

각 항목은 trigger와 예상 효과를 함께 기록한다.

### A. 매 요청 crypto sync.Pool화 (작은 ROI, 작은 비용)
- 대상: `sha256.New` (33MB), `hmac.New` (21MB), MD5 ETag (별도 측정 필요)
- 영향: GC pause 감소 → tail latency p99 개선 가능. 절대 throughput 영향은 작음 (CPU 84%가 syscall이므로).
- **트리거**: 운영 telemetry에서 GC pause p99가 5ms 초과 시.

### B. PUT path metadata fsync 합치기 / 비동기화 (가장 큰 ROI)
- 가설: PUT마다 BadgerDB metadata fsync + EC shard write fsync가 직렬 발생.
- 측정 필요: PUT handler의 단계별 latency breakdown (`handlePut` 내 트레이스).
- 후보:
  - BadgerDB `WriteBatch` group commit
  - metadata write를 EC shard write와 병렬화
  - WAL 한 번 fsync로 묶기
- 위험: durability 약화 가능. 테스트로 검증 필수.
- **트리거**: 실제 측정에서 metadata fsync가 PUT 총 latency의 30%+ 차지하는지 확인 후.

### C. cluster 내부 QUIC PQ TLS handshake 비용 검증
- 매 raft RPC connection 시 mlkem decapsulation key 생성 (14MB allocs/30s).
- raft 노드 간 connection은 long-lived여야 정상. 만약 매 RPC마다 reconnect면 큰 낭비.
- **확인 필요**: quic connection lifecycle. connection idle timeout이 너무 짧으면 늘리기.

### D. 1MB+ 측정 도구 개선 (즉시)
- `s3_bench.js`에서 `randomString(N)`을 setup 시 1회만 호출, 매 iteration은 동일 buffer 재사용.
- TODOS 항목으로 추가.

### E. Raft propose batching 재평가 (조건부)
- 이전 (2026-04-28) 측정에서 NOT_PURSUE 결론이었으나 그건 leader-only CPU 분석.
- 현재 측정에서 propose-to-apply latency가 PUT 격차의 큰 부분이라면 재평가 가능.
- **트리거**: handlePut 단계별 latency 측정에서 propose 단계가 5ms+ 차지 확인 시.

## NOT_PURSUE 재확인

- **SIMD**: CPU 84% syscall, EC encode/AES/hash가 sample에 거의 안 잡힘. 이전 결론(2026-04-28) 그대로.
- **Raft leader CPU 분산**: 본 측정에서도 leader CPU의 propose 함수가 top에 안 잡힘. 이전 결론 유효.

## 측정 한계

1. **단일 머신**: grainfs와 minio가 같은 disk를 쓰지만 순차 실행이라 경합은 없음. 다만 진짜 distributed 네트워크 latency (RTT 0.1ms+)는 측정 못 함.
2. **macOS**: Linux의 io_uring이나 production NVMe 환경과 다름. fsync 비용이 macOS에서 크게 잡힘 (APFS 안전 대기).
3. **k6 sleep(0.01)**: throttle. 실제 throughput 한계가 아니라 latency 위주 측정. throughput 비교는 ratio로만 의미 있음.
4. **1MB+ 시나리오 미측정**: 도구 한계. 후속에서 fix.

## 결론

GrainFS는 GET path에서 minio와 동급 또는 우위지만, **PUT path가 single에서도 4x, cluster에서 최대 5.3x 느리다**. 원인은 CPU 알고리즘이 아니라 PUT마다 발생하는 metadata fsync + Raft propose 동기화의 직렬 I/O 단계 횟수다. SIMD/Raft commit-cycle 같은 후보는 이번에도 NOT_PURSUE이고, 진짜 다음 수는 **PUT handler의 단계별 latency breakdown 측정 → metadata fsync 합치기**다.
