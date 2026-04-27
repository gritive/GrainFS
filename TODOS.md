# TODOS

> **Design principle — Zero Config, Zero Ops.** 기본 설정으로 잘 돌아가고, 잘 운영되며,
> 크리티컬한 문제는 사용자에게 알려서 선제대응하게 만든다.
> 각 Phase 항목에 "— *zero config*" / "— *zero ops*" 표시가 있는 것들이 이 원칙에 해당.

### 기타

- [ ] **Thin pool quota (cross-volume)** — 여러 볼륨이 공유하는 물리 용량 예산 풀. 볼륨별 `PoolQuota` 옵션(Phase A)보다 정교한 전체 클러스터 수준 quota 관리. Phase A 완료 이후.
- [ ] Memory usage validation
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] **Degraded mode (storage) — 단일 블롭 손상 격리** — Phase 1에서 클러스터 레벨 EC degraded 구현 완료. 단일 블롭 손상 시 해당 객체만 격리하는 후속 작업 남음.

## Phase 17: Scale-Out

- [ ] **BadgerDB atomic auto-recovery** — 이전 Phase 16에서 이연. log-based replay + snapshot restore 자체 구현 (단순 `badger.Open` 내장 복구를 넘어서는 원자적 복구 레이어)
- [ ] **Blame Mode v2 — shard-level 시각적 replay** — Phase 16은 텍스트 타임라인 + JSON download만, v2에서 shard 재생 UI
- [ ] **PagerDuty 네이티브 webhook 매핑** — Phase 16은 Slack-compatible JSON + docs 매핑만
- [ ] Sharding, multi raft
- [ ] Raft leader 부하 분산 검토 (follower proxy, read-only query, lease read 등)
- [ ] Migration: NFS virtual overlay
- [ ] Migration: NBD block proxying
- [ ] nbd over internet for edge computing (powered by wireguard)
- [ ] **Rolling upgrade safety** — *zero ops* — 버전 간 binary 교체로 downtime/데이터 손실 없음 (schema migration 자동, snapshot forward-compat 보장)

## Phase 19: Performance

- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] Unified buffer cache: Centralized Page Cache
- [ ] io_uring
- [ ] SPDK
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] **Predictive resource warnings — BadgerDB / goroutine / FD** — Phase 1에서 디스크 사용률 임계값 경고 구현 완료. BadgerDB value log 크기, goroutine/FD 추세 추적은 후속 작업.
- [ ] control plane, data plane 분리
- [ ] **QUIC 내부 통신 압축 도입 검토** — 클러스터 노드 간 QUIC 스트림에 압축(zstd/lz4) 적용 가능 여부 및 성능 트레이드오프 측정. 벤치마크 필수 (압축 CPU 비용 vs. 네트워크 절감); EC shard 데이터는 이미 랜덤 바이트이므로 압축 이득 미미할 수 있음 — gossip/receipt/metadata 트래픽 우선 검토.

## Phase 20: Protocol Extensions

- [ ] Redis 프로토콜 지원 (RESP, Streaming, Pub/Sub 이벤트)
- [ ] TSDB (Time Series DB) — Metric 저장 및 쿼리 지원

## Phase 20: Operations & Onboarding

운영자 개입 없이도 안정적으로 동작하고, 문제 발생 시 명확하게 알려주는 기본기.

- [ ] **One-command bootstrap** — *zero config* — `grainfs init` 하나로 cluster key, encryption key, 기본 credential, volume 생성 + 필요 파일 권한 설정
- [ ] **Config drift detection — hot reload 감지** — Phase 1에서 startup snapshot + 재시작 간 변경 감지 구현 완료. 런타임 hot reload 시 drift 감지는 config 파일 시스템 도입 후 가능.
