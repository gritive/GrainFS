# Balancer Operator Runbook

GrainFS auto-balancer는 클러스터 노드 간 디스크 사용량 불균형을 자동으로 해소합니다.
이 문서는 balancer 운영·디버깅·튜닝에 필요한 절차를 기술합니다.

## 목차

1. [개요](#개요)
2. [설정 플래그](#설정-플래그)
3. [정상 동작 확인](#정상-동작-확인)
4. [메트릭 해석](#메트릭-해석)
5. [알람 대응 절차](#알람-대응-절차)
6. [튜닝 가이드](#튜닝-가이드)
7. [비상 중지](#비상-중지)

---

## 개요

Balancer는 Raft 리더 노드에서만 실행됩니다.

동작 흐름:

```
GossipSender  → QUIC 멀티캐스트로 각 노드 DiskUsedPct 전파
GossipReceiver → NodeStatsStore 갱신
BalancerProposer → 30초마다 디스크 불균형 평가
  imbalance > 20% → CmdMigrateShard Raft 제안
  imbalance < 5%  → 마이그레이션 중단 (hysteresis)
FSM.applyMigrateShard → MigrationExecutor.Execute()
  Phase1: shards src→dst 복사
  Phase2: CmdMigrationDone Raft 제안
  Phase3: FSM 커밋 확인 (NotifyCommit)
  Phase4: src 샤드 삭제
```

채널 풀 시 task가 BadgerDB `pending-migration:` 키에 영속화되어 재시작 후에도 복구됩니다.

---

## 설정 플래그

| 플래그                             | 기본값 | 설명                                                 |
| ---------------------------------- | ------ | ---------------------------------------------------- |
| `--balancer-enabled`               | `true` | balancer 활성화 여부                                 |
| `--balancer-gossip-interval`       | `30s`  | gossip + 불균형 평가 주기                            |
| `--balancer-imbalance-trigger-pct` | `20.0` | 마이그레이션 시작 임계값 (max-min %)                 |
| `--balancer-imbalance-stop-pct`    | `5.0`  | 마이그레이션 중단 임계값 (hysteresis)                |
| `--balancer-migration-rate`        | `1`    | tick당 최대 제안 수 (미래 rate limiting 용)          |
| `--balancer-leader-tenure-min`     | `5m`   | 리더 최소 보유 시간 (과부하 전 leader transfer 방지) |

예시 (프로덕션 보수적 설정):

```bash
grainfs serve \
  --balancer-gossip-interval=60s \
  --balancer-imbalance-trigger-pct=30.0 \
  --balancer-imbalance-stop-pct=10.0 \
  --balancer-leader-tenure-min=10m
```

---

## 정상 동작 확인

### 로그 확인

```bash
# balancer 시작 확인
journalctl -u grainfs | grep 'component=balancer'

# 마이그레이션 시작
journalctl -u grainfs | grep 'migrate\|migration'
```

정상 시퀀스:
```
INFO  balancer started component=balancer gossip_interval=30s trigger_pct=20
INFO  migration execute... component=migration task=mybucket/mykey/
INFO  migration complete  component=migration
```

### 보류 중인 마이그레이션 확인

BadgerDB에 `pending-migration:` 키가 많이 쌓여 있으면 migration channel이 지속적으로 backpressure 상태입니다.

```bash
# pending-migration 키 개수 조회 (grainfs doctor 또는 badger CLI 사용)
badger-cli list --prefix pending-migration: --db /data/meta | wc -l
```

---

## 메트릭 해석

| 메트릭                                       | 설명                           | 알람 기준                |
| -------------------------------------------- | ------------------------------ | ------------------------ |
| `grainfs_balancer_gossip_total`              | gossip 브로드캐스트 횟수       | gossip_errors > 0 지속   |
| `grainfs_balancer_migrations_proposed_total` | CmdMigrateShard 제안 횟수      | —                        |
| `grainfs_balancer_migrations_done_total`     | 완료된 마이그레이션 수         | —                        |
| `grainfs_balancer_migrations_failed_total`   | 실패한 마이그레이션 수         | > 0 지속 시 조사         |
| `grainfs_balancer_imbalance_pct`             | 현재 max-min 불균형 %          | > trigger_pct 지속       |
| `grainfs_balancer_pending_tasks`             | pending-migration DB 키 수     | > 10 지속 시 조사        |
| `grainfs_balancer_leader_transfers_total`    | 부하 기반 leader transfer 횟수 | 빈번하면 클러스터 불안정 |

---

## 알람 대응 절차

### 알람: 마이그레이션이 수시간 완료되지 않음

1. 리더 노드에서 로그 확인:
   ```bash
   journalctl -u grainfs | grep 'migration.*failed\|migration.*error' | tail -20
   ```

2. src 노드의 QUIC 연결 상태 확인:
   ```bash
   grainfs doctor --check-peers
   ```

3. dst 노드 디스크 여유 공간 확인:
   ```bash
   df -h /data
   ```

4. 마이그레이션 수동 중단 (긴급 시):
   ```bash
   # balancer 비활성화 후 재시작
   systemctl edit grainfs --force  # --balancer-enabled=false 추가
   systemctl restart grainfs
   ```

### 알람: pending-migration 키 증가

migration channel(크기 256)이 지속적으로 풀인 상태입니다.

원인:
- shard 복사 속도 < 마이그레이션 제안 속도
- `--balancer-gossip-interval` 값이 너무 짧음

대응:
```bash
# gossip 주기를 늘려 제안 빈도 감소
systemctl edit grainfs --force  # --balancer-gossip-interval=120s 추가
systemctl restart grainfs
```

### 알람: leader transfer 빈번

`grainfs_balancer_leader_transfers_total` 이 빈번하게 증가하면 특정 노드에 과부하가 집중됩니다.

1. 각 노드 `requests_per_sec` 확인 (gossip 로그 또는 Prometheus)
2. 핫 버킷/키 패턴 분석: `grainfs doctor --hot-keys`
3. 클라이언트 로드 밸런싱 설정 검토

---

## 튜닝 가이드

### 소규모 클러스터 (3-5 노드)

```bash
--balancer-gossip-interval=30s
--balancer-imbalance-trigger-pct=20.0
--balancer-imbalance-stop-pct=5.0
```

### 대규모 클러스터 (10+ 노드) — 마이그레이션 storm 방지

```bash
--balancer-gossip-interval=60s
--balancer-imbalance-trigger-pct=30.0
--balancer-imbalance-stop-pct=10.0
--balancer-leader-tenure-min=10m
```

### 신규 노드 추가 시

신규 노드 join 후 10분간 자동으로 balancer warm-up 대기합니다 (`--balancer-gossip-interval * WarmupTimeout` 기준). 이 동안에는 마이그레이션이 발생하지 않습니다.

---

## 비상 중지

balancer를 즉시 중지해야 하는 경우 (예: 대규모 장애 조사 중):

```bash
# 1. balancer 비활성화 (재시작 필요)
grainfs serve --balancer-enabled=false ...

# 2. 이미 실행 중인 마이그레이션은 자연 완료까지 대기
#    (진행 중인 shard 복사 중단 없음)

# 3. pending-migration 키 확인 후 필요 시 수동 삭제
#    (일반적으로 재시작 시 RecoverPending이 자동 처리)
```

진행 중인 migration을 강제 중단하면 src에 복사된 shard가 dst에 남을 수 있습니다.
이 경우 scrubber의 orphan shard 정리 기능이 자동으로 처리합니다 (Phase 14 예정).

---

## 테스트 / 개발

실제 디스크를 채우지 않고도 balancer 동작을 재현하려면 `GRAINFS_TEST_DISK_PCT` 환경 변수를 사용합니다.

```bash
# 로컬 노드의 디스크 사용률을 80%로 고정
GRAINFS_TEST_DISK_PCT=80 grainfs serve --data ./data --peers peer-a:9001
```

`GRAINFS_TEST_DISK_PCT`는 `DiskCollector`의 실제 `syscall.Statfs` 호출을 대체합니다.
값이 유효하지 않으면 서버가 시작 시 즉시 실패합니다.

Prometheus에서 디스크 사용률을 확인하려면:

```promql
grainfs_disk_used_pct{node_id="your-node-id"}
```
