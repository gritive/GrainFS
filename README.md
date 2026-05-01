# GrainFS

경량 분산 객체 스토리지. 싱글 바이너리로 S3 호환 스토리지를 즉시 실행하고, 필요하면 클러스터로 확장한다.

**Object Storage (S3 API) + Block Storage (NBD) + File Storage (NFS)** 를 하나의 바이너리로 제공.

## Quick Start

```bash
# 빌드
make build

# 실행 — 2초 내 S3 엔드포인트 동작
./bin/grainfs serve --data ./storage --port 9000

# 사용
aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test/
```

서버 시작 시 `default` 버킷이 자동 생성된다. 브라우저에서 `http://localhost:9000/ui/` 로 Object Browser 접근 가능.

## Features

| 기능               | 설명                                                          |
| ------------------ | ------------------------------------------------------------- |
| S3 API             | PUT, GET, HEAD, DELETE, LIST, Multipart Upload, Presigned URL |
| Erasure Coding     | Solo: Reed-Solomon 4+2 / Cluster: N× replication (Phase 18 예정) |
| QUIC Transport     | quic-go 기반 멀티플렉싱, TLS 1.3 내장                         |
| Custom Raft        | QUIC 위 합의, 리더 선출/로그 복제/스냅샷                      |
| 단일 노드 → Cluster     | 무중단 클러스터 전환                                          |
| Volume Device      | NBD (Linux) + NFS v3/v4.0 (macOS/Linux)                       |
| Object Browser     | 웹 UI에서 버킷/오브젝트/볼륨 관리                             |
| At-rest Encryption | AES-256-GCM, 키 자동 생성                                     |
| Monitoring         | Prometheus 메트릭 + 대시보드                                  |

## CLI Options

```
grainfs serve [flags]

Flags:
  -d, --data string              데이터 디렉토리 (default "./data")
  -p, --port int                 HTTP 포트 (default 9000)
      --nfs-port int             NFS v3 포트 (default 9002, 0=비활성)
      --nfs4-port int            NFS v4.0 포트 (default 2049, 0=비활성)
      --nbd-port int             NBD 포트 (default 0=비활성, Linux only)
      --nbd-volume-size int      기본 NBD 볼륨 크기 바이트 (default 1073741824 = 1GB)
      --ec                       Erasure Coding 활성화 (default true, 단일 노드 모드 전용)
      --ec-data int              데이터 샤드 수 (default 4, 단일 노드 모드 전용)
      --ec-parity int            패리티 샤드 수 (default 2, 단일 노드 모드 전용)
      --access-key string        S3 인증 Access Key
      --secret-key string        S3 인증 Secret Key
      --encryption-key-file      암호화 키 파일 경로
      --no-encryption            암호화 비활성화
      --node-id string           노드 ID (클러스터 모드)
      --raft-addr string         Raft 주소 (클러스터 모드)
      --peers string             피어 목록 (클러스터 모드)
      --badger-managed-mode      Raft 로그 GC 활성화. 활성화 시 on-disk 포맷 변경; 이후 플래그 없이 재시작 불가
                                 (상세: docs/badger-managed-mode-rollback.md)
      --raft-log-gc-interval duration Raft 로그 GC 실행 주기 (default 30s, 0=비활성)

  Balancer (클러스터 모드 전용):
      --balancer-enabled                    디스크 자동 균형 활성화 (default true)
      --balancer-gossip-interval duration   불균형 평가 주기 (default 30s)
      --balancer-imbalance-trigger-pct      마이그레이션 시작 임계값 % (default 20)
      --balancer-imbalance-stop-pct         마이그레이션 중단 임계값 % (default 5)
      --balancer-migration-rate int         tick당 최대 제안 수 (default 1)
      --balancer-leader-tenure-min duration 리더 최소 보유 시간 (default 5m)
      --balancer-warmup-timeout duration    노드 시작 후 마이그레이션 유예 시간 (default 60s)
      --balancer-cb-threshold float         Circuit Breaker 임계값 — 디스크 사용률 fraction (default 0.90)
      --balancer-migration-max-retries int  shard write 최대 재시도 횟수 (default 3)
      --balancer-migration-pending-ttl duration 좀비 마이그레이션 자동 취소 TTL (default 5m)
```

### Recovery Commands

```bash
grainfs recover --dry-run --data /var/lib/grainfs

grainfs recover cluster plan \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100

grainfs recover cluster execute \
  --source-data /var/lib/grainfs \
  --target-data /var/lib/grainfs-recovered \
  --new-node-id node-recovered \
  --new-raft-addr 10.0.0.10:19100

grainfs recover cluster verify \
  --target-data /var/lib/grainfs-recovered \
  --mark-writable
```

`recover --auto`는 더 이상 데이터를 변경하지 않고 실패한다. 다수결을 잃은 클러스터는 먼저 `recover cluster plan`으로 offline source를 읽기 전용 검사한 뒤 fresh target에 복구한다.

상세 절차: [docs/recover-cluster.md](docs/recover-cluster.md)

## 클러스터 Balancer

클러스터 모드에서 노드 간 디스크 불균형이 20% 이상이면 자동으로 샤드를 이동한다.

### 상태 확인

```bash
curl http://localhost:9000/api/cluster/balancer/status | jq .
```

응답 예시:
```json
{
  "available": true,
  "active": false,
  "imbalance_pct": 12.3,
  "nodes": [
    {"node_id": "node-a", "disk_used_pct": 62.1, "disk_avail_bytes": 38654705664},
    {"node_id": "node-b", "disk_used_pct": 49.8, "disk_avail_bytes": 53687091200}
  ]
}
```

`active: true`이면 마이그레이션 진행 중. `imbalance_pct`가 5% 미만으로 내려가면 자동 중단.

> 상세 운영 가이드: [docs/operations/balancer.md](docs/operations/balancer.md)

## Documentation

- [Backup and restore](docs/BACKUP_RESTORE.md)
- [Disaster recovery drill log](docs/DISASTER_RECOVERY.md)
- [Drill manual](docs/DRILL_MANUAL.md)
- [Production runbook](docs/RUNBOOK.md)
- [SLI/SLO](docs/SLI_SLO.md)
- [RecoverCluster drill](docs/recover-cluster.md)
- [Badger managed mode rollback](docs/badger-managed-mode-rollback.md)

## Development

### 요구사항

- Go 1.26+

### 빌드 & 테스트

```bash
make build          # 바이너리 빌드
make test           # 전체 테스트
make test-race      # race detector 포함
make test-e2e       # E2E 테스트
make lint           # go vet + gofmt 검사
```

### 벤치마크

[k6](https://k6.io/) 설치 후:

```bash
make bench
```

S3 PUT/GET/DELETE throughput과 P50/P99 지연을 측정한다. 결과는 `benchmarks/report.json`에 저장.

### FUSE-over-S3 마운트 (rclone/s3fs/goofys)

GrainFS는 **표준 S3-compatible** API를 제공하므로, 별도 클라이언트 바이너리 없이 기존 FUSE-over-S3 도구로 마운트할 수 있다. 클라이언트 머신에 GrainFS 바이너리 설치는 불필요하다.

**예: rclone (Linux 클라이언트, macOS 서버)**

```bash
# 1. (서버) GrainFS 시작 — auth 필수 (rclone v4 sig 사용)
grainfs serve --port 9000 --access-key <ak> --secret-key <sk>

# 2. (클라이언트) rclone config 작성: ~/.config/rclone/rclone.conf
[grainfs]
type = s3
provider = Other
access_key_id = <ak>
secret_access_key = <sk>
endpoint = http://<server-host>:9000
region = us-east-1
force_path_style = true

# 3. (클라이언트) 버킷 생성 + 마운트
rclone mkdir grainfs:mybucket
rclone mount grainfs:mybucket /mnt/grainfs \
    --vfs-cache-mode writes --dir-cache-time 1s --allow-other --daemon
```

**지원/미지원 연산** (S3 시맨틱의 한계)

| 연산                 | 지원 | 비고                                                |
| -------------------- | ---- | --------------------------------------------------- |
| read / write         | ✅   | rclone `--vfs-cache-mode writes`로 close-to-open    |
| mkdir / ls / rm      | ✅   | S3 prefix 기반 (실제 디렉토리 inode 없음)           |
| rename (mv)          | ⚠️   | CopyObject + DeleteObject — **non-atomic**          |
| chmod / chown        | ❌   | S3는 POSIX permissions 미지원                       |
| atomic create+rename | ❌   | rsync `--inplace` 필요한 워크로드는 NFSv4 권장      |
| file locking         | ❌   | DB / git 인덱스 동시쓰기 워크로드는 NFSv4 권장      |

엄격한 POSIX 시맨틱이 필요하면 NFSv4를 사용한다 (`mount -t nfs4 host:/ /mnt/x`).

검증: `make test-fuse-s3-colima` (macOS 호스트 + Colima Linux VM, rclone 마운트로 핵심 연산 round-trip 테스트).

**처리량 벤치** (`make bench-fuse-s3-colima`, 64 MiB 페이로드, Apple M3, Colima loopback, 3회 평균)

| 경로                       | Write       | Read        |
| -------------------------- | ----------- | ----------- |
| Direct S3 (rclone copyto)  | 96.8 MB/s   | 108.0 MB/s  |
| FUSE mount (rclone mount)  | 106.7 MB/s  | 107.3 MB/s  |
| FUSE 오버헤드              | ≈ 0%        | ≈ 0%        |

`--vfs-cache-mode off`로 close(2)가 PUT 완료까지 블록되도록 설정 → 동기 처리량 측정. `--vfs-cache-mode writes/full`을 쓰면 close 후 백그라운드 업로드 + 로컬 캐시 효과로 체감 처리량은 더 높지만, 정확한 S3 round-trip 측정에는 부적절.

### NBD 테스트 (macOS)

macOS에서 NBD는 커널 모듈이 필요하므로 Docker Desktop 대신 **colima**를 사용한다. Docker Desktop의 LinuxKit VM에는 NBD 모듈이 없다.

```bash
# 1회 설치
brew install colima qemu docker
colima start --vm-type qemu

# NBD E2E 테스트 실행
DOCKER_HOST=unix://$HOME/.colima/docker.sock make test-nbd-docker
```

Linux에서는 Docker Desktop이든 colima든 상관없이 동작한다:

```bash
make test-nbd-docker
```

## License

Apache 2.0
