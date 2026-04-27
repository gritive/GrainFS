# benchmarks/ — GrainFS vs MinIO

GrainFS S3 워크로드 측정 + MinIO 비교 도구.

## 빠른 시작

```bash
# 전체 비교 (grainfs vs minio, single + 4-node cluster, 4/64/1024 KB)
./benchmarks/bench_compare.sh
```

결과는 `benchmarks/results-{timestamp}/SUMMARY.md` 에 마크다운으로,
각 시나리오 raw JSON은 `{system}-{mode}-{size}KB.json` 으로 저장된다.

## 의존성

- `k6` (`brew install k6`)
- `minio` community AGPL build 필요. brew의 `minio`는 AIStor enterprise이라 license 없으면 모든 S3 op 거부됨.
  ```bash
  curl -fL https://dl.min.io/server/minio/release/darwin-arm64/archive/minio.RELEASE.2025-09-07T16-13-09Z \
    -o ~/.local/bin/minio-community && chmod +x ~/.local/bin/minio-community
  ```
  `bench_compare.sh`는 `~/.local/bin/minio-community`를 자동 검출한다.
- `jq` (마크다운 생성)

## 환경 변수

| 변수 | 기본 | 설명 |
|------|------|------|
| `SYSTEMS`   | `"grainfs minio"`     | 비교 대상 |
| `MODES`     | `"single cluster"`   | single = 1-node, cluster = N-node |
| `SIZES_KB`  | `"4 64 1024"`        | 오브젝트 크기 sweep |
| `DURATION`  | `30s`                | 부하 지속 시간 |
| `VUS`       | `20`                 | 최대 동시 VU |
| `NODES`     | `4`                  | cluster 노드 수 |
| `EC_DATA`   | `2`                  | grainfs EC data shards |
| `EC_PARITY` | `2`                  | grainfs EC parity shards |
| `PROFILE`   | `0`                  | 1이면 grainfs pprof 수집 (CPU/heap/mutex) |
| `NO_BUILD`  | `0`                  | 1이면 `make build` 스킵 |
| `OUT_DIR`   | `benchmarks/results-{ts}` | 결과 디렉토리 |
| `MINIO_BIN` | (auto)               | minio binary 경로 override |
| `GRAINFS_BIN`| `./bin/grainfs`      | grainfs binary |

## 워크로드

`s3_bench.js` — k6 시나리오:

- **setup_bucket**: PUT bucket 1회 (cluster init이 늦게 끝나는 minio 대비 retry는 lib에서 처리)
- **mixed_workload**: VU 1→`VUS` 램프 후 `DURATION` 동안 정속 — 매 iteration: PUT → GET → 50% 확률로 DELETE
- 인증: AWS SigV4 (host + x-amz-* 헤더만 서명, content-type 제외 — Hertz/fasthttp 정규화 호환)

`s3_bench.js`의 `OBJECT_SIZE_KB` 환경변수로 사이즈 고정. `MAX_VUS`, `DURATION`, `BUCKET`, `REPORT_PATH`도 환경변수.

## fair comparison 방침

- **클러스터 토폴로지**: 같은 머신, 같은 NODES, 같은 base port 패턴
- **EC 비율**: grainfs는 `--ec-data 2 --ec-parity 2`로 명시. minio는 4-drive 자동(EC 2+2)
- **Encryption**: grainfs `--no-encryption`. minio는 default no-encryption.
- **워크로드**: 동일 `s3_bench.js` (endpoint URL만 다름)
- **Warmup**: k6 ramping-vus가 첫 5-10초로 warmup 역할

## 단발 사용 예시

```bash
# 한 시스템만, 한 모드만, 한 사이즈만 (스모크)
SYSTEMS=grainfs MODES=cluster SIZES_KB=64 DURATION=10s VUS=4 NODES=4 \
  ./benchmarks/bench_compare.sh

# grainfs만 다양한 사이즈 (튜닝)
SYSTEMS=grainfs MODES="single cluster" SIZES_KB="4 64 1024 16384" PROFILE=1 \
  ./benchmarks/bench_compare.sh

# 빠른 회귀 (20s, 1MB만)
SYSTEMS="grainfs minio" MODES=cluster SIZES_KB=1024 DURATION=20s VUS=10 NODES=4 \
  ./benchmarks/bench_compare.sh
```

## pprof 분석 (PROFILE=1)

`results-{ts}/grainfs-cluster-pprof/`에 `cpu.pb.gz`, `heap_post.pb.gz`, `allocs.pb.gz`, `mutex.pb.gz`.

```bash
go tool pprof -top -nodecount=15 results-{ts}/grainfs-cluster-pprof/cpu.pb.gz
go tool pprof -http=:8080 results-{ts}/grainfs-cluster-pprof/cpu.pb.gz
```

## 파일

- `bench_compare.sh` — 메인 비교 스크립트
- `lib_grainfs.sh` — grainfs single/cluster 기동 헬퍼 (sourced)
- `lib_minio.sh` — minio single/distributed 기동 헬퍼 (sourced)
- `s3_bench.js` — k6 워크로드 (S3 SigV4)
- `bench_cluster.sh` — 기존 grainfs-only 3노드 EC 2+1 + pprof 스크립트 (회귀 측정용 보존)
- `bench_profile.sh` — grainfs-only 프로파일링 스크립트
- `run-baseline.sh` — grainfs single-node baseline 측정
