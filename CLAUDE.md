# GrainFS

## Commands

```bash
make build               # bin/grainfs 빌드 (make lint를 선행으로 실행, golangci-lint 필요)
make test-unit           # unit 테스트만 (colima/e2e 제외)
make test                # test-unit + test-colima (colima VM 필요)
make test-race           # race detector 포함
make test-e2e            # E2E (binary 필요, 자동 빌드)
make test-nbd-colima     # NBD 테스트 (colima VM)
make test-nfs4-colima    # NFSv4 테스트 (colima VM)
make test-fuse-s3-colima # FUSE/S3 테스트 (colima VM)
make lint                # golangci-lint
make fbs                 # FlatBuffers (.fbs → .go) 재생성
make bench-iceberg-table # Iceberg table API benchmark (single-node)
make bench-iceberg-table-cluster # Iceberg table API benchmark (cluster)
```

Module: `github.com/gritive/GrainFS`. 단일 binary `bin/grainfs`.

## Codebase Review

### 기술 스택
- Language: Go 1.26+
- HTTP Framework: Hertz (cloudwego/hertz)
- CLI: Cobra (spf13/cobra)
- Transport: QUIC (quic-go/quic-go)
- Metadata DB: BadgerDB (dgraph-io/badger/v4)
- Erasure Coding: klauspost/reedsolomon
- NFSv4: 자체 구현 (internal/nfs4server, XDR/RPC)
- Monitoring: Prometheus client_golang
- Test: go test + testify, MinIO warp (공식 S3 비교 벤치마크)

### 아키텍처 원칙
- Go 표준 레이아웃: cmd/ (진입점), internal/ (비공개 패키지)
- 단일 바이너리: S3 + NFSv4 + NBD + Web UI를 하나로 제공
- 계층 분리: storage(블롭) → metadata(BadgerDB) → server(HTTP) → transport(QUIC/Raft)
- internal 하위 패키지: cluster, raft, transport(QUIC), storage, vfs, volume, server, server/execution, s3auth, iam, nfs4server, nbd, encrypt, badgerrole, badgerutil, cache, dashboard, adminapi, clusteradmin, volumeadmin, alerts, eventstore, icebergcatalog, incident, lifecycle, metrics, migration, otel, policy, pool, receipt, resourceguard, resourcewatch, scrubber, serveruntime, serveruntime/executioncluster, snapshot, chunkref, config, nodeconfig
- FlatBuffers: 내부 통신은 `internal/**/*.fbs` → `make fbs`로 .go 생성 (메모리: "내부 통신 JSON 미사용")

### cmd 경계 계약 (cmd thin-runner)
`cmd/grainfs/*.go` (non-test) 는 다음 네 가지만 포함한다:
1. cobra command 정의 (`var xxxCmd = &cobra.Command{...}`)
2. flag 등록 (`cmd.Flags().String/Int/Bool/Duration(...)`)
3. `init()` 트리 와이어링 (`parentCmd.AddCommand(...)`)
4. 한 줄 `RunE`: `Options` 빌드 후 `<feature>admin.RunX(ctx, opts)` 호출

HTTP/UDS client, 렌더링, 오케스트레이션 같은 비즈니스 로직은 `internal/<feature>admin/` 또는 `internal/serveruntime/` 으로 이동한다. 가드: `cmd/grainfs/cmd_loc_guard_test.go` 가 file ≤ 250 LOC OR 함수 ≤ 90 LOC 를 강제. 상세: `docs/superpowers/specs/2026-05-20-cmd-thin-runner-design.md`.

### 보안 규칙
- S3 인증: admin UDS로 부트스트랩한 SA의 access_key/secret_key로 HMAC-SHA256 서명 검증
- At-rest Encryption: AES-256-GCM (기본 활성)
- 시크릿은 환경변수 또는 파일 경로로만 전달
- 하드코딩 금지

### 코딩 규칙
- gofmt/goimports 필수
- 에러는 fmt.Errorf("%w") 로 래핑
- 인터페이스는 사용처에서 정의
- 테이블 드리븐 테스트 사용

### 성능 규칙
- Erasure Coding: Reed-Solomon 4+2 기본, 가변 설정 가능
- QUIC 멀티플렉싱으로 클러스터 통신
- 벤치마크: `make bench`/`make bench-cluster`/`make bench-s3-compat-compare`로 MinIO warp 기반 S3 PUT/GET/DELETE 측정, `make bench-iceberg-table`/`make bench-iceberg-table-cluster`로 `warp iceberg` 기반 Iceberg REST Catalog 측정

## Persona Test

### 인터페이스
| 인터페이스 | URL/명령어                                     | 확인 방법                 |
| ---------- | ---------------------------------------------- | ------------------------- |
| CLI        | `./bin/grainfs serve --data ./tmp --port 9000` | Cobra, `--help`           |
| S3 API     | `http://localhost:9000`                        | `aws --endpoint-url` 호환 |
| Web UI     | `http://localhost:9000/ui/`                    | 브라우저 Object Browser   |
| NFSv4      | `localhost:2049`                               | `mount -t nfs4` (Linux)   |
| NBD        | `localhost:{nbd-port}`                         | Linux only, `nbd-client`  |

### 테스트 계정
- S3 인증: admin UDS 통해 부트스트랩한 SA의 access_key/secret_key (`grainfs iam sa create admin --endpoint <data>/admin.sock`)

### 제품 스펙
- CONTEXT.md: 도메인 용어/현재 상태 (루트, 13KB)
- ROADMAP.md: 개발 로드맵 및 Phase별 기능 정의
- README.md: Quick Start 및 CLI 옵션
- CHANGELOG.md: 버전별 변경 기록 (`VERSION` 파일과 함께 릴리스 source of truth)
- docs/adr/: 아키텍처 결정 기록
- docs/architecture/request-single-cluster-flow.md: single/cluster request execution actor 설계
- docs/operators/runbook.md, docs/operators/sli-slo.md: 운영/SLO 문서

### 테스트 레이아웃
- `tests/e2e/`: 일반 E2E (Go test)
- `tests/nbd_interop/`: NBD interop (Linux 필요)
- `tests/nbd_colima/`, `tests/nfs4_colima/`, `tests/fuse_s3_colima/`: colima VM, 빌드 태그 `colima` 필수
- NBD 테스트는 colima VM에서 실행

## Tasks

이 프로젝트의 태스크 파일은 `TODOS.md` (루트 디렉토리)입니다.

> Coding Behavior Guidelines와 Skill routing은 글로벌 `~/.claude/CLAUDE.md`에서 상속받습니다.
