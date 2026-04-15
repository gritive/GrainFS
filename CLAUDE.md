# GrainFS

## Codebase Review

### 기술 스택
- Language: Go 1.26+
- HTTP Framework: Hertz (cloudwego/hertz)
- CLI: Cobra (spf13/cobra)
- Transport: QUIC (quic-go/quic-go)
- Metadata DB: BadgerDB (dgraph-io/badger/v4)
- Erasure Coding: klauspost/reedsolomon
- NFS: willscott/go-nfs
- Monitoring: Prometheus client_golang
- Test: go test + testify, k6 (벤치마크)

### 아키텍처 원칙
- Go 표준 레이아웃: cmd/ (진입점), internal/ (비공개 패키지)
- 단일 바이너리: S3 + NFS + NBD + Web UI를 하나로 제공
- 계층 분리: storage(블롭) → metadata(BadgerDB) → server(HTTP) → transport(QUIC/Raft)
- internal 하위 패키지: cluster, encrypt, erasure, metadata, metrics, nbd, nfsserver, raft, s3auth, server, storage, transport, vfs, volume

### 보안 규칙
- S3 인증: access-key/secret-key 플래그로 HMAC-SHA256 서명 검증
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
- 벤치마크: k6 기반 S3 PUT/GET/DELETE throughput 측정

## Persona Test

### 인터페이스
| 인터페이스 | URL/명령어                                     | 확인 방법                 |
| ---------- | ---------------------------------------------- | ------------------------- |
| CLI        | `./bin/grainfs serve --data ./tmp --port 9000` | Cobra, `--help`           |
| S3 API     | `http://localhost:9000`                        | `aws --endpoint-url` 호환 |
| Web UI     | `http://localhost:9000/ui/`                    | 브라우저 Object Browser   |
| NFS        | `localhost:9002`                               | `mount -t nfs`            |
| NBD        | `localhost:{nbd-port}`                         | Linux only, `nbd-client`  |

### 테스트 계정
- S3 인증: `--access-key` / `--secret-key` 플래그 (미설정 시 인증 없음)

### 제품 스펙
- ROADMAP.md: 개발 로드맵 및 Phase별 기능 정의
- README.md: Quick Start 및 CLI 옵션
