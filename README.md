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
| Erasure Coding     | Reed-Solomon 4+2 (가변), 2노드 장애 허용                      |
| QUIC Transport     | quic-go 기반 멀티플렉싱, TLS 1.3 내장                         |
| Custom Raft        | QUIC 위 합의, 리더 선출/로그 복제/스냅샷                      |
| Solo → Cluster     | 무중단 클러스터 전환                                          |
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
      --ec                       Erasure Coding 활성화 (default true)
      --ec-data int              데이터 샤드 수 (default 4)
      --ec-parity int            패리티 샤드 수 (default 2)
      --access-key string        S3 인증 Access Key
      --secret-key string        S3 인증 Secret Key
      --encryption-key-file      암호화 키 파일 경로
      --no-encryption            암호화 비활성화
      --node-id string           노드 ID (클러스터 모드)
      --raft-addr string         Raft 주소 (클러스터 모드)
      --peers string             피어 목록 (클러스터 모드)
```

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
