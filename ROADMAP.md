# GrainFS: Technical Specification & Roadmap

## 1. 프로젝트 개요

GrainFS는 경량 분산 객체 스토리지다. 싱글 바이너리로 로컬에서 즉시 S3 호환 스토리지를 띄우고, 필요하면 클러스터로 점진 확장한다. Object Storage(S3 API) + Block Storage(NBD) + File Storage(NFS)를 하나의 바이너리로 제공한다.

**핵심 가치:** `grainfs serve --data ./storage --port 9000` → 2초 내 S3 엔드포인트 동작.

## 2. 시스템 아키텍처

| 레이어             | 역할                                           | 구현 전략                                    |
| ------------------ | ---------------------------------------------- | -------------------------------------------- |
| **L0: Transport**  | 신뢰성 있는 전송, 스트림 멀티플렉싱            | **quic-go** (도입)                           |
| **L1: Consensus**  | 리더 선출, 메타데이터 합의, 샤드 맵 관리       | **Custom Raft** (직접 구현)                  |
| **L2: Data Plane** | Erasure Coding, 병렬 샤드 전송, 로컬 Blob 엔진 | **klauspost/reedsolomon** (도입) + 직접 구현 |
| **L3: API Layer**  | S3 호환 REST API, Volume Device                | 직접 구현                                    |

### 기술 스택 결정

| 구성 요소      | 선택                  | 근거                                                              |
| -------------- | --------------------- | ----------------------------------------------------------------- |
| HTTP Framework | Hertz (cloudwego)     | 고성능 HTTP 프레임워크, netpoll 기반 비동기 I/O                    |
| CLI            | Cobra                 | Go 표준 CLI 프레임워크, 서브커맨드 지원                           |
| Transport      | quic-go               | HOLB 해결, 내장 신뢰성/혼잡 제어/TLS 1.3                          |
| Consensus      | Custom Raft           | QUIC 위에서 동작하는 합의 알고리즘 완전 제어. 학습 및 최적화 목적 |
| Erasure Coding | klauspost/reedsolomon | SIMD 최적화, 검증된 정확성. 데이터 무결성은 검증된 구현이 안전    |
| Metadata KV    | BadgerDB              | LSM-tree 기반 MVCC, 동시 읽기/쓰기 지원                           |
| 라이선스       | Apache 2.0            | 상업적 채택에 유리, AGPL 제약 없음                                |

### QUIC 전송 설계

스트림을 용도별로 분리한다:

| 스트림 타입    | 용도                                        | 특성                |
| -------------- | ------------------------------------------- | ------------------- |
| Control Stream | Raft 메시지 (투표, 하트비트, AppendEntries) | 양방향, 저지연 우선 |
| Data Stream    | 샤드 전송/수신                              | 단방향, 대용량 처리 |
| Admin Stream   | 클러스터 관리, 헬스 체크                    | 양방향, 저빈도      |

## 3. 경쟁 환경

MinIO가 2025.12 maintenance mode, 2026.02 archived되면서 시장에 진공 상태 발생.

| 프로젝트 | 특징                           | GrainFS 차별화                                    |
| -------- | ------------------------------ | ------------------------------------------------- |
| MinIO    | Archived (2026.02)             | 활발한 개발, Apache 2.0                           |
| Garage   | 싱글 바이너리, CRDT, 단순 복제 | Erasure Coding, 강한 일관성 (Raft), Volume Device |
| S2       | Go, 싱글 바이너리, 단일 노드   | 클러스터 모드, EC, Volume Device                  |
| RustFS   | Rust, 소형 객체 빠름           | Go 생태계, Solo→Cluster 확장, Volume Device       |
| Ceph     | 검증됨, Object+Block+File      | 싱글 바이너리, 운영 경량                          |

**GrainFS 고유 포지션:** QUIC 전송 + Erasure Coding + Volume Device (NBD/NFS) + Solo-to-Cluster 확장. 전부 싱글 바이너리.

## 4. 단계별 로드맵

### Phase 1: Solo S3 API ✅

**목표:** `grainfs serve`를 실행하면 즉시 S3 호환 스토리지가 동작한다.

- Go single binary (Hertz + Cobra): `grainfs serve --data ./storage --port 9000`
- S3 호환 API: PUT, GET, HEAD, DELETE, LIST, CreateBucket, HeadBucket
- Multipart Upload: CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload
- 로컬 디스크 저장 (flat files) + BadgerDB 메타데이터
- AWS Signature V4 검증 (Authorization header만)
- 동시성: BadgerDB MVCC 기반

**검증:**
```bash
grainfs serve --data ./tmp --port 9000
aws --endpoint-url http://localhost:9000 s3 mb s3://test
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test/
aws --endpoint-url http://localhost:9000 s3 ls s3://test/
```

### Phase 2: QUIC Transport + Custom Raft ✅

**목표:** QUIC 위에서 안정적인 Raft 클러스터를 동작시킨다.

#### Phase 2-1: QUIC 전송 레이어
- quic-go 기반 노드 간 연결 관리 (Connection Pool)
- 스트림 타입별 멀티플렉싱
- 메시지 프레이밍 프로토콜

**검증:** 3노드에서 스트림 멀티플렉싱 동작, 한 스트림 지연이 다른 스트림에 영향 없음.

#### Phase 2-2: Raft - 리더 선출
- 노드 상태 머신 (Follower, Candidate, Leader)
- RequestVote RPC, Election Timeout 랜덤화, Term 관리

**검증:** 리더 반복 종료/재시작 시 Election Timeout 내 새 리더 선출.

#### Phase 2-3: Raft - 로그 복제
- AppendEntries RPC, 로그 일관성 체크 및 충돌 해결
- Commit Index 관리, FSM 적용

**검증:** netem 10% 패킷 유실 환경에서 로그가 전 노드 100% 일치.

#### Phase 2-4: Raft - 영속성 및 스냅샷
- BadgerDB에 Raft 로그 영속화
- 스냅샷 생성 및 로그 압축

**검증:** 노드 재시작 후 스냅샷 + 이후 로그로 정확히 복구. Jepsen 스타일 linearizability 테스트.

### Phase 3: Solo → Cluster 전환 + 분산 스토리지 ✅

**목표:** Solo 인스턴스를 무중단으로 클러스터 노드로 전환한다.

- Storage Backend 인터페이스 정의 (로컬 ↔ 분산 교체 가능)
- Solo 인스턴스 → 클러스터 seed 노드 전환 워크플로우
- 데이터 디렉토리 포맷 계약, 메타데이터 마이그레이션 경로
- Raft FSM에 파일별 샤드 위치, 버전 정보 기록

**검증:**
- Solo → 3노드 전환 시 데이터 무손실, 서비스 중단 최소화
- 전환 전후 `aws s3 ls`로 동일 데이터 확인

### Phase 4: Erasure Coding + Fan-out ✅

**목표:** 대용량 데이터를 쪼개고, 분산 저장하고, 복구한다.

- klauspost/reedsolomon 통합 (기본 4+2, k+m 가변)
- Storage Backend 인터페이스를 통한 분산 샤드 저장
- QUIC Data Stream으로 다중 노드 Fan-out 전송
- Distributed GC, Failover, Re-replication

**검증:**
- 4+2 구성에서 임의 2노드 장애 후 원본 비트 단위 복구
- Network Partition, Disk Failure 시나리오
- Chaos Test: 임의 노드 킬/복구 반복 24시간 무결성 유지

### Phase 5: Operations & Hardening ✅

**목표:** 프로덕션 운영 도구를 완성한다.

- Presigned URL
- AWS Signature V4 전체 지원 (chunked encoding, POST policy, presigned URL)
- Prometheus 메트릭 (노드별 지연, EC 연산 시간, 스트림 상태)
- 운영 대시보드: 클러스터 상태, 노드 헬스, 샤드 분포, 스토리지 용량, 실시간 성능 모니터링(요청 처리량, 지연시간, EC 인코딩/디코딩 시간) 시각화 (순수 Go + embed, 싱글 바이너리 원칙 유지)
- 데이터 암호화: at-rest encryption (AES-256-GCM), 키 관리 (KMS 연동 또는 로컬 키 파일)
- in-transit encryption은 QUIC TLS 1.3으로 이미 제공
- 클러스터 멤버십 변경 (Joint Consensus)
- 클러스터 보안: PSK/토큰 기반 피어 인증, 무단 노드 연결 차단
- 버킷 단위 EC 정책: CreateBucket 시 EC on/off 설정, 새 객체부터 적용, 기존 객체는 읽기 시 포맷 자동 감지
- 런타임 EC 토글: 대시보드 또는 API로 버킷별 EC 정책 변경 가능 (성능 이슈 대응, 무중단 전환)
- SDK 호환 테스트: aws-cli, boto3, aws-sdk-go

**검증:**
- k6로 수만 동시 연결에서 P99 응답 속도 측정
- Signed URL 만료/변조 시 거부 확인
- 암호화 활성화 시 데이터 파일이 평문으로 읽히지 않음 확인
- 대시보드에서 클러스터 상태 실시간 반영 확인
- 버킷별 EC 정책 변경 후 새 객체는 해당 정책으로, 기존 객체는 원본 포맷으로 정상 읽기 확인

### Phase 6: Volume Device ✅

**목표:** Object Storage 위에 블록/파일 시스템 레이어를 제공한다.

- Linux: NBD (Network Block Device) 서버 (`//go:build linux`)
- macOS: NFS v3 서버 (`willscott/go-nfs`)
- Volume 관리 REST API (`PUT/GET/DELETE /volumes/{name}`)
- 볼륨 추상화 레이어 (`internal/volume/`) - ReadAt/WriteAt over storage.Backend
- VFS 레이어 (`internal/vfs/`) - `billy.Filesystem` 구현
- 동일 싱글 바이너리에 포함 (`--nfs-port`로 활성화)
- Solo/Cluster 모드 모두 지원

**검증:** NFS 클라이언트로 파일 생성/읽기/삭제 동작 확인 (E2E).

### Phase 7: First User Experience ✅

**목표:** 실사용자가 즉시 가치를 느끼는 환경을 만든다.

- **Object Browser**: 대시보드에 버킷/오브젝트 브라우저 + 볼륨 관리 탭 (aws-cli 없이 브라우저에서 업로드/다운로드/삭제/조회)
- **기본 버킷 자동 생성**: 서버 시작 시 기본 버킷("default") 자동 생성
- **Graceful Shutdown**: 진행 중인 요청 drain, NFS 세션 정리, Raft 리더 이전
- **벤치마크 스위트**: k6 기반 성능 베이스라인 측정 (S3 API throughput, 지연, EC 오버헤드, NFS 처리량). Phase 8 최적화의 근거 데이터
- **Docker 기반 NBD 테스트**: macOS에서 Docker 컨테이너로 NBD E2E 테스트 (`make test-nbd-docker`)

**검증:**
- Object Browser에서 파일 업로드/다운로드/삭제 + 볼륨 생성/삭제 동작 확인
- `grainfs serve` 즉시 실행 후 브라우저에서 오브젝트 조작 가능
- 벤치마크 리포트 자동 생성 (ops/sec, P50/P99 지연, EC 인코딩 시간)

### Phase 8: Performance (측정 기반 최적화) ✅

**목표:** Phase 7 벤치마크 결과를 근거로, 확인된 병목을 제거한다.

- ~~**내부 직렬화 protobuf 전환**~~: ✅ 완료 (Phase 7에서 선행). raft, storage, erasure, cluster, volume 전 모듈 protobuf 전환
- **읽기 캐시**: ✅ CachedBackend (LRU, 바이트 크기 기반 관리). Solo 모드에서 GetObject/HeadObject 캐시, PutObject/DeleteObject/CompleteMultipartUpload 시 자동 무효화. 클러스터 모드는 데이터 일관성을 위해 캐시 미적용 (Phase 9에서 Raft FSM 기반 무효화 검토)
- **NFS 성능 최적화**: ✅ VFS 레이어에 Stat/ReadDir TTL 캐시 도입. NFS가 반복 호출하는 Stat, ReadDir, isDir 결과를 캐싱하여 backend 호출 대폭 감소. 파일 생성/수정/삭제 시 자동 무효화

**검증:**
- protobuf 전환 후 Raft heartbeat 지연 50% 이상 감소 확인
- 캐시 전후 벤치마크 비교 (Apple M3, 4KB 객체 100개, warm cache):
  - GetObject: 17,000 ns → 140 ns/op (**~120x 개선**, allocs 29→5)
  - HeadObject: 1,700 ns → 88 ns/op (**~19x 개선**, allocs 24→2)

### Phase 9: Security & Scale

**목표:** 프로덕션/멀티테넌트 배포를 위한 보안과 대규모 운영을 완성한다.

- **버킷/볼륨 인증**: 버킷별, 볼륨별 접근 제어 (ACL 또는 IAM 스타일 정책)
- **클러스터 읽기 캐시**: Raft FSM 적용 시 캐시 무효화 이벤트 발생 → 노드 간 일관된 캐시. Phase 8에서 Solo 모드 전용으로 도입한 CachedBackend를 클러스터로 확장
- **NFS v4 지원**: NFSv3 → NFSv4 업그레이드 (보안, 상태 관리, 잠금/위임)
- **WAL vs BadgerDB 내장 WAL 검토**: BadgerDB 자체 WAL 내구성 한계 분석 후, 별도 WAL 필요 여부 결정. kill -9 시나리오 테스트로 판단
- **Packed Blob 포맷**: 대량 소형 객체 시나리오에서 inode 압박 해소를 위한 append-only 로그 포맷
- **Rate Limiting**: API 엔드포인트별 요청 제한

**검증:**
- 버킷 ACL 설정 후 무단 접근 거부 확인
- 클러스터 3노드에서 노드A PUT → 노드B GET 시 stale 캐시 없이 최신 데이터 반환 확인
- NFSv4 마운트 후 잠금, 위임 동작 확인
- BadgerDB WAL kill -9 테스트 → 별도 WAL 필요 시 구현 후 동일 테스트 통과
- 100만 소형 객체 (< 1KB) 저장 후 inode 사용량 비교 (flat vs. packed)

## 5. 핵심 설계 사양

| 항목               | 값             | 비고                                |
| ------------------ | -------------- | ----------------------------------- |
| Default Shard Size | 4MB            | 메타데이터 부하와 I/O 효율의 균형점 |
| EC Config          | 4+2 (기본)     | 가용성 99.99% 지향, k+m 가변        |
| Raft Heartbeat     | 50ms           | QUIC 저지연 활용                    |
| 전송 프로토콜      | QUIC (quic-go) | TLS 1.3 내장, 혼잡 제어 내장        |
| Metadata KV        | BadgerDB       | LSM-tree, MVCC                      |
| 라이선스           | Apache 2.0     |                                     |
