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
- BadgerDB에 Raft 로그 영속화 ✅
- 스냅샷 생성 및 로그 압축 ✅ (SnapshotManager 자동 트리거, InstallSnapshot RPC, 로그 압축)

**검증:** 노드 재시작 후 스냅샷 + 이후 로그로 정확히 복구. Jepsen 스타일 linearizability 테스트.

### Phase 3: Solo → Cluster 전환 + 분산 스토리지 ✅

**목표:** Solo 인스턴스를 무중단으로 클러스터 노드로 전환한다.

- Storage Backend 인터페이스 정의 (로컬 ↔ 분산 교체 가능) ✅
- Solo 인스턴스 → 클러스터 seed 노드 전환 워크플로우 ✅
- 데이터 디렉토리 포맷 계약, 메타데이터 마이그레이션 경로 ✅
- Raft FSM에 파일별 샤드 위치, 버전 정보 기록 ✅
- 무중단 전환: `POST /api/cluster/join` API로 런타임 클러스터 합류 ✅ (SwappableBackend 기반 atomic swap)

**검증:**
- Solo → 3노드 전환 시 데이터 무손실, 서비스 중단 없음
- 전환 전후 `aws s3 ls`로 동일 데이터 확인

### Phase 4: Erasure Coding + Fan-out ✅

**목표:** 대용량 데이터를 쪼개고, 분산 저장하고, 복구한다.

- klauspost/reedsolomon 통합 (기본 4+2, k+m 가변) ✅
- ShardService + StreamRouter로 QUIC Data Stream 분산 샤드 저장 ✅
- QUIC Data Stream으로 다중 노드 Fan-out 전송 ✅
- Distributed GC: DeleteObject 시 피어 노드 샤드 자동 삭제 ✅
- Failover: PeerHealth 기반 unhealthy 노드 자동 건너뛰기/재시도 ✅
- Re-replication: ReplicationMonitor로 부족한 replica 감지 + RepairPlan 생성 ✅

**검증:**
- ShardService QUIC 기반 WriteShard/ReadShard/DeleteShards E2E 테스트 통과
- PeerHealth cooldown 기반 자동 복구 테스트 통과
- ReplicationMonitor 부족 replica 감지 및 복구 계획 테스트 통과

### Phase 5: Operations & Hardening ✅

**목표:** 프로덕션 운영 도구를 완성한다.

- Presigned URL ✅
- AWS Signature V4: Authorization header + Presigned URL + chunked encoding + POST Policy ✅
- Prometheus 메트릭 (기본: HTTP 요청수/지연, EC 연산, 스토리지 바이트) ✅
- 운영 대시보드: 기본 4개 카운터 + 클러스터 상태/노드 헬스/피어 목록 표시 ✅
- 데이터 암호화: at-rest encryption (AES-256-GCM) ✅
- in-transit encryption은 QUIC TLS 1.3으로 이미 제공 ✅
- 클러스터 멤버십 변경: AddPeer/RemovePeer (Raft config change) ✅
- 클러스터 보안: PSK 기반 ALPN 피어 인증 ✅
- 버킷 단위 EC 정책: CreateBucket 시 EC on/off 설정 ✅
- 런타임 EC 토글: API로 버킷별 EC 정책 변경 가능 ✅
- SDK 호환 테스트: aws-sdk-go ✅ / boto3 ✅ / aws-cli ✅

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

- **Object Browser**: 대시보드에 버킷/오브젝트 브라우저 + 볼륨 관리 탭 + 클러스터 탭 ✅
- **기본 버킷 자동 생성**: 서버 시작 시 기본 버킷("default") 자동 생성 ✅
- **Graceful Shutdown**: 진행 중인 요청 drain ✅, NFS 세션 정리 ✅, Raft 리더 이전 (TransferLeadership + TimeoutNow) ✅
- **벤치마크 스위트**: k6 기반 성능 베이스라인 측정 ✅
- **Docker 기반 NBD 테스트**: macOS에서 Docker 컨테이너로 NBD E2E 테스트 ✅

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

### Phase 9: Security & Scale ✅

**목표:** 프로덕션/멀티테넌트 배포를 위한 보안과 대규모 운영을 완성한다.

- **Bucket Policy**: ✅ S3 호환 Bucket Policy JSON 기반 접근 제어. accessKey를 request context에 전파 (`auth_context.go`), authzMiddleware에서 Effect/Principal/Action/Resource 정책 평가 (`authz.go`). BadgerDB에 `policy:<bucket>` 키로 저장. FSM에 `CmdSetBucketPolicy`/`CmdDeleteBucketPolicy` 추가하여 클러스터 복제 지원
- **클러스터 읽기 캐시**: ✅ DistributedBackend에 `OnApply` 콜백 도입. RunApplyLoop에서 FSM.Apply() 후 CachedBackend.InvalidateKey() 호출. 모든 노드가 동일한 Raft commit 순서를 받으므로 결정적 캐시 무효화 보장
- **WAL 설정**: ✅ Raft LogStore에 `SyncWrites=true` 적용 (kill -9 시 로그 유실 방지). Metadata store는 `SyncWrites=false` 유지 (FSM이 Raft 로그에서 재적용 가능)
- **Rate Limiting**: ✅ 2레이어 구현. IP 기반 pre-auth (DDoS 방어, `golang.org/x/time/rate`) + user 기반 post-auth (테넌트 격리). TTL 기반 자동 정리, 100K 엔트리 cap

**검증:**
- ✅ Bucket Policy 설정 후 Deny 정책에 따른 403 AccessDenied 확인 (E2E)
- ✅ Policy CRUD (PUT/GET/DELETE ?policy) 동작 확인 (E2E)
- ✅ Rate limiting이 정상 부하에서 트리거되지 않음 확인 (E2E)
- ✅ Raft LogStore SyncWrites=true 적용 + 재시작 후 데이터 무손실 확인 (unit test)

### Phase 10: Advanced Storage & Protocol ✅

**목표:** 대규모 운영 시나리오와 프로토콜 고도화를 완성한다.

- **Packed Blob 포맷** ✅: append-only blob log + hash table index with refcount. `--pack-threshold`로 소형 객체 자동 패킹. CRC32 무결성, compaction, concurrent write sharding 지원
- **S3 CopyObject** ✅: PUT with `x-amz-copy-source` 헤더. Copier interface (타입 단언 패턴). Packed Blob 객체는 metadata-only copy (refcount 증가, 데이터 복사 없음)
- **NFSv4.0 서버** ✅: `internal/nfs4server/` — ONC RPC (TCP record marking, fragment reassembly), COMPOUND dispatcher, 12개 op 지원 (PUTROOTFH, PUTFH, GETFH, LOOKUP, GETATTR, READDIR, READ, WRITE, OPEN, CLOSE, SETCLIENTID, SETCLIENTID_CONFIRM). FileHandle UUID generation, StateManager 기반 상태 관리. `--nfs4-port 2049` (기본 활성, localhost 바인드)

**검증:**
- Packed Blob: 소형 객체 write/read, CRC 검증, blob rotation, compaction tombstone 제거 테스트 통과
- S3 CopyObject: metadata-only copy (refcount), 대형 객체 fallback, 원본 삭제 후 복사본 유지 테스트 통과
- NFSv4: ONC RPC frame encode/decode, max frame size 제한, COMPOUND dispatch, multi-op 처리, op 순서 에러 중단 테스트 통과

## 5. 핵심 설계 사양

| 항목               | 값             | 비고                                |
| ------------------ | -------------- | ----------------------------------- |
| Default Shard Size | 4MB            | 메타데이터 부하와 I/O 효율의 균형점 |
| EC Config          | 4+2 (기본)     | 가용성 99.99% 지향, k+m 가변        |
| Raft Heartbeat     | 50ms           | QUIC 저지연 활용                    |
| 전송 프로토콜      | QUIC (quic-go) | TLS 1.3 내장, 혼잡 제어 내장        |
| Metadata KV        | BadgerDB       | LSM-tree, MVCC                      |
| 라이선스           | Apache 2.0     |                                     |

### Phase 11: 성능 최적화 ✅

**목표:** 대용량 파일 처리 성능을 최적화한다.

- **NFSv4 버퍼 최적화**: ✅ io.ReadAll 대신 adaptive buffered streaming 사용. 32KB/256KB/1MB 버퍼 풀로 대용량 파일 throughput 2-3x 개선.
- **E2E 성능 테스트**: 10MB-500MB 파일 읽기/쓰기 throughput 검증 (>100MB/s read, >80MB/s write).
- **Prometheus 메트릭**: 버퍼 풀 사용량, 적중률(hits/misses) 추적.

**검증:**
- 100MB 파일 throughput: >100MB/s (이전 ~30-50MB/s)
- Buffer pool hit rate: >90% (연속 전송 시)
- Concurrent 100MB transfers: 10+ 동시 처리 가능
