# TODOS

## P0: 분산 데이터 플레인 (Phase 4 미구현)

### 분산 샤드 Fan-out
- **What:** QUIC Data Stream으로 EC 샤드를 다중 노드에 분산 저장
- **Why:** 현재 모든 샤드가 로컬 디스크에만 저장됨. 노드 장애 시 데이터 유실. Phase 4의 핵심 목표("분산 저장")를 충족하지 못함
- **Context:** erasure/backend.go에서 os.WriteFile()로 로컬에만 쓰는 구조. QUIC transport는 Raft 메시지 전용. 샤드 placement 로직, 원격 write RPC, 노드별 저장소 관리 필요
- **Depends on:** P1 전체 (QUICTransport wiring, Raft log offset, 스냅샷/압축, InstallSnapshot)

### Failover & Re-replication
- **What:** 노드 장애 감지 시 손실된 샤드를 다른 노드에서 자동 재생성
- **Why:** 분산 EC의 존재 이유. 4+2 구성에서 2노드 장애까지 복구 가능해야 함
- **Context:** 현재 Raft heartbeat로 노드 생존 감지는 되지만, 데이터 샤드 복구 로직은 없음
- **Depends on:** 분산 샤드 Fan-out

### Distributed GC
- **What:** 고아(orphan) 샤드 정리 메커니즘
- **Why:** 삭제/재기록 시 원격 노드에 남은 오래된 샤드가 디스크 공간 차지
- **Context:** 현재 DeleteObject()는 os.RemoveAll()로 로컬만 정리
- **Depends on:** 분산 샤드 Fan-out

### 클러스터 읽기 라우팅
- **What:** 팔로워 노드에서 읽기 요청 시 리더 또는 데이터 보유 노드로 포워딩
- **Why:** DistributedBackend.GetObject()가 로컬 디스크에서만 읽음. 팔로워는 메타데이터만 있고 데이터 파일이 없어서 모든 읽기가 실패
- **Context:** 현재 read forwarding, redirect-to-leader, proxy 없음
- **Depends on:** QUICTransport→Raft wiring

## P1: Raft 기반 인프라 (Phase 2 미완성)

### QUICTransport → Raft Node wiring
- **What:** serve.go의 stub transport를 실제 QUICTransport로 교체
- **Why:** serve.go:237-243에서 클러스터 모드 Raft 노드가 항상 에러를 반환하는 stub transport 사용. 멀티노드 클러스터가 실제로 동작하지 않음
- **Context:** internal/transport/quic.go에 QUICTransport 구현 있음. runCluster()에서 Raft Node에 연결 필요

### Raft log offset (firstIndex)
- **What:** raft.Node에 firstIndex 개념 도입, 모든 slice 접근을 n.log[idx-firstIndex]로 변경
- **Why:** 현재 n.log[idx-1] raw slice indexing 사용. 스냅샷 후 로그 압축 시 n.log가 비어지면 인덱스가 깨짐. 스냅샷 통합의 필수 선행 조건
- **Context:** raft.go 전체에서 n.log[...] 접근하는 모든 곳 수정 필요. lastLogIdx(), lastLogInfo(), applyLoop, replicateTo, HandleAppendEntries, advanceCommitIndex 등

### Raft persistence 에러 처리
- **What:** SaveState/AppendEntries 에러를 무시하지 않고 노드 crash 또는 shutdown
- **Why:** raft.go:766,775에서 `_ = n.store.SaveState()` / `_ = n.store.AppendEntries()`. 디스크 장애 시 SaveState 실패하면 노드가 동일 term에서 두 번 투표 가능 (Raft safety 위반)
- **Context:** 에러 시 log.Fatal() 또는 panic() 호출로 즉시 중단. Raft 논문에서 persistence 실패는 복구 불가능 상태

### 스냅샷 오케스트레이션 wiring
- **What:** SnapshotManager를 Raft Node와 cluster 시작 코드에 연결
- **Why:** snapshot.go에 SnapshotManager(자동 트리거, 복원) 구현 완료. cluster/apply.go에 FSM.Snapshot()/Restore() 존재. 하지만 호출하는 코드가 없음
- **Context:** (1) applyLoop에서 MaybeTrigger() 호출 추가, (2) 노드 시작 시 Restore() 호출 추가, (3) 로그 압축(TruncateAfter)은 InstallSnapshot RPC가 준비될 때까지 비활성화 또는 retain window 적용
- **Depends on:** Raft log offset (firstIndex)

### InstallSnapshot RPC
- **What:** 느린 팔로워에게 스냅샷 전송하는 RPC
- **Why:** 팔로워가 리더 로그보다 너무 뒤처지면 로그만으로 동기화 불가. 스냅샷 전송이 유일한 방법. 로그 압축 활성화의 전제 조건
- **Context:** raft.proto에 SnapshotMeta만 있고 전송 메시지 없음. 새 RPC 메시지 타입과 핸들러 필요
- **Depends on:** 스냅샷 오케스트레이션 wiring

## P1: 클러스터 멤버십 변경 (Phase 5 미구현)

### Joint Consensus
- **What:** Raft 클러스터에 노드 동적 추가/제거
- **Why:** 현재 클러스터는 부트스트랩 후 정적. 노드 교체나 확장 불가
- **Context:** FSM command 타입에 AddNode/RemoveNode 없음. Raft에 멤버십 변경 로직 없음. Raft 논문의 Joint Consensus 알고리즘 구현 필요

## P1: 클러스터 보안 (Phase 5 미구현)

### PSK/토큰 기반 피어 인증
- **What:** QUIC 노드간 연결 시 피어 신원 검증
- **Why:** transport/quic.go:120에 InsecureSkipVerify: true — 아무 노드나 클러스터에 합류 가능. 프로덕션 보안 위험
- **Context:** TLS 인증서 검증 활성화 + PSK 헤더 또는 상호 TLS(mTLS) 구현 필요

## P2: Graceful Shutdown - Raft 리더 이전 (구현 완료, shutdown 연결 필요)

### Leader Transfer shutdown 연결
- **What:** serve.go의 graceful shutdown에서 Raft.TransferLeadership() 호출
- **Why:** 리더가 갑자기 종료되면 새 선거까지 클러스터 쓰기 불가 (수백ms~수초)
- **Context:** raft.go:739에 TransferLeadership() 구현 완료 (simple step-down). serve.go shutdown 경로에 호출 추가 필요

### Targeted Transfer 개선
- **What:** matchIndex 기반 best peer 선택 + TimeoutNow 메시지로 즉시 선거 시작
- **Why:** simple step-down은 느린 팔로워가 리더가 될 수 있고 추가 선거 라운드로 수백ms 쓰기 중단
- **Context:** 현재 raft.go:739는 단순 step-down. Raft 논문 §3.10 참고
- **Depends on:** Leader Transfer shutdown 연결

## P2: Raft 성능 개선

### applyLoop 폴링 제거
- **What:** raft.go의 applyLoop에서 time.Sleep(5ms) 폴링을 sync.Cond 또는 channel signal로 교체
- **Why:** P99 apply 레이턴시에 최대 5ms 추가. Raft heartbeat 50ms 대비 10% 오버헤드
- **Context:** raft.go:259-290, commitIndex 변경 시 signal 발송 필요
- **Depends on:** 스냅샷 wiring과 함께 구현 권장

## P2: Zero-Downtime Solo→Cluster 전환 (Phase 3 미구현)

### 무중단 마이그레이션
- **What:** Solo 모드에서 클러스터 모드로 서비스 중단 없이 전환
- **Why:** 현재 grainfs migrate는 서비스 중지 → 마이그레이션 → 재시작 필요. Phase 3 목표는 "무중단"
- **Context:** cluster/migrate.go에 메타데이터 변환 로직은 있음. 온라인 마이그레이션(서비스 유지 중 Raft 부트스트랩)이 필요

## P2: SigV4 고급 기능 (Phase 5 미구현)

### aws-chunked Content Encoding
- **What:** S3 청크 업로드 시 각 청크에 서명을 포함하는 SigV4 확장
- **Why:** aws-sdk가 대용량 업로드 시 자동으로 사용. 미지원 시 대용량 업로드 실패 가능
- **Context:** s3auth/sigv4.go에 Authorization header와 presigned URL만 구현

### POST Policy (Form-based Upload)
- **What:** 브라우저에서 직접 S3에 업로드하기 위한 POST 폼 인증
- **Why:** 웹 클라이언트 직접 업로드에 필요. Object Browser에서도 활용 가능
- **Context:** server/handlers.go handlePost()가 multipart upload만 처리

## P2: 운영 대시보드 고도화 (Phase 5 미구현)

### 클러스터 모니터링 대시보드
- **What:** 클러스터 상태(노드 목록/리더/term), 노드 헬스, 샤드 분포, 실시간 성능 모니터링
- **Why:** 현재 대시보드는 4개 기본 카운터(Uptime, Requests, Storage, Objects)만 표시. 클러스터 운영에 필요한 가시성 없음
- **Context:** server/ui/index.html의 Dashboard 탭. Prometheus 메트릭은 /metrics에서 제공되므로 추가 API 엔드포인트와 UI 구현 필요

## P3: SDK 호환 테스트 (Phase 5 미구현)

### aws-cli / boto3 테스트
- **What:** aws-cli와 boto3(Python)로 실제 S3 호환성 테스트
- **Why:** 현재 aws-sdk-go-v2만 테스트. 다른 SDK에서 비호환 동작 가능
- **Context:** tests/e2e/에 Go SDK 테스트만 존재. 쉘 스크립트 또는 Python 테스트 추가 필요

## 테스트 커버리지

### 80% 미만 패키지 커버리지 향상
- **What:** storage(79.6%) 패키지를 80%+ 달성
- **Why:** 프로젝트 표준 80% 미달
- **Context:** 현재 커버리지 (2026-04-16 기준): cluster 81.5%, encrypt 83.3%, erasure 86.6%, nfsserver 100%, raft 85.9%, s3auth 91.2%, server 88.1%, storage 79.6%, transport 86.9%, vfs 91.5%, volume 84.1%
