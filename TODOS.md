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

## P1: 클러스터 멤버십 변경 (Phase 5 미구현)

### Joint Consensus
- **What:** Raft 클러스터에 노드 동적 추가/제거
- **Why:** 현재 클러스터는 부트스트랩 후 정적. 노드 교체나 확장 불가
- **Context:** FSM command 타입에 AddNode/RemoveNode 없음. Raft에 멤버십 변경 로직 없음. Raft 논문의 Joint Consensus 알고리즘 구현 필요

## P2: Zero-Downtime Solo→Cluster 전환 (Phase 3 미구현)

### 무중단 마이그레이션
- **What:** Solo 모드에서 클러스터 모드로 서비스 중단 없이 전환
- **Why:** 현재 grainfs migrate는 서비스 중지 → 마이그레이션 → 재시작 필요. Phase 3 목표는 "무중단"
- **Context:** cluster/migrate.go에 메타데이터 변환 로직은 있음. 온라인 마이그레이션(서비스 유지 중 Raft 부트스트랩)이 필요

## P2: SigV4 고급 기능 (Phase 5 미구현)

### POST Policy (Form-based Upload)
- **What:** 브라우저에서 직접 S3에 업로드하기 위한 POST 폼 인증
- **Why:** 웹 클라이언트 직접 업로드에 필요. Object Browser에서도 활용 가능
- **Context:** server/handlers.go handlePost()가 multipart upload만 처리

## P2: 운영 대시보드 고도화 (Phase 5 미구현)

### 클러스터 모니터링 대시보드
- **What:** 클러스터 상태(노드 목록/리더/term), 노드 헬스, 샤드 분포, 실시간 성능 모니터링
- **Why:** 현재 대시보드는 4개 기본 카운터(Uptime, Requests, Storage, Objects)만 표시. 클러스터 운영에 필요한 가시성 없음
- **Context:** server/ui/index.html의 Dashboard 탭. Prometheus 메트릭은 /metrics에서 제공되므로 추가 API 엔드포인트와 UI 구현 필요


