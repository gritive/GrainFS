# /autoplan restore point: 
# Production Hardening Plan

## Problem Statement

GrainFS는 Phase 1-10까지 모든 핵심 기능 구현이 완료되었습니다. 하지만 프로덕션 배포를 위한 **운영 안전장치**가 미흡합니다. 현재 상황:
- ✅ Raft snapshot 기능 존재 (자동/수동)
- ✅ E2E 테스트 존재 (S3, NFS, NBD, Cluster)
- ❌ 장애 발생 시 대응 절차 문서 부재
- ❌ 데이터 손실 시나리오 테스트 부족
- ❌ 백업/복구 절차 미정의
- ❌ 인시던트 대응 playbook 부재

## Goal

프로덕션 환경에서 안전하게 운영할 수 있도록 **장애 복구 능력**을 완성한다.

## Target Users

1. SRE/DevOps 엔지니어 - GrainFS 운영 담당
2. 온콜 엔지니어 - 장애 대응 담당

## Dream State

**12개월 후:**
- 장애 발생 시 온콜 엔지니어가 playbook을 보고 10분 내 복구
- 정기 백업으로 데이터 손실 0
- 장애 시나리오 테스트로 잠재적 문제 조기 발견
- 인시던트 대응 프로세스 자동화

## Current Plan

### Phase 1: Disaster Recovery Playbook

**Scope:** 장애 상황별 대응 절차 문서화

**Deliverables:**
1. **DR Playbook 문서** (`docs/DISASTER_RECOVERY.md`)
   - 시나리오별 대응 절차 (노드 장애, 네트워크 분할, 디스크 장애, 데이터 손실)
   - 복구 체크리스트
   - 복구 완료 검증 절차
   - 복구 시간 목표 (RTO, RPO)

2. **주요 시나리오:**
   - 단일 노드 장애 복구
   - 다중 노드 장애 (quorum 상실)
   - 네트워크 분할 (split-brain)
   - 디스크 장애 및 데이터 corruption
   - 리더 장애 및 장기 추방 (leader eviction)
   - 클러스터 전체 장애 (catastrophic failure)

**What already exists:**
- Raft snapshot 기능 (`internal/raft/snapshot.go`)
- Graceful shutdown (TransferLeadership)
- PeerHealth 기반 failover
- E2E 테스트 (`tests/e2e/cluster_test.go`)

**Implementation strategy:**
- 기존 기능 조사 및 문서화
- 시나리오별 절차 작성
- 복구 명령어 정리
- 검증 체크리스트 작성

### Phase 2: Data Loss Scenario Testing

**Scope:** 데이터 손실 가능성이 있는 시나리오를 테스트로 검증

**Deliverables:**
1. **장애 주입 테스트** (`tests/e2e/disaster_test.go`)
   - 노드 kill 시 데이터 무손실 확인
   - 디스크 쓰기 장애 시 복구 확인
   - 네트워크 분할 시 데이터 일관성 확인
   - Raft 로그 손실 시 복구 확인

2. **복구 테스트:**
   - Snapshot 기본 복구
   - 피어 노드 재합류 시 데이터 동기화
   - Under-replicated shard 자동 복구 (ReplicationMonitor)

3. **Corruption 감지 테스트:**
   - BadgerDB vlog corruption 감지
   - Shard 데이터 CRC 검증
   - Packed Blob CRC32 검증

**What already exists:**
- Raft 영속성 테스트 (`internal/raft/store_test.go`)
- Snapshot 복구 테스트 (`internal/raft/snapshot_test.go`)
- Corruption 방지 테스트 (`internal/storage/packblob/blob_corruption_test.go`)

**Implementation strategy:**
- 기존 단위 테스트를 E2E로 확장
- chaos engineering 툴 활용 (kill -9, netem, iptables)
- 테스트 결과를 DR Playbook에 반영

### Phase 3: Backup and Restore Procedures

**Scope:** 정기 백업 및 복구 절차 정의

**Deliverables:**
1. **백업 절차** (`docs/BACKUP_RESTORE.md`)
   - 메타데이터 백업 (BadgerDB)
   - Blob 데이터 백업 (저장소)
   - 백업 스케줄 권장사항
   - 백업 무결성 검증

2. **복구 절차:**
   - 전체 복구 (disaster recover)
   - 부분 복구 (특정 버킷/볼륨)
   - Point-in-Time 복구 (WAL 기반)
   - 복구 후 데이터 검증

3. **자동화 도구:**
   - `grainfs backup` CLI 명령
   - `grainfs restore` CLI 명령
   - 백업 스케줄러 (cron/cronjob)
   - 백업 검증 자동화

**What already exists:**
- Raft snapshot (메타데이터 스냅샷)
- Blob 저장소 (로컬 파일 시스템)
- BadgerDB 백업 API (`Badger.Backup`)

**Implementation strategy:**
- BadgerDB Backup API 활용
- Blob 데이터 rsync/자기 복사
- 복구 시점 선택 가능하도록 WAL 활용 (Phase 9에서 이미 도입)
- 복구 검증 E2E 테스트

### Phase 4: Recovery Procedure Automation

**Scope:** 복구 절차 자동화로 복구 시간 단축

**Deliverables:**
1. **자동 복구 스크립트**
   - 노드 대체 (failed node → new node)
   - Quorum 복구 (add voter)
   - 데이터 재동기화 (trigger reshard)

2. **대시보드 통합:**
   - 노드 상태 모니터링 (PeerHealth)
   - 복구 진행률 표시
   - 복구 완료 알림
   - 클러스터 건강도 점수

3. **Health Check 강화:**
   - 디스크 공간 모니터링
   - Replica completeness 체크
   - Data integrity 검증 (periodic scrubbing)

**What already exists:**
- PeerHealth 기반 unhealthy 노드 감지
- ReplicationMonitor (under-replicated shard 복구)
- Prometheus metrics
- Dashboard (기본 건강도 카운터)

**Implementation strategy:**
- 기존 PeerHealth/ReplicationMonitor 확장
- Health check endpoint 추가
- Dashboard에 복구 상태 표시
- Alert rule 정의 (Prometheus)

### Phase 5: Incident Response Procedures

**Scope:** 인시던트 대응 프로세스 정의

**Deliverables:**
1. **인시던트 대응 playbook** (`docs/INCIDENT_RESPONSE.md`)
   - 심각도별 대응 절차 (SEV1-4)
   - 에스컬레이션 경로
   - 커뮤니케이션 템플릿
   - Post-mortem 템플릿

2. **모니터링 강화:**
   - 핵심 메트릭 정의 (SLO/SLA)
   - Alerting rule 정의
   - Runbook 자동화

3. **온콜 준비:**
   - 온콜 핸드오버 절차
   - 온콜 훈련 시나리오
   - 온콜 Survival Guide

**What already exists:**
- Prometheus metrics (기본)
- Dashboard (클러스터 상태)

**Implementation strategy:**
- SLO/SLA 정의 (가용성 99.9%, 지연 시간 P99 < 100ms 등)
- Alert rule 작성 (Prometheus)
- PagerDuty/OpsGenie 연동 가이드
- 인시던트 템플릿 작성

## What NOT in Scope

- ~~새로운 기능 개발~~ (Performance Optimization, Advanced Features는 별도 Phase)
- ~~아키텍처 변경~~ (현재 아키텍처 유지)
- ~~성능 최적화~~ (Phase 8에서 이미 완료)

## Success Criteria

1. DR Playbook 완성 - 모든 시나리오 문서화
2. 장애 주입 테스트 통과 - 데이터 손실 0
3. 백업/복구 절차 정의 및 CLI 구현
4. 복구 자동화 - RTO 10분 달성
5. 인시던트 대응 프로세스 정의

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| 복구 절차 미검증 | 데이터 손실 가능 | E2E 테스트로 검증 |
| 백업 불일치 | 복구 실패 | 백업 무결성 검증 |
| Playbook 부실 | 온콜 패닉 | 정기 훈련 및 업데이트 |
| 복구 시간 초과 | 장기 서비스 중단 | 자동화 및 반복 테스트 |

## Timeline Estimation

- Phase 1 (DR Playbook): 1주
- Phase 2 (Data Loss Testing): 2주
- Phase 3 (Backup/Restore): 2주
- Phase 4 (Automation): 1주
- Phase 5 (Incident Response): 1주
- **Total: 7주**

## CEO Review Results

### Executive Summary
**STATUS: MAJOR REFRAME REQUIRED**

The CEO review identified critical misalignment between the plan and actual product needs:
- **Wrong problem**: "Production Hardening" (enterprise SRE focus) → Should be "Time to First Production Deployment" (evaluator trust building)
- **Wrong approach**: Documentation-first → Should be Automation-first
- **Missing alternatives**: Custom CLI → Should research Velero/Restic/Rclone integration first
- **Unrealistic timeline**: 7 weeks sequential → Should be 3 weeks iterative

### Critical Findings

1. **WRONG PROBLEM TO SOLVE (CRITICAL)**
   - Building enterprise SRE-level DR playbooks for a system with 0 production customers
   - **10x reframing**: Focus on "Can a skeptical engineer trust GrainFS in a 2-hour PoC?" not "Can we respond to SEV1 incidents?"
   - **Recommendation**: Pivot from 5 phases to 2: (1) Automated failure injection tests, (2) One-command backup/restore

2. **UNSTATED PREMISE: "Documentation beats Automation" (HIGH)**
   - Plan assumes writing docs = safety, but humans panic/ignore docs during SEV1s
   - Existing code already has extensive automated recovery (PeerHealth, ReplicationMonitor, SnapshotManager)
   - **6-month regret**: Spend 7 weeks on docs, first disaster engineer ignores them
   - **Recommendation**: Cancel Phase 1 (DR Playbook) and Phase 5 (Incident Response), replace with `grainfs doctor` and `grainfs recover --auto`

3. **MISSING ALTERNATIVE (HIGH)**
   - Zero competitive analysis: Velero, Restic, Rclone integration not considered
   - **Competitive risk**: While building custom CLI (7 weeks), competitor releases "GrainFS + Velero" (2 weeks)
   - **Recommendation**: Week 1 research existing tools, build custom only if insufficient

4. **DANGEROUS ASSUMPTION (MEDIUM)**
   - "Testing equals Safety" - passing tests ≠ production safety
   - Missing: Jepsen-style linearizability tests, network partition tests, silent corruption tests
   - **Recommendation**: Add Jepsen tests, network fault injection (toxiproxy), soak tests

5. **TIMING RISK (MEDIUM)**
   - 7-week sequential plan: Phase 2 (testing) might invalidate Phase 1 (playbook), causing rework
   - **Recommendation**: Test-driven approach - Write failing test → implement → document

### CEO Consensus Table

| Dimension | Status | Finding |
|-----------|--------|---------|
| Premises | ⚠️ PARTIAL | Wrong problem, doc>automation bias |
| Market | ⚠️ UNCLEAR | 0 production customers, wrong target segment |
| Solution | ❌ MISALIGNED | Documentation-heavy, need automation-first |
| Alternatives | ❌ INSUFFICIENT | Missing Velero/Restic research |
| Scope | ⚠️ OVER-SCOPED | 5 phases/7 weeks → 3 phases/3 weeks |
| Risks | ⚠️ UNADDRESSSED | Testing gaps (Jepsen, network partition) |
| GTM | ❌ PREMATURE | Enterprise SRE focus for pre-PMF product |
| Metrics | ⚠️ INCOMPLETE | Missing SLI/SLO with math |
| Unknowns | ❌ TOO MANY | No validation that docs/custom CLI is right |
| Next Steps | ⚠️ VAGUE | Missing user validation or spike |

### Taste Decisions (→ Final Gate)

1. **Problem reframing** (CRITICAL): Production Hardening → Time to First Production Deployment
2. **Scope restructuring**: 5 phases → 3 phases (Test → Automate → Document)
3. **Timeline compression**: 7 weeks → 3 weeks iterative
4. **Priority shift**: Documentation-first → Automation-first
5. **Build vs buy**: Custom CLI → Research Velero/Restic first

### CEO-Recommended Alternative: 3-Week Plan

**Week 1: Safety Foundation**
- [ ] Add Jepsen test for Raft consistency (prove zero data loss)
- [ ] Add network partition tests (toxiproxy integration)
- [ ] Define SLI/SLO (quantitative "production ready" definition)
- [ ] Add automated smoke test for deployment verification

**Week 2: Self-Healing**
- [ ] Build `grainfs doctor` diagnostic command
- [ ] Build `grainfs recover --auto` (orchestrate existing recovery)
- [ ] Expose snapshot/restore as HTTP API (not just CLI)
- [ ] Add Prometheus alerting rules

**Week 3: Backup Strategy**
- [ ] Research phase: Test Velero/Restic/Rclone integration
- [ ] If insufficient: Build `grainfs backup` (expose BadgerDB API)
- [ ] Add backup verification test (restore and verify)
- [ ] Document "Backup and Restore" as single markdown page

**Total: 3 weeks, higher quality, better product-market fit.**

### NOT in Scope (CEO Review)

- ~~Multi-region DR~~ - defer to future phase
- ~~Auto-remediation with ML~~ - defer to future phase
- ~~Incident Response Procedures~~ - cancel (no on-call staff yet)
- ~~DR Playbook (50-page doc)~~ - replace with `grainfs doctor` command
- ~~5-phase sequential plan~~ - restructure to 3-phase iterative

## Design Review Results

### Executive Summary
**STATUS: CRITICAL DESIGN FOUNDATION MISSING (0.7/10 completeness)**

The design review identified that the plan has zero implementable UI/UX specifications. Every deliverable is a placeholder ("복구 진행률 표시") without defining what it actually looks like or how it behaves.

### Critical Findings (Design)

1. **INFORMATION HIERARCHY: COMPLETELY ABSENT (CRITICAL)**
   - "Dashboard integration"만 명시되고, 실제 visual hierarchy 전혀 없음
   - Health score 시각화 정의 없음 (색상? 숫자? 라벨?)
   - Recovery progress 명세 없음 (progress bar? percentage? ETA?)
   - **결과**: Implementer는 "복구 진행률 표시"라는 요구사항을 어떻게 구현할지 혼란

2. **MISSING STATES: EMOTIONAL JOURNEY UNCONSIDERED (HIGH)**
   - 성공 state만 정의되어 있음
   - Distributed system failure의 emotional arc: Panic → Confusion → Investigation → Action → Waiting → Relief
   - Undefined states: Loading, Degraded, Recovering, Partial Recovery, Recovery Failed, Quorum Lost, Empty State

3. **USER JOURNEY: ASSUMES RATIONAL BEHAVIOR (HIGH)**
   - Plan: 엔지니어가 emergency에 documentation을 읽는다고 가정 ❌
   - 현실: 2AM pager → Half-asleep engineer → "Health Score: 42" (what does this mean??) → CLI 실행 → Dashboard 변화 없음 → Duplicate recovery 실행
   - CEO review와 alignment: `grainfs doctor`와 `grainfs recover --auto`가 필요하지만, UI 표시 방법 전혀 없음

4. **SPECIFICITY: GENERICS EVERYWHERE (HIGH)**
   - 모든 deliverable이 placeholder ("복구 진행률 표시" → Show progress somehow)
   - 필요한 component spec: RecoveryProgressCard, HealthScoreBadge, AlertBanner

5. **DESIGN DECISIONS THAT WILL HAUNT YOU (CRITICAL)**
   - "Health Score" Without Definition → Implementer마다 다른 algorithm
   - "Recovery Progress" Without State Machine → Frontend가 spinner 무한 표시
   - "Dashboard Integration" Without Layout → Recovery card를 bottom에 배치해서 아무도 안 봄

### Design Litmus Scorecard

| Dimension | Score (0-10) | Finding |
|-----------|--------------|---------|
| 1. Information Hierarchy | 1/10 | Complete absence - no visual hierarchy |
| 2. Layout & Visual Design | 0/10 | No layout hierarchy, no component placement |
| 3. Interaction States | 1/10 | Only success states, missing loading/error/degraded |
| 4. Responsive Strategy | 0/10 | No mobile design (engineers use phones!) |
| 5. Accessibility | 0/10 | No keyboard nav, contrast, touch targets |
| 6. Design System Consistency | 2/10 | Existing patterns not referenced |
| 7. Specificity | 1/10 | All placeholders, not implementable specs |

**Overall: 0.7/10 — CRITICAL GAP**

### Implementation Blockers

Design review는 **모든 dimension이 critical/high issue**로, implementation을 block합니다:

1. **No visual hierarchy** → Layout chaos
2. **No state machine** → Frontend/backend sync bugs  
3. **No health score definition** → User confusion
4. **No mobile design** → Broken on-call experience
5. **No error state specs** → Implementation paralysis

### Required Design Artifacts (Before Implementation)

1. **System States × UI States Matrix**: Healthy/Degraded/Recovering/Critical/Unknown
2. **Component Specs**: HealthScoreBadge, RecoveryProgressCard, AlertBanner (HTML+CSS+JS)
3. **State Machine**: Recovery progression (not_started → in_progress → completed/failed/cancelled)
4. **Layout Hierarchy**: Top row (Health+Alerts), Second row (Recovery), Third row (Stats)
5. **Responsive Design**: Mobile breakpoint, touch targets min 44px×44px
6. **Interaction Flows**: Node Failure Recovery journey (detection → acknowledgment → progress → completion)

### Design-CEO Alignment Issues

- **CEO Finding 1** ("Time to First Production Deployment"): Design missing pre-flight checklist UI
- **CEO Finding 2** ("Automation Over Documentation"): `grainfs doctor` UI 표시 방법 없음
- **CEO Finding 3** ("Missing Alternatives"): Velero/Restic integration UI 없음

## Next Steps

1. User approval required for problem reframing
2. If approved: Restructure plan to 3-week automation-first approach
3. If rejected: Continue with original 5-phase documentation approach
