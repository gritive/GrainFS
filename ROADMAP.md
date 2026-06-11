# `GrainFS` Technical Roadmap v2

> 배경: PUT 0.41x(외부 S3 대비)는 **구조적** — per-PUT raft 합의 2회(data_raft 객체메타 + meta_index 라우팅).
> knob(disk/net/cpu/fsync/raft/stage/pipeline)은 직전 에픽에서 전부 고갈, 천장 보존 확인.
> **가설(보장 아님)**: write-path 합의 제거로 PUT 개선. 단 직전 두 에픽이 반대 증거 — consensus-tax REFUTED(cap=receiver shard-write 직렬화 ~7/node), meta_index 17→11ms인데 throughput FLAT(overlapped-not-additive). 합의 latency 단축은 shard-write와 overlap돼 throughput을 안 움직였다. **유일하게 다를 수 있는 이유**: quorum은 raft fsync/replication "일" 자체를 제거(샤딩/detach는 일을 남겼다). 정직한 기대 = **flat 가능성 높음, win은 optimistic tail** → Phase 0/3/5로 싸게 실패하도록 gate.
> greenfield로 데이터 모델을 결정론 placement + quorum 메타로 전환.

## 원칙

- **raft는 저volume 강일관 control plane 전용**: membership + group topology + bucket/IAM + multipart manifest.
  (원안 "membership only"에서 좁힘 — bucket/IAM/multipart 완료는 강일관 필요. PUT QPS에 안 실리므로 0.41x와 무관.)
- **고volume object data plane은 합의 없음**: 결정론 placement + per-node quorum 메타 + LWW versioning.
- **placement = `hash(bucket+key) % numGroups`(동결)** + Rendezvous Hashing(weighted, static capacity)로 group 내 노드 선택.
  group identity 안정 → 노드 손실은 EC 복원으로 치유. GET 라우팅은 인덱스 없이 group 재계산(group→node 토폴로지만 control-plane raft에서 read, cache 가능).
- **Bounded Load은 soft 신호로 격하**(read-replica/balancer), placement deflection엔 미사용 — deflection은 findability를 깨므로.
- **Gossip으로 soft state 공유**(부하/용량/health) → Bounded Load·balancer 입력. membership은 gossip 아닌 raft.
- **API 처리는 단일 라인**: single/cluster request actor 모델 폐기, 하나의 경로.
- **Data plane transport = streaming HTTP(Hertz) over SPKI-pinned mTLS** *(별개 베팅 — Phase 8)*: 노드간 shard/메타 전송을 자작 mux/chunk/pool/desync 대신 HTTP로. body는 raw 바이트 **스트리밍**(버퍼링 금지). FlatBuffers는 raft/control plane 전용. H3(QUIC)·JSON 금지.
- **Greenfield, 하위호환성 없음.** 데이터 모델 변경은 비가역(QUIC→TCP flip과 동일 성격).
- **Git-gated greenfield (eager-delete)**: 신규 data plane은 브랜치에서 옛 경로를 **즉시 삭제하며** 만든다(런타임 flag 없음 — flag-threading은 코드만 더럽힘). 가역성=git(옛 시스템=master, **벤치 통과 전 merge 안 함**), baseline=master 바이너리. **비가역 지점은 flip이 아니라 merge.** (노이즈가 크면 대안: 두 구현을 단일 `DataPlane` 인터페이스 뒤 boot서 1회 선택 → 깨끗한 공존+within-run A/B, 기본은 plain eager.)
- **결정 벤치는 cross-binary라 엄격성 non-negotiable**(flag 없으니 within-run A/B 불가): 외부 S3 앵커 필수 · master↔브랜치 같은 VM back-to-back · within-run 비율만 · 다회 실행. (4b-2를 inconclusive하게 만든 across-boot 노이즈 회피.)
- **group 수는 Phase 7까지 고정**: 노드-in-group은 EC heal로 변동하나 group 추가/감소 불가(운영 제약, Phase 7에서 해제).
- **유지**: Erasure Coding, At-rest Encryption(XAES-256-GCM), zero-CA, putpipeline streaming 산개(꼬리만 quorum-write로 교체).
- **별개 베팅(성능 비요구, data plane 안정 후): 라이브러리 분리(raft/HRW/bounded/gossip) · 비-S3 프로토콜 재연결 · data-plane HTTP transport.**
- **dead code 삭제** 아키텍처 변경으로 생기는 dead code는 과감히 삭제, 단, 향후 다시 연결할 nfs, 9p, nbd 등 프로토콜은 예외
- **테스트도 1급 변경 대상** (코드만큼 churn 큼):
  - eager-delete라 삭제 경로의 테스트는 **코드와 함께 삭제**(Phase 3=data_raft, Phase 4=meta_index서 동시). 단일 경로라 dual-path 2배 부담 없음.
  - 신규 경로(placement/quorum/LIST)는 net-new 테스트. 정확성은 S3 e2e green 게이트, 성능은 Phase 5 cross-binary 벤치가 판정.
  - must-solve(조건부 PUT CAS / LWW / version 순서)는 net-new **동시성·clock-skew·RAW** 테스트 필요(raft 직렬화가 공짜로 주던 보장 소멸).
  - 비활성 프로토콜(Phase 1)의 e2e/colima 테스트는 삭제 아닌 **skip**(재연결 Phase 10 대비). S3 e2e는 전 구간 게이트 유지.
  - 프레임워크(CLAUDE.local): unit=testify, integration/e2e=ginkgo/gomega.

## 폐기 / 유지 / 재사용

```
삭제:  meta-raft 객체 인덱스 전체 · data_raft ObjectMeta 경로 · sharded-index 에픽 머신(#721~) · request actor 모델
       · 자작 data-plane transport 머신(mux carrier / chunked framing / per-peer conn pool / desync detection / control·bulk lane)
유지:  putpipeline streaming 산개 · EC · 암호화 · zero-CA · VFS/프로토콜 seam · control-plane raft · FlatBuffers(raft/control 전용)
재사용: sharded-index 에픽 Slice2 k-way merge → LIST scatter-gather · 기존 Hertz HTTP 스택 · cluster-PSK mTLS tls.Config
삭제 시점: data_raft = Phase 3 · meta-raft 인덱스 = Phase 4 (eager, 코드와 함께) · transport 머신 = Phase 8. 비가역 지점은 삭제가 아니라 **merge**(Phase 5 벤치 통과가 게이트).
```

## 단계별 로드맵

각 단계는 독립 에픽. 세부 스펙/plan은 단계 진입 시 작성(root worktree, git-untracked).
**순서 원칙: kill-only 측정 → 가역 축소 → eager 재작성(옛 경로 즉시 삭제) → 합의 제거 직후 결정 벤치 → 통과 시 merge.**
가역성은 git이 준다 — 옛 시스템=master, 신규=브랜치, **벤치 통과 전 merge 안 함**. 합의 2회는 Phase 4서 완전 제거되므로 **결정 게이트는 Phase 4 직후(Phase 5)**, perf 무관한 plane-split은 그 뒤.

### Phase 0 — Perf spike ★kill-only 필터 (삭제 0, 가역)
- 기존 시스템에 per-node quorum 메타 write를 shadow로 추가(GET/LIST/delete 불요), 이 에픽의 put_trace로 write-tail을 raft-commit과 conc32 비교.
- **목표**: 명백한 dealbreaker만 차단(quorum-write가 비상식적으로 느리면 STOP).
- **한계(정직)**: raft가 *여전히 도므로* stage-tail ≠ end-to-end. raft의 *부재* 효과(contention 해소+overlap 붕괴)는 측정 불가 — 64KiB서 meta_index 17→11ms인데 http_put_total flat이었던 바로 그 confound. **necessary-not-sufficient: 통과해도 green-light 아님, 진짜 confirm은 Phase 5 벤치.**
- **검증**: quorum-write 꼬리가 raft-commit 꼬리 대비 비상식적으로 크지 않음. 크면 STOP·재평가.

### Phase 1 — Strip-down (disable, 삭제 아님) ✅ DONE
- 비-S3 프로토콜(NFS/NBD/Iceberg/9p)을 seam 뒤로 **비활성화**(재연결 경계 보존, 삭제 아님), request actor 모델 → API 단일 라인.
- **목표**: S3-only 최소 코어·단일 경로로 data-plane 수술 전 surface 축소. greenfield = 하위호환 제거지 *프로토콜은 skip*(재연결 대비)이지 삭제 아님.
- **검증**: S3 PUT/GET/LIST/DELETE green, 단일 경로. 토글로 프로토콜 복구 가능.
- **결과**: NFS4/NBD port=0 기본, `--enable-iceberg` 플래그 추가, executioncluster+execution 패키지 삭제, 비-S3 e2e/colima 테스트 skip 처리.

### Phase 2 — 결정론 placement ✅ DONE
- placement 선택을 결정론으로 교체: group = `hash % numGroups` 동결, HRW static-weighted 노드 선택, Bounded Load → soft 격하.
- **목표**: 고정 토폴로지에서 인덱스 없이 GET이 group 재계산.
- **검증**: GET 라우팅이 인덱스 없이 동작. placement 결정성 테스트.
- **결과**: `groupIDForObject` 순수 함수로 쓰기·읽기 경로 동일 hash 공유(equivalence by construction), `OpRouter.placementGroupIDs` 동결 리스트로 라우팅 일관성 보장, BoundedLoads hot-demotion 쓰기 경로에서 제거(WRH capacity-weighted만 유지). v0.0.527.0.

### Phase 3 — quorum 메타 (data_raft 제거, A의 본체) ✅ DONE
- data_raft ObjectMeta 커밋 → per-node quorum 메타 write(shard에 동봉) + GET quorum read + version-LWW. **옛 data_raft 경로 eager 삭제.**
- **목표**: object PUT의 합의 1회(data_raft) 제거. (하드 문제 #1, 최고 위험.)
- **must-solve (스펙 단계 선결 — LWW가 깨는 지점)**:
  - 조건부 PUT(If-Match/If-None-Match) = CAS → 순수 LWW로 불가, 조정 필요. GrainFS가 광고하면 하드 요구.
  - versioned bucket = 단조 version 순서 → 코디네이터 간 clock skew가 LWW로 역전 가능.
  - multipart 완료 객체 메타 = control-plane raft(manifest) vs quorum-LWW 경계 명확화.
  - quorum 내구성 의미론 = quorum 크기 + commit 기준(직전 streaming-EC 에픽 교훈: data-shards-required / parity-best-effort) + 쓰기 중 노드 실패 처리.
- **검증**: PUT/GET 무합의(data_raft) round-trip, 동시 PUT LWW, RAW(quorum write+read).
- **★early-kill 체크포인트**: 여기서 브랜치 vs master partial 벤치. **data_raft 제거만으로 안 움직이면 = 천장 보존 조기 신호 → Phase 4 만들기 전에 중단.** (정직: quorum write = K+M majority+각 fsync = latency-free 아님, 현실적 win `data_raft 8.2ms→4~5ms`지 →0 아님 — work 이동, fire-and-forget이 보인 것.)

### Phase 4 — index-free LIST (meta_index 제거)
- meta-raft 인덱스 → group별 로컬메타 scatter-gather + k-way merge(에픽 Slice2 재사용) + quorum DELETE/tombstone. **옛 인덱스 eager 삭제 → 합의 2회 완전 제거.**
- **목표**: 인덱스 의존 0. (하드 문제 #2, #5.)
- **must-solve (스펙 단계 선결)**:
  - LIST 일관성 = 동시 쓰기 중 부분/stale read 모델 정의(인덱스의 강일관 열거 소멸).
  - pagination/continuation-token을 scatter-gather 위에서 정의(group별 커서 병합·안정 정렬).
  - fan-out 비용 ∝ numGroups×nodes → metacache/병렬도 설계.
- **검증**: group 횡단 LIST 정확성·delete 전파.

#### Phase 4 진행 현황

- [완료] **S4-0: ★ early-kill 벤치 (WAIVED)** — Phase 3 완료 브랜치 vs master 부분 벤치. 4-node GCP back-to-back PUT 측정. data_raft 제거 단독 효과 확인. **통과 기준**: PUT latency/throughput 가시적 개선 OR 사용자 명시적 계속 결정. → S4-1 게이트. **WAIVED (2026-06-10, 사용자 결정)**: downstream(S4-1…S4-4d)이 모두 구현 완료돼 early-kill 결정은 "계속"으로 암묵 확정됨. 진짜 PUT/GET 판정 벤치는 Phase 5(cross-binary, merge go/no-go)가 수행 — 이 게이트는 그쪽으로 superseded. 이 환경에서 실행 불가한 user-run GCP 벤치라 코드 산출물 없음.
- [완료] **S4-1: DELETE tombstone (quorum meta write)** — `deleteObjectWithMarker`: `deleteQuorumMetaLocal` 대신 `PutObjectMetaCmd{IsDeleteMarker: true}` K-of-N quorum meta write. `PutObjectMetaCmd.IsDeleteMarker` 필드 이미 존재 → 인코딩/팬아웃만 추가. 테스트: DELETE → `readQuorumMetaLocalDecoded` → IsDeleteMarker=true. 전제: S4-0 통과.
- [완료] **S4-2: 로컬 quorum meta 버킷 스캔** — `ShardService.ScanQuorumMetaBucket(bucket, prefix string) ([]PutObjectMetaCmd, error)`: `IterQuorumMetaECShardTargets` 동일 WalkDir 패턴, prefix 필터, `decodeQuorumMetaCmdBlob` 디코딩. 반환: tombstone 포함 전체 (호출처가 필터). 테스트: PUT 2개 + DELETE 1개 populate → ScanQuorumMetaBucket → tombstone 포함 3개 반환 검증. 전제: S4-1.
- [완료] **S4-3: scatter-gather LIST RPC + LWW merge** — ①`ScanQuorumMeta` shard RPC(handler in `shard_service.go`, `ShardService.ScanQuorumMetaBucket` 위임) ②`DistributedBackend.scatterGatherList(ctx, bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, error)`: shardGroup 피어 병렬 fan-out(quorumMetaReadTimeout), (bucket,key) 별 LWW(max ModTime), 정렬, truncate, tombstone(IsDeleteMarker) 필터링. 테스트: in-process 3-node scatter — stale vs fresh conflict → LWW 채택; tombstone → 결과에서 제외. 전제: S4-2.
- [완료] **S4-4a: LIST flip** — `object_list.go`: `ListObjects`/`ListObjectsPage`/`WalkObjects`의 BadgerDB `lat:`+`obj:` 스캔을 `scatterGatherList` 로 교체. pagination: marker 기반(기존 ObjectIndexShardSet 정렬 패턴 재사용). 통합 테스트: 3-node in-process cross-group LIST + pagination(marker 경계) + tombstone 필터 정확성. 전제: S4-3.
- [완료] **S4-4b: meta-index eager delete** — 즉시 제거(dual-path 미유지): ①`forward_receiver.go` ProposeObjectIndex 5개 + ProposeDeleteObjectIndex 1개 ②`cluster_coordinator.go` `indexWriter.ProposeObjectIndex` ③`MetaFSM.objectIndex`/`objectLatest` 맵 + `applyPutObjectIndex`/`applyDeleteObjectIndex`/인코딩 함수들 ④`index_group.go` 전체 + `ObjectIndexShardSet` + `serveruntime` index-group boot/seed wiring + `--object-index-groups` flag ⑤boot wiring 제거. 테스트: `ProposeObjectIndex` grep 0, write-path 테스트 정리(obsolete index 테스트 삭제, forward-receiver는 status-OK로 strip). 전제: S4-4a. **부수효과**: DEK refcount 드라이버 소멸(prune은 S7까지 fail-closed라 무해), `VolumeReplicaSummaries` nil 반환(S4-4c서 quorum-meta로 복원).
- [완료] **S4-4c: GET/HEAD read-path index-free migration** — S4-4b write-path 제거로 노출된 read 경로 회귀 수정. `cluster_coordinator.go`의 GET/HEAD/GetObjectVersion/ReadAt/SetObjectACL/SetObjectTags가 제거된 object index와 비교하던 경로(`routeIndexedReadOrBucket`/`getObjectLocalCurrentFollower`/`readAtLocalCurrentFollower`/`objectMatchesIndexForFollowerRead`)를 모두 제거 → **index-free `routeReadOrBucket`(deterministic placement)** + `ResolveRead`(sole-voter/internal=로컬, user-bucket leader=linearizable, follower=forward)로 단순화. 권위 메타데이터는 GroupBackend의 quorum-meta read(로컬→peer fan-out)가 제공. delete-marker 405 fold 수정: `decodeQuorumMetaBlob`이 `IsDeleteMarker`를 carry하도록 → versioned `GetObjectVersion`이 quorum-meta delete marker를 405로 fold(`TestGetObjectVersion_DeleteMarker_EC` 통과). skip된 11 unit + 1 ginkgo 재조정: obsolete index-mechanism 테스트 삭제(9), missing-object 테스트 salvage/skip(2), internal-bucket Truncate ginkgo un-skip(통과). **결정(minimal-correct, advisor plan-gate)**: leader-down EC read availability(옛 `ResolveObjectPlacementRead` index-gated 경로)는 **의도적으로 drop**(transient election window, reduce→measure→optimize; 복원 필요시 Phase 5 벤치 후). follower-local-read latency 최적화도 drop(follower forward; Phase 5서 측정). 전제: S4-4b.
- [완료] **S4-4d: VolumeReplicaSummaries quorum-meta 복원** — admin `VolumeReplicaSummaries`를 quorum-meta 기반으로 복원. `DistributedBackend.ScanObjectMetaEntries(ctx, bucket, prefix)`(scatterGatherList 위에 tombstone-filtered cmd→ObjectIndexEntry 변환, EC placement 필드 carry) 추가 → `VolumePlacementAdapter`가 `__grainfs_volumes` scan + FSM.ShardGroups + `aggregateVolumeReplicaLayout`(복원)로 per-volume `ClassifyObjectLayout` 집계. adapter에 `*DistributedBackend` 핸들 배선(boot_phases_admin). nil backend/metaRaft 또는 scan 실패 시 nil 반환(incident-only graceful degrade 유지). 테스트: ScanObjectMetaEntries placement-field carry + tombstone filter(integration), aggregateVolumeReplicaLayout(unit). 전제: S4-4c.

### Phase 5 — ★결정 벤치 (cross-binary, merge go/no-go)
- 합의 2회가 제거된 브랜치 vs master 바이너리. **엄격성 필수**(원칙): 외부 S3 앵커 · 같은 VM back-to-back · within-run 비율 · 다회. Phase 0이 거른 뒤 여기서 end-to-end를 실측.
- **스코프 = PUT + GET + HEAD.** GET/HEAD 메타 read가 단일 raft-read→다중 quorum-read로 바뀌므로(이미 0.64–0.72x) **퇴행 위험 실측 필수.**
- **목표**: 물질적 win 입증/반증. (confound 정직: 브랜치엔 strip-down·결정론 placement도 포함 → "consensus 단독"이 아니라 "신규 전체 vs 옛 전체". Phase 1/2는 PUT throughput ~중립 가정이라 confound 허용.)
- **결정(merge-blocker, 동등)**: ① PUT win 입증 **AND** ② **GET/HEAD no-regress.** 하나라도 실패 = 브랜치 폐기(master 무손상). 둘 다 통과 시에만 Phase 6 후 merge.

#### ★ Phase 5 VERDICT (2026-06-11, 사용자 결정) — **GO (eyes-open override → Phase 6 후 merge)**
**사용자가 GO로 결정** (직접 A/B는 인프라 블록으로 미실행; 선행 실측 증거는 PUT win을 시사하지 않으나 사용자가 override). 즉 **측정된 PUT win이 아니라 eyes-open override** — Phase 1-4(결정론 placement·quorum-meta·index-free LIST)의 비-PUT 아키텍처 가치 + S5-2a forward fix 동반 + 진행 우선. (QUIC→TCP flip을 벤치 없이 강행한 것과 동일 성격의 결정권 행사.)
- **정직한 근거 기록**: 게이트 ①(물질적 PUT win)은 실측 미입증 — 직접 devel-vs-master A/B 인프라 블록(S5-2b), 선행 증거(PUT 분해서 raft propose_meta=8%뿐, write_shards 57%+spool 35%=92% disk/EC; sharded-index A/B서 meta 17→11ms 줄여도 http_put FLAT="overlapped not additive")는 material PUT win 불가를 시사. GO는 이 게이트의 override임.
- **귀결**: devel→master merge 진행(GO). 단 로드맵 규칙대로 **Phase 6**(control/data plane 경계 확정 + gossip) 완료 후 merge(비가역 지점). forward fix(S5-2a)는 devel에 있어 merge 시 함께 land. GET/HEAD no-regress(게이트 ②)는 미측정 — Phase 6/merge 전 멀티호스트 read 실측 권장(잔여 리스크).

#### Phase 5 진행 현황
Phase 5는 구현 단계가 아니라 **terminal 결정 게이트**다 (S4-0와 달리 후속 단계가 없다 — 이 게이트가 곧 devel→master merge 판정). 두 슬라이스로 분해:

- [완료] **S5-1: cross-binary A/B 벤치 harness + 결정 규칙 문서** — devel(신규 전체) 바이너리 vs master(옛 전체) 바이너리를 같은 호스트에서 back-to-back로 돌려 PUT+GET+HEAD within-run 비율을 산출하고 merge-blocker 규칙(① PUT win AND ② GET/HEAD no-regress)을 자동 판정하는 harness. 기존 `bench_s3_compat_compare.sh`(cluster boot + warp + minio anchor)를 재사용하는 얇은 래퍼(`benchmarks/cross_binary_ab.sh`) — 두 ref를 transient worktree로 빌드, RUNS회 반복, minio 외부 앵커 옵션, `verdict.md` 산출. 결정 규칙 사전등록 문서 `benchmarks/cross_binary_ab/README.md`. 로컬 스모크로 end-to-end 실행 검증(harness smoke ≠ Phase 5 판정). 전제: Phase 4 완료.
- [완료] **S5-2a: 멀티호스트 forward read-fence 데드락 발견·수정 (선결 블로커)** — `benchmarks/gcp/bench_gcp_cluster.sh`로 실제 GCP 4-node(n2-standard-4, asia-northeast3)를 띄워보니 PUT/GET/HEAD가 비-leader 노드에서 전멸. **investigate로 근본 원인 확정**(라이브 4노드서 비-perturbing 진단으로 `barrier:1, lastApplied:0, committed:1, isLeader:true` 캡처): fresh 데이터그룹의 첫 committed entry는 raft NoOp(leader 선출)/ConfChange인데 ApplyCh 브리지가 이를 필터링 + apply loop가 cursor 미전진 → `DistributedBackend.lastApplied`가 0에 stuck(commitIndex=1). forward read fence(`waitForwardReadFence`→ReadIndex barrier→`WaitApplied(barrier)`)가 `lastApplied>=barrier`를 폴링하는데 log tail이 non-command면 영영 불충족 → 모든 forwarded HeadObject/GetObject(PUT previous-lookup 포함) 5s 타임아웃 → 데드락(읽기는 적용 대기, 적용될 쓰기는 읽기에 막힘). localhost(RTT≈0, leader-직행 PUT)는 마스킹, 실제 멀티호스트만 발현. 기존 테스트가 못 잡음(fake-transport/single-host만). **수정**(`raftnode_adapter.go` ApplyCh 브리지가 전 엔트리 전달 + `apply_actor.go`가 non-command 엔트리에 lastApplied 전진). **실제 4노드 GCP 검증: warp PUT 343 MiB/s, GET 676 MiB/s, HEAD 2080 obj/s, 에러 0, WaitApplied 타임아웃 0**(수정 전 100% 실패). make test-unit + make lint green. 배제됐던 가설: Nagle, per-msg handshake, pool-starvation(conc1도 실패), peer-resolution, mux-establishment, ReadIndex-round-confirm(ReadIndex는 성공·WaitApplied가 진짜). LESSON: actor hot-path 동기 stderr 계측=Heisenbug(genesis DEK Apply 지연 사망)→read-path 폴링/에러-wrap만 안전; harness=systemd-run boot/외부IP/Ops Agent/CPU-pin(Cascade Lake, duckdb SIGILL 방지)/fan-out md5 검증 필요(IAP scp가 122MB 바이너리 간헐 손상). 전제: S5-1.
- [완료] **S5-2b: ★결정 벤치 (merge go/no-go) → VERDICT = GO** (2026-06-11, 사용자 결정 eyes-open override → Phase 6 후 merge; 위 ★Phase 5 VERDICT 참조: 게이트 ① PUT win은 실측 미입증·선행 증거는 불리하나 사용자가 Phase 1-4 비-PUT 가치로 override; forward fix는 devel merge 시 동반 land; 게이트 ② GET/HEAD no-regress는 미측정=잔여 리스크). **직접 devel-vs-master A/B 미실행 상세(인프라 블록)**: forward fix 검증 후 시도(2026-06-11). master에도 동일 read-fence 코드 존재 → A/B는 fix를 양 arm 적용해야 실행(fix=측정 델타와 직교) → master-p5fix 생성·시도했으나 미완: ① IAP scp가 122MB 바이너리 간헐~지속 손상(md5/gzip 검증해도 깨끗한 전송 못 얻음) ② degraded long-running SPOT서 reset후 재부팅 genesis `WaitDEKReady`(60s, meta-raft gen-0, data-group fix 무관) marginal 타임아웃 → 2-arm reliable boot 불가(boot#1 fresh=OK, reboot=flaky; 디스크73%·메모리충분=timing). devel-fixed 단독 멀티호스트는 검증됨: PUT 343/GET 676 MiB/s, HEAD 2080 obj/s, 에러 0. 직접 A/B로 verdict 재확인하려면 안정 인프라(fix를 master land + arm마다 fresh VM/non-SPOT + GCS-relay 전송) 필요. 전제: S5-2a. — forward fix 검증 후 시도(2026-06-11). **확정**: master(옛 전체)에도 동일 read-fence 코드 존재(apply_actor:133/브리지:201/forward fence) → master도 fix 없이는 멀티호스트 forward 데드락 = devel 회귀 아닌 **production 잠재 버그**. 따라서 A/B는 fix를 양 arm에 적용해야 실행 가능(fix는 측정 델타와 직교) → `master-p5fix`(master + fix cherry-pick) 생성, devel-fixed vs master-fixed A/B 시도. **그러나 A/B 미완(인프라)**: ① IAP scp가 122MB 바이너리 간헐~지속 손상(md5 검증이 잡았으나 깨끗한 전송 못 얻음; gzip 40MB로도 일부 실패) ② degraded long-running SPOT VM서 **reset 후 재부팅 genesis가 `WaitDEKReady`(60s, meta-raft gen-0, data-group fix와 무관) marginal 타임아웃** → A/B의 2-arm reliable boot 불가(boot #1 fresh는 OK, reboot은 flaky; 디스크 73%·메모리 충분=리소스 아닌 timing). **남은 작업**: 안정 인프라(fix를 master에 cherry-pick/land + arm마다 fresh VM 또는 non-SPOT/큰 VM + GCS-relay 바이너리 전송)로 cross-binary A/B 실행 → verdict(① PUT win AND ② GET/HEAD no-regress) → GO면 Phase 6 후 merge, NO-GO면 폐기. devel-fixed 단독 멀티호스트 수치는 검증됨(S5-2a: PUT 343/GET 676 MiB/s, HEAD 2080 obj/s, 에러 0). 전제: S5-2a + 안정 인프라.

### Phase 6 — control/data plane 경계 + gossip (벤치 후) → merge
- raft 범위를 membership/bucket/IAM/multipart manifest로 확정, PUT 임계경로 밖 보장. gossip이 soft state(부하/용량/health) → Bounded Load/balancer 공급. (PUT-perf 무관이라 벤치 뒤.)
- **목표**: control/data 경계 코드 확정 후 **master merge**(비가역 지점).
- **검증**: bucket/IAM/multipart 강일관 유지, object PUT/GET는 data-plane raft 미접촉. 전체 e2e green → merge.

#### Phase 6 진행 현황 (Phase 5 GO 후 진입, 2026-06-11 부트스트랩)
기존 인프라 상당: `internal/cluster/gossip.go` + `internal/metrics/bounded_loads.go` 존재, object PUT은 Phase 4 index-free로 meta-raft 직접 미접촉. Phase 6는 대형 빌드가 아니라 **경계 확정·감사 + gossip 공급 확정 + merge-readiness 검증**. 슬라이스:
- [완료] **S6-1: control/data plane 경계 감사 + 갭 수정** — object PUT/GET/HEAD 임계경로가 control-plane raft(meta membership/bucket/IAM/multipart manifest)를 안 타고 data-plane(group raft + per-node quorum-meta)만 타는지 정적 grep + 동적 검증. **VERDICT = CLEAN(위반 0, 코드 수정 불필요)**. ① 정적: PUT(`PutObjectWithRequest`→routeWriteOrBucket→ResolveObjectWrite→GroupBackend `writeQuorumMeta`/group-raft propose)·GET/HEAD(`routeReadOrBucket`→ResolveRead→GroupBackend quorum-meta read)·forward 경로(forward_receiver `dg.Backend()`=GroupBackend) 모두 meta-raft propose/ReadIndex 미접촉. 핫패스의 **유일한** control-backend 접촉=`requireObjectBucket`의 `c.base.HeadBucket`(bucket.go:89 `db.View`=local BadgerDB read, raft 아님)이며 `bucketAssigned()`(in-memory router/meta map) 캐시 히트 시 그것도 skip. ② 동적: `TestControlDataPlaneBoundary_ObjectHotPathDoesNotTouchControlRaft`(control_data_plane_boundary_test.go)—**non-collapsed** 토폴로지(실제 group-raft GroupBackend ≠ control-plane base)에서 PUT+GET+HEAD 후 `base.calls`==0 단언, positive-control `CreateBucket`이 base 기록=spy non-vacuous; mutation-RED 검증 완료(핫패스에 HeadBucket 노출 시 `[HeadBucket:data-bucket]`로 FAIL). **caveat 기록**: (a) legacy single-backend(`WrapDistributedBackend`)는 의도적으로 두 plane을 한 raft로 collapse—경계는 dedicated meta-raft를 갖는 multi-group에서 성립. (b) **multipart manifest 라벨 정정**: ROADMAP은 multipart manifest를 meta-raft control-plane 범위로 적으나, `CompleteMultipartUpload`는 실제로 **group-raft(data-plane)** 에 propose(single-txn atomicity 때문에 quorum-meta 대신 group-raft; meta-raft 아님)하며 PUT/GET/HEAD 핫패스 밖. object_put.go:830 주석 정정. VERSION 0.0.534.0. 전제: Phase 5 GO.
- [대기] **S6-2: gossip soft-state → Bounded Load/balancer 공급 확정** — 기존 gossip.go/BoundedLoads가 부하(RPS)·용량·health를 balancer에 실제 공급하는지 확인, 미비분 보완. PUT-perf 무관(control plane). 전제: S6-1.
- [대기] **S6-3: merge-readiness 검증 → master merge 준비** — ① bucket/IAM/multipart 강일관 유지(e2e) ② object PUT/GET data-plane raft 미접촉(S6-1 검증) ③ **게이트 ② GET/HEAD no-regress 멀티호스트 실측**(Phase 5서 미측정한 잔여 리스크 — 안정 인프라 A/B 또는 read-path 분석) ④ 전체 e2e green. 통과 시 devel→master merge(forward fix 동반, 비가역). 전제: S6-2.

### Phase 7 — numGroups 증설 / 토폴로지 migration
- 하드 문제 #3: group 수 증가를 remap 없이(새 pool) 또는 migration으로. **그 전까지 클러스터는 group 추가 불가**(노드-in-group EC heal만) — Phase 7이 이 운영 제약 해제.

### Phase 6.5 — MetadataStore 추상화 (Clean Architecture)
- `internal/cluster`가 `*badger.Txn`을 직접 사용하는 30개 파일을 `MetadataTxn` / `MetadataStore` 인터페이스로 추상화. BadgerDB 구현체를 `internal/badgermeta`(또는 `internal/raft` 내부)로 이동하고 composition root에서 주입.
- **전제**: Phase 4 완료 후 시작 — meta-index 제거로 cluster 내 BadgerDB 사용처가 제어 플레인(멀티파트 manifest, IAM, bucket 설정, Raft state)으로 축소된 뒤 진행해야 리팩터링 범위가 최소화됨.
- **목표**: cluster Domain 레이어가 DB 구현체에 직접 의존하지 않음(CLAUDE.md Clean Architecture 규칙 완전 준수). 단위 테스트에서 in-memory `MetadataStore` 주입 가능 → BadgerDB 없는 격리 테스트.
- **인터페이스 초안**:
  ```go
  // internal/cluster 에 정의 (사용처 패키지)
  type MetadataTxn interface {
      Get(key []byte) ([]byte, error)
      Set(key, value []byte) error
      Delete(key []byte) error
      Scan(prefix []byte) Iterator
  }
  type MetadataStore interface {
      View(fn func(MetadataTxn) error) error
      Update(fn func(MetadataTxn) error) error
  }
  ```
- **검증**: BadgerDB import가 cluster 패키지에서 0. 기존 integration 테스트 green.

## 분리된 베팅 (성능 비요구, data plane 안정 후)

### Phase 8 — data-plane transport HTTP화
- 노드간 shard/메타 RPC를 **SPKI-pinned mTLS 위 streaming HTTP(Hertz)**로. 자작 mux carrier·chunked framing·per-peer conn pool·desync detection·control/bulk lane 삭제(Hertz client 풀 + HTTP chunked가 대체). 내부 shard RPC = 얇은 S3-유사(`PUT/GET /shard/{bucket}/{key}/{idx}`, body=바이트). FB envelope(`buildShardEnvelope`)→HTTP 헤더/작은 바이너리 preamble.
  - **스트리밍 필수**: 바이너리 shard가 오가므로 req/resp body를 스트림으로 — Hertz `WithStreamRequestBody`/response stream **명시 활성화**(기본 버퍼링 → 대용량 shard 메모리 적재). 전 구간 `io.Reader`/`io.Copy`, full-buffer 금지.
  - **제약**: H3(QUIC) 금지(에픽 제거 사유 재발) → H1.1+풀 또는 H2-over-TCP. zero-CA SPKI pinning 보존(커스텀 tls.Config + `ConnectionState` SPKI 체크). body=raw 바이트, JSON 미사용.
  - **성능**: Hertz netpoll 기본 처리, transport는 PUT 천장 아님(에픽 확인) → throughput 중립. *단순화·유지보수* 목적(perf 레버 아님)이라 perf 수술과 분리.
- **검증**: shard round-trip이 HTTP 스트리밍으로(대용량서 메모리 flat).

### Phase 9 — primitive 라이브러리 추출
- raft/HRW/bounded/gossip 분리. 두 번째 소비자 생길 때. **선행 검토**: hashicorp/raft·memberlist 등 Layer-1 채택 vs 자체 유지.

### Phase 10 — 비-S3 프로토콜 재연결
- 비활성 프로토콜(NFS/NBD/Iceberg/9p)을 신규 코어의 보존된 seam에 재활성화. Iceberg는 강일관 catalog commit → control-plane raft 의존(설계상 자연 정합).
