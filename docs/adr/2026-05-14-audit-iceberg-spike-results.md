# ADR: Audit Iceberg Spike Results — 2026-05-14

## Status
ACCEPTED

## Context
이전 21-task plan이 `/codex review`에서 19개 critical findings로 깨졌고, 이어진 5-issue spike-only plan도 DuckDB-vs-local-disk 차단 가능성 등으로 다시 깨졌다. 본 spike는 그 두 차단 가능성을 단순화한 round-trip 한 개로 통과/실패만 가르는 게 목적.

## Findings

### 1. apache/iceberg-go writer 사용성

- 사용한 API: `NewSchema`, `NewPartitionSpec`, `NewDataFileBuilder`, `NewManifestEntry`, `WriteManifest`, `NewManifestListWriterV2`, `ManifestListWriter.AddManifests`, `NewManifestFile`, `ManifestBuilder.Build`
- **그대로 작동:** `WriteManifest(filename, out io.Writer, version, spec, schema, snapshotID, entries)` — `filename`은 Avro 내부 메타데이터에만 쓰이고 실제 disk write는 caller가 전달한 `out io.Writer` 책임. `NewManifestFile(version, path, length, partitionSpecID, addedSnapshotID)` → `ManifestBuilder` 체인으로 `ManifestFile` 구성 후 `AddManifests`에 전달.
- **우회 없이 직접 사용:** `NewDataFileBuilder(spec, content, path, format, nil, nil, nil, recordCount, fileSize)` — partition/logicalType/fixedSize는 nil 허용.
- metadata.json은 `table.Metadata` 빌더 없이 직접 JSON build (v2 포맷). `iceberg-go`에는 `table.Metadata` struct가 있지만 builder/marshal path가 없음 — JSON map 직접 구성이 유일한 경로.

### 2. icebergcatalog.Store metadata 의미

- 확정: `Store`는 Metadata 바이트를 Badger에 직접 저장 (`store.go:204,293`). `MetadataLocation`이 비어도 LoadTable 가능 — `cluster.MetaCatalog`와 의미 다름.
- 다음 plan 적용: audit committer가 단일-노드 모드에서는 `PutObject(metadata)`를 strictly 필요 하지 않지만, cluster 모드와 코드 공유 위해 항상 PutObject 하는 컨벤션 채택 권고.

### 3. storage.Backend.PutObject 사용성

- 확정: `(ctx, bucket, key, io.Reader, contentType) (*Object, error)` 그대로 작동.
- 헬퍼 패턴: `bytes.NewReader(body)` + `"application/octet-stream"`.
- 주의: `LocalBackend.PutObject`는 `HeadBucket` 체크 — `CreateBucket` 선행 필수.
- **Critical deviation:** `LocalBackend.objectPath(bucket, key) = root + "/data/" + bucket + "/" + key` — plan scaffold는 `root/bucket/key`로 잘못 가정. Iceberg content paths(parquet, manifest, manifest-list)는 모두 `root/data/` prefix 포함해야 함.

### 4. DuckDB iceberg_scan(로컬 file path)

- **통과:** DuckDB `SELECT count(*) FROM iceberg_scan('<absolute-path>/v2.metadata.json')` == `1` ✓
- DuckDB 버전: v1.5.2 (Variegata) 8a5851971f
- Iceberg extension 버전: 11fea8ed
- **Critical deviation:** `allow_moved_paths=true`는 DuckDB v1.5.2에서 metadata.json 파일 경로 자체를 base로 사용하는 버그 있음 → `v2.metadata.json/metadata/snap-1-manifest-list.avro` 형태의 잘못된 경로 생성. **해결책:** manifest-list, manifest, data file paths를 모두 정확한 절대 경로로 쓰면 `allow_moved_paths` 불필요 — 그냥 쓰지 않으면 된다.
- 발견된 metadata.json/manifest list 호환성 이슈: 없음 (format-version=2, snapshot 구조 정확하면 DuckDB 정상 읽음).

### 5. Bootstrap metadata.json 작성 노하우

- `iceberg_api.go:391` 헬퍼는 `last-column-id=0`인 빈 schema 기반 → verbatim copy 불가능.
- 우리 schema(`ts timestamptz`, `sa_id string`)용 패턴: `last-column-id=2`, `current-schema-id=0`, `current-snapshot-id=-1`(v1), `last-sequence-number=0`.
- v2 with snapshot: `current-snapshot-id=1`, `last-sequence-number=1`, `snapshots[0]` = `{snapshot-id:1, sequence-number:1, manifest-list:<abs-path>, summary:{operation:append,...}}`.
- Schema JSON: `schema.MarshalJSON()` → `json.RawMessage` 그대로 `"schemas"` 배열에 embed.

## Decisions for the next plan

- **D-SP-1:** `apache/iceberg-go`의 `WriteManifest` + `NewManifestListWriterV2` 사용. metadata.json은 직접 JSON build (Go map + `schema.MarshalJSON()`). table.Metadata struct에 builder/marshal 경로 없음.
- **D-SP-2:** audit committer는 단일-노드/cluster 양쪽 호환 위해 metadata 바이트를 항상 `PutObject(metadata)` 먼저, 그다음 `catalog.Create/CommitTable`. 단일-노드에서는 PutObject가 strictly 필요 없지만 cluster.MetaCatalog는 LoadTable 시 storage 재읽음 (`iceberg_catalog.go:133`).
- **D-SP-3:** DuckDB readback은 **로컬 file path (절대 경로)**로만 검증. `allow_moved_paths=true`는 DuckDB v1.5.2 버그로 사용 금지 — content paths가 정확한 절대 경로면 불필요. cluster + httpfs 변형은 별도 plan.
- **D-SP-4:** 다음 plan의 audit committer 통합 테스트는 `storage.Backend` 인터페이스 그대로 받고 (`interface{}` 우회 금지), `LocalBackend` 사용 시 content paths에 `root/data/` prefix 포함 필수.
- **D-SP-5:** Ring buffer 설계는 본 ADR 범위 밖. 다음 plan에서 Vyukov per-slot seq 또는 channel-based MPSC로 별도 설계.

## OPEN (다음 plan이 풀어야 할 grounding-completed 질문들)

1. **eventstore overlap (`handlers.go:609,794`)**: S3 GET/range-GET이 이미 `ev:` 키로 emit. audit committer가 (a) eventstore에서 폴링해서 Parquet으로 flush, (b) 별도 ring을 새로 만들어 emit. 둘 중 어느 쪽?
2. **cluster.MetaCatalog 경로**: 본 spike는 단일-노드만. cluster 변형은 `MetaCatalog.CreateTable`이 raft propose 후 `LoadTable`이 storage re-read — PutObject 선행이 strictly 필요. 별도 spike or 본 plan의 후속 task로 추가.
3. **server wiring**: `bootState.AddCleanup` (`boot_state.go:171`), `server.New(opts...)` (`server.go:390`)에 audit committer를 어떻게 끼울지. `WithAuditEmitter` 옵션은 `server.New` 호출 **이전에** 적용돼야 함 (Codex 검증).
4. **S3 handler coverage**: PUT/GET/DELETE/LIST/copy/multipart/version/range — `handlers.go`에서 각 op 분기점 매핑 후 hook.
5. **`ops.PutObjectWithACL`** vs raw `backend.PutObject` — Iceberg metadata 쓸 때 `iceberg_api.go:610`은 전자를 씀. audit committer는 어느 쪽?
6. **FlatBuffers ship frame**: `feedback_no_internal_json` 메모리 적용 — emitter→committer 간 직렬화는 JSON 아님.
7. **Compactor scope**: orphan parquet뿐 아니라 orphan metadata.json도 cleanup 대상.
8. **IAM regression test**: 기존 `iam_audit` 라인의 정확한 필드명/타입/순서까지 비교 (이전 plan의 `"event"` 토큰 매칭은 weak).

## Re-open trigger
없음 — 본 spike는 다음 plan의 one-shot 입력.
