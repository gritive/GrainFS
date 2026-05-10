# ADR 0010: Relocate Bucket-Upstream Admin/CLI Surface

## Status
Accepted (2026-05-09). Supersedes the surface-placement portion of ADR 0009;
0009 의 FSM/snapshot/AAD 결정은 그대로 유지된다.

## Context
ADR 0009은 bucket-scoped upstream credentials를 IAM Store에 저장하기로 결정하면서
admin path와 CLI도 `iam` 카테고리 아래(`/v1/iam/bucket-upstream`,
`grainfs iam bucket-upstream …`)에 배치했다. v0.0.123.0 출시 후 사용자 검토에서
`iam`은 principal(SA/AccessKey/Grant) 축인 반면 `bucket-upstream`은 resource(bucket)
축의 외부 자격증명 설정이라 카테고리가 미스핏임이 확인됐다.

## Decision
- CLI: `grainfs iam bucket-upstream {set,…}` → `grainfs bucket upstream {put,get,list,delete}`
- Admin path:
  - `POST /v1/iam/bucket-upstream` → `PUT /v1/buckets/upstream`
  - `GET  /v1/iam/bucket-upstream` → `GET /v1/buckets/upstream`
  - `GET  /v1/iam/bucket-upstream/:bucket` → `GET /v1/buckets/:bucket/upstream`
  - `DELETE /v1/iam/bucket-upstream/:bucket` → `DELETE /v1/buckets/:bucket/upstream`
- HTTP method: POST → PUT (idempotent upsert; grant 와 align)
- 구버전 path/CLI는 즉시 제거 (admin UDS는 내부 인터페이스)
- FSM(`internal/iam`), snapshot trailer, AAD `"bucket-upstream:"+bucket`,
  MetaCmdType IDs 32/33, FlatBuffers payload 모두 보존

## Consequences
- v0.0.122 ↔ v0.0.132 raft 호환성 유지 (payload/snapshot 무변경)
- BREAKING: 기존 CLI 명령 사용자 스크립트는 수정 필요. CHANGELOG 매핑 표 제공
- 향후 bucket sub-resource(policy/lifecycle/event 등)가 같은 `/v1/buckets/:bucket/*`
  prefix와 `grainfs bucket *` 트리에 합류 가능

## Out of scope
FSM을 별도 패키지로 분리하는 것은 raft snapshot/AAD/types 의존이 깊어 별도 ADR/PR
대상으로 남긴다.
