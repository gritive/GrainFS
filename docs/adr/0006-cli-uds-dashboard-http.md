# ADR 0006: CLI uses admin UDS, dashboard uses HTTP

**Date:** 2026-05-07
**Status:** Accepted

## Context

GrainFS의 cluster CLI(`grainfs cluster status|peers|events|remove-peer`)는
HTTP URL endpoint(`http://127.0.0.1:9000`)로 동작했고, volume CLI(`grainfs
volume *`)는 admin Unix socket(`<data-dir>/admin.sock`)으로 통합되어 있어
두 admin CLI 그룹의 endpoint scheme이 일관되지 않았다. 동시에 dashboard
(web UI, `/ui/`)는 직접 HTTP `/api/cluster/*` 및 `/api/eventlog`를 호출하고
있어 같은 라우트가 CLI와 dashboard 양쪽에서 사용되었다.

## Decision

CLI와 dashboard의 endpoint scheme을 명확히 분리한다.

- **CLI는 admin UDS** — `grainfs cluster *` (rotate-key 제외)는 admin
  Unix socket을 통해 `/v1/cluster/*` 라우트를 호출한다. `grainfs volume *`,
  `grainfs dashboard`와 동일 패턴.
- **Dashboard는 HTTP** — web UI는 기존대로 HTTP `/api/cluster/*`,
  `/api/eventlog`를 호출한다. 외부에서 접근 가능한 dashboard에는 UDS가
  적합하지 않고, 공인된 인증(presigned token, S3 인증)으로 보호된다.
- **핸들러 단일 소스** — `*server.Server`의 메서드(`clusterStatus`,
  `removePeerHandler`, `queryEventLog`)는 한 번만 정의하고, admin UDS
  Hertz에 `RegisterClusterAdminUDS`로 등록한다. HTTP `/api/...`와 UDS
  `/v1/cluster/...`가 동일 메서드를 가리킨다.
- **rotate-key는 별도 socket 유지** — `cluster rotate-key`의 rotate.sock
  은 line-delimited JSON 프로토콜(non-Hertz)이라 admin.sock와 합치는 데
  추가 작업이 필요. 본 ADR에서는 분리 유지하고 향후 별도 ADR로 검토.

## Consequences

**Positive**:
- volume CLI와 cluster CLI의 endpoint UX가 일치 (`--endpoint <admin.sock>`).
- UDS 파일 권한(mode 0660 + group)으로 인가 — `localhostOnly` 미들웨어
  중복 제거.
- dashboard는 변경 없이 동작.
- 운영자가 "이 명령은 어디로 dial하지?" 의문 가질 때 단순 규칙 적용 가능.

**Negative**:
- Breaking change: `grainfs cluster --endpoint http://...` 호출 패턴 깨짐.
  운영자/스크립트 마이그레이션 필요. CHANGELOG 및 RUNBOOK으로 안내.
- `cluster join` 루트 이동도 동시 적용(별도 결정이지만 같은 릴리스).

**Out of scope**:
- rotate.sock의 admin.sock 통합.
- HTTP `/api/cluster/*` 라우트 제거 (dashboard 의존).

## References

- 기존 admin UDS 패턴: `internal/volumeadmin/`, `internal/server/admin/`
- 핸들러 단일 소스 메서드: `(*server.Server).clusterStatus` (`internal/server/handlers.go`),
  `(*server.Server).removePeerHandler` (`internal/server/handlers.go`),
  `(*server.Server).queryEventLog` (`internal/server/events_api.go`)
