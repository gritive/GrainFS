# Transport Mux Protocol Versioning

## ALPN

- `grainfs-mux-v1` (current) — capability exchange enabled, framed via BinaryCodec.

## Capability Exchange (CE) wire format

CE 프레임: BinaryCodec frame, Type=`StreamCapabilityExchange` (0x12), Payload 정확히 2바이트.

| Byte | Field    | Current value | Notes                                                           |
|------|----------|---------------|-----------------------------------------------------------------|
| 0    | version  | 0x01          | 단일 값. 불일치 시 즉시 reject (`version_mismatch`).             |
| 1    | features | 0x00          | bitmask. 미정의 비트가 set이면 reject (`feature_unsupported`).  |

Acceptor는 정확히 2바이트 payload를 기대합니다. 짧거나 긴 payload는 `payload_length` reason으로 reject됩니다.

## Feature bit registry

| Bit  | Name       | Status   | Description                                      |
|------|------------|----------|--------------------------------------------------|
| 0x01 | (reserved) | —        | 향후 사용 시 본 문서 update + `ceFeaturesSupportedMask` 확장 |
| 0x02 | (reserved) | —        | 동상                                             |
| 0x04 | (reserved) | —        | 동상                                             |
| 0x08 | (reserved) | —        | 동상                                             |
| 0x10 | (reserved) | —        | 동상                                             |
| 0x20 | (reserved) | —        | 동상                                             |
| 0x40 | (reserved) | —        | 동상                                             |
| 0x80 | (reserved) | —        | 동상                                             |

**현재 정의된 비트 없음.** 모든 비트가 0인 features 바이트만 합법입니다.

새 feature bit을 추가하려면:
1. 이 표에 bit 등록
2. `internal/transport/version.go`의 `ceFeaturesSupportedMask`에 bit 추가
3. `handleCapabilityExchange`에서 해당 bit 처리 로직 추가
4. PR 설명에 wire 호환성 영향 명시

## Version bump policy

- **v1 → v2**: wire-incompatible 변경 시 (frame layout, CE payload size 변경, 새 stream type 의무화, BinaryCodec 헤더 변경 등).
- **v1 안에서 features 비트 사용**: 비트 의미를 본 doc에 등록 + acceptor가 그 비트를 처리하게 함. 미지원 acceptor는 `feature_unsupported`로 reject (v1 양방향 호환 보장 없음).
- **Rollback**: v1 baseline 기준 **one-way door**. 다운그레이드 시 두 노드 모두 v1으로 맞춰야 함.

## Failure reasons (peer-visible)

CloseWithError 메시지 포맷: `"capability exchange failed: <error-detail>"`.

ErrorResponse payload 포맷 (BinaryCodec frame): `"<reason>: <detail>"`.

`<reason>` ∈ {`version_mismatch`, `wrong_first_stream`, `payload_length`, `feature_unsupported`, `timeout`, `io_error`}.

이 reason 토큰은 `grainfs_transport_ce_total{outcome="failure",reason="..."}` Prometheus 레이블과 동일합니다.

## Prometheus metric

`grainfs_transport_ce_total{role,outcome,reason}` — 모든 CE 시도에서 emit됩니다.

| Label     | Values                                     |
|-----------|--------------------------------------------|
| `role`    | `dialer`, `acceptor`                       |
| `outcome` | `success`, `failure`                       |
| `reason`  | `""` (success시), 또는 위 reason 토큰 중 하나 |

## Why no rollback

v1 = baseline. 구버전 (CE 미지원) 노드는 새 노드의 CE 응답을 raft mux 입력으로 오인해 거부됩니다. 마이그레이션은 **클러스터 전체 동시 업그레이드** 또는 **별도 ALPN 채널로 분리**한 단계적 전환 — 후자는 v2 plan에서 다룹니다.

v2 검토 트리거:
- 한 클러스터 안에 CE 미지원 노드가 의도적으로 남아야 하는 시나리오 발생
- features 비트가 8개 모두 소진되어 layout 확장 필요
- `BinaryCodec` 자체 변경 (frame header 바뀜)
- TLS layer 위에 CE 외 새로운 stage 추가 필요
