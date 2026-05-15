# Transport Mux Protocol Versioning

## ALPN

- `grainfs-mux-v1` is the current ALPN. It uses capability exchange and
  BinaryCodec framing.

## Capability Exchange Wire Format

The capability exchange frame is a BinaryCodec frame with
`Type=StreamCapabilityExchange` (`0x12`). Its payload is exactly 2 bytes.

| Byte | Field | Current value | Notes |
| --- | --- | --- | --- |
| 0 | version | `0x01` | The only accepted value. Mismatch rejects the peer with `version_mismatch`. |
| 1 | features | `0x00` | Bitmask. Any undefined bit rejects the peer with `feature_unsupported`. |

The acceptor expects exactly 2 payload bytes. Shorter or longer payloads are
rejected with `payload_length`.

## Feature Bit Registry

| Bit | Name | Status | Description |
| --- | --- | --- | --- |
| `0x01` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x02` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x04` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x08` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x10` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x20` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x40` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |
| `0x80` | reserved | Unused | Register the bit here before use and extend `ceFeaturesSupportedMask`. |

No feature bits are currently defined. A valid v1 features byte is therefore
`0x00`.

To add a feature bit:

1. Register the bit in this table.
2. Add the bit to `ceFeaturesSupportedMask` in `internal/transport/version.go`.
3. Add handling in `handleCapabilityExchange`.
4. State the wire-compatibility impact in the PR description.

## Version Bump Policy

- Bump from v1 to v2 for wire-incompatible changes, including frame layout,
  capability payload size, mandatory new stream types, or BinaryCodec header
  changes.
- Use v1 feature bits only after documenting the bit and teaching acceptors how
  to handle it. Unsupported acceptors reject with `feature_unsupported`; v1 does
  not guarantee bidirectional compatibility.
- Rollback is a one-way door against the v1 baseline. Downgrades require both
  nodes to run the same v1 behavior.

## Failure Reasons

`CloseWithError` message format:

```text
capability exchange failed: <error-detail>
```

BinaryCodec error response payload:

```text
<reason>: <detail>
```

Valid reason tokens:

- `version_mismatch`
- `wrong_first_stream`
- `payload_length`
- `feature_unsupported`
- `timeout`
- `io_error`

These reason tokens also appear in
`grainfs_transport_ce_total{outcome="failure",reason="..."}`.

## Prometheus Metric

`GrainFS` emits `grainfs_transport_ce_total{role,outcome,reason}` for every
capability-exchange attempt.

| Label | Values |
| --- | --- |
| `role` | `dialer`, `acceptor` |
| `outcome` | `success`, `failure` |
| `reason` | Empty for success, or one of the failure tokens above. |

## Rollback Rules

v1 is the baseline. Older nodes that do not understand capability exchange can
misread the new node's capability exchange response as Raft mux input and reject
it. Operators must use a full-cluster upgrade until a later version adds a
staged transition on a separate ALPN channel.

Review v2 when one of these conditions appears:

- A cluster must intentionally keep nodes that do not support capability
  exchange.
- Peers use all eight feature bits and the layout must grow.
- `BinaryCodec` changes its frame header.
- The protocol needs another stage above the TLS layer in addition to capability exchange.
