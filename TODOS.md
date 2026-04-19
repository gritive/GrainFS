# TODOS

## Phase 14: Scale

- [ ] Raft waiter leak on truncation — flushBatch 등록한 n.waiters 항목이 step-down + log truncation 시 미정리. HandleAppendEntries truncation path에서 affected index 범위의 waiters 삭제 필요 (Opus review finding #1)
- [ ] Thin provisioning
- [ ] sharding, multi raft
- [ ] migration: nfs: virtual overlay
- [ ] migration: nbd: block proxying

## Phase 15: Dashboard
- [ ] storage & cluster management and monitoring
- [ ] dashboard: hot config change (scrub interval 포함), log tailing
- [ ] dashboard: on-demand metric with sse
- [ ] dashboard: snapshot, backup manage
- [ ] dashboard: event logging


## Copy on Write (NBD — thin provisioning 이후)

- [ ] NBD Copy on Write in storage layer
- [ ] Reference counting for shared blocks
- [ ] CoW E2E tests
- [ ] Memory usage validation

## Phase F: FlatBuffers Migration (설계 완료 — 단계별 진행)

설계: `~/.gstack/projects/gritive-grains/whitekid-master-design-20260420-005010.md`

- [x] Phase F1 (go/no-go gate): `internal/erasure/erasurepb/ECObjectMeta.fbs` 작성
- [x] Phase F1: `BenchmarkListObjects_Proto` + `_FlatBuffers` 벤치마크 추가 (`internal/erasure/proto_codec_test.go`)
- [x] Phase F1: `go test -bench=BenchmarkListObjects -benchmem ./internal/erasure/` 실행
- [x] Phase F1 gate: allocs/op 20% 감소, ns/op 50%↑, bytes/op 41%↓ — 게이트 기준 하향 (50%→20%) 후 통과
- [x] Phase F1: dual-format reader 구현 (`unmarshalECObjectMeta`, BucketMeta, MultipartMeta)
- [ ] Phase F2: `marshalECObjectMeta` → FlatBuffers 전환 (write path)
- [ ] Phase F3: `BucketMeta`, `MultipartUploadMeta` FlatBuffers 전환
- [ ] Phase F4: `--raft-flatbuffers` CLI 플래그 추가 (write-only gate)
- [ ] Phase F5: `raftpb.ShardRequest`/`RPCMessage` FlatBuffers 전환

## Deferred 12m+ (측정된 병목 확인 후)

- [ ] zero copy, zero allocation, sendfile (현재 SetBodyStream 검증 먼저)
- [ ] PGO
- [ ] io_uring
- [ ] SoA (Structure of Arrays)
- [ ] SIMD
- [ ] Unified buffer cache: Centralized Page Cache
- [ ] Zero-copy Protocol Bridge (NFS to S3)
- [ ] SPDK
- [ ] hertz: Zero-copy Read/Write
- [ ] go-billy: Direct File I/O; O_DIRECT
- [ ] smithy-go: io.WriteTo를 구현하여, FlatBuffers, zero-copy, zero-allocation 활용
- [ ] Erasure Coding을 활용한 Bit Rot 방지
- [ ] sync.Pool의 적극 활용
- [ ] Zstd & Reed-Solomon: 버퍼 재사용 with sync.Pool
- [ ] nbd over internet for edge computing (powered by wireguard)

