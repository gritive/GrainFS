# TODO

## Follow-ups
- [S3 read-plane 단일화 epic] WRITE는 이미 단일경로(simple PUT·multipart 모두 SegmentWriter→putObjectChunked). READ는 layout-driven 분기(B SegmentReader=오늘 객체 / C whole-object EC reader=convert산물 / D local-file=non-EC). 모든 객체 EC-from-PUT·reshard 미사용(owner 확인)이라 C/D 제거해 읽기를 B로 단일화.
  - Phase 1 DONE: 미사용 reshard/convert writer 제거 (v0.0.595.0). 읽기 영향 0.
  - Phase 2 TODO: C(getObjectECReaderAtShardKey/readObjectECAtShardKey + ResolvePlacement→EC arms) + D(openObjectIfSizeMatches/objectPath local readers) + whole-replica E(object_get.go:197-234) 제거. **선결: volume-block N× 복제(RepairReplica가 objectPathV 씀)도 미사용인지 owner 결정** — D 제거를 S3 user 버킷만 할지 volume 포함할지. ErrNotEC 심볼은 유지(RepairShard/placement monitor/appendable).
  - Phase 3 TODO: orphan 정리(commitECObjectWriteResult 등; writeSpooledShards[coalesce]·objectPathV[volume]·ResolvePlacement는 유지).
