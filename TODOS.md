# TODO

## Follow-ups

- [P2] propose 타임아웃 phantom-commit 윈도우에서의 shard cleanup 위험 (pre-existing, multipart-LIST 수정과 무관). `b.propose`가 timeout을 반환했지만 raft entry가 실제로 commit되는 경우(`backend.go:41-42`에 문서화), `commitCompleteMultipartObjectWriteResult`(`object_put.go:754`)와 `runChunkedPutWithParts`(`segment_backend.go`)의 propose-실패 cleanup 경로가 `deleteShardsAsync`로 shard를 지우는데, 이후 FSM apply가 manifest를 삭제하고 객체 메타를 (이미 삭제된) shard에 연결시킴 → 읽을 수 없는 객체 + 재시도 불가. 모든 propose 기반 커밋에 공통인 깊은 이슈로, cleanup 전 commit 확정(read-back) 메커니즘이 필요. codex code-gate(2026-06-14)가 multipart-LIST 수정 리뷰 중 발견.
