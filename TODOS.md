# TODO

- [ ] production-dead `storage.LocalBackend` 제거 (별도 에픽, 접근 결정 대기)
  - 배경: single-node serve도 `NewDistributedBackendForGroup(group-0)`를 쓰고, `storage.LocalBackend`는
    production에서 구성되지 않음(test-only, ~3000 LoC + storagepb codec/Badger 영속화). storagepb
    `SegmentRef`가 `stored_size`를 흘리는 codec #2 gap도 이 백엔드에 속함(현재 라이브 read 미도달).
  - 걸림돌: ~87개 테스트 파일이 substrate로 사용. 특히 `internal/server/*_test.go` 29개는
    `setupTestServerWithBackend()`(server_test.go) 경유로 production server-layer(auth/ACL/versioning/
    lifecycle) 커버리지를 이 백엔드 위에서 검증 → 무작정 삭제 시 커버리지 손실.
  - 접근 미결(A/B/C): (A) LocalBackend 유지 + codec #2 gap은 note로 / (B) server 테스트를 production
    단일노드 `DistributedBackend`로 재홈 후 삭제 / (C) 얇은 in-memory test 백엔드 신규 후 삭제.
    → 별도 spec+plan gate에서 정확한 비용으로 확정 후 진행.