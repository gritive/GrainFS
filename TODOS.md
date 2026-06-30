# TODO

- [ ] EC zstd 압축 follow-ups (from 2026-07-01 final review, non-blocking)
  - [ ] `ObjectMeta` SegmentRef wire codec(`codec.go` marshal/unmarshalObjectMeta)에 `StoredSize` parity 추가. 현재 `marshalObjectMeta`는 test-only(`//nolint:unused`)이고 production unmarshal 소비자는 placement 전용(scrubber/quarantine/shardkey)이라 도달 불가하나, 미래 byte-serving read가 이 경로를 타면 StoredSize가 조용히 누락→압축 segment를 비압축으로 읽어 corruption. 미래 방지용 parity.
  - [ ] (ops note) 압축 segment ranged-GET은 `ReadAtSegment`가 `OpenSegment`로 segment 전체(≤16MiB) decompress 후 slice — 작은 range도 full decompress. spec 수용된 non-goal(decompress-then-slice). 대용량 압축객체 동시 ranged-GET 부하 시 메모리/CPU 증폭 주의.