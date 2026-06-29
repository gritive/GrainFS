# TODO

## [P1] PUT 경로 MD5 낭비 제거 (진행중)

- [ ] spool MD5 lazy화: `Content-MD5` 헤더 없을 때 spool 단계 MD5 스킵 (~15% PUT CPU 절감)
- [ ] `writeDataShards` segment MD5 완전 제거: ETag가 `WriteSegmentBytes`에서 `_` 폐기되므로 `md5.Sum` 완전 제거

## [P2] spool 이중 암호화 제거 (streaming EC)

- [ ] spool 암호화 → spool 복호화 → EC shard 재암호화 사이클 제거. plaintext를 EC path로 직접 streaming.
