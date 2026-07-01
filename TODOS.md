# TODO

<!-- LocalBackend-removal follow-ups resolved: ListMultipartUploads bucket-existence
     and append-segment StoredSize codec parity fixed (this PR); EC ranged-GET
     compressed-segment re-decompress moved to GitHub Projects #2 (GrainFS). A
     systemic authz question surfaced (non-existent bucket list → 403 no_grant vs
     404 NoSuchBucket) is also tracked on GitHub Projects #2. -->

<!-- Streaming-only unification follow-ups resolved: one-shot ReadShardRange RPC,
     ReadObject/readShards + full-shard collector, and WriteLocalShardContext
     retired; real-ShardService EC integration test added; orphaned buffered
     stripeDeinterleave removed (this PR). The in-heap full-shard cache cleanup
     (former item 3a) was withdrawn as invalid — the shard cache is LIVE for
     ranged GET (readDataShardAt → ReadAt), so it and the --shard-cache-size flag
     stay. Deferred to GitHub Projects #2 (GrainFS): warm-GET cache restore
     pending cluster bench (3b), EC streaming reader close/cancel hang (6), and
     streaming read peer-health granularity (marks healthy at stream-open, not
     after full shard read). -->
