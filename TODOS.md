# TODO

- [Phase 8 N4 follow-up — retune NOT warranted by local measurement; Linux re-run optional]
  `raftRPCTimeout` (80ms, `internal/cluster/raft_rpc.go`) and `metaRaftRPCTimeout` (500ms,
  `meta_raft_rpc.go`) are kept at their proven values — now backed by a local measurement
  (2026-06-13, macOS 8-core, `TestMeasureRaftRPCLatency` in
  `internal/cluster/raft_rpc_latency_measure_test.go`, gated by `GRAINFS_MEASURE_RAFT_RPC=1`):
  warm pooled HTTP POST round-trip (FB encode → POST → decode, real inbound codec work) over
  9,000+ samples shows ZERO breaches of 80ms in every condition — idle heartbeat p99=46µs/max=199µs,
  idle bulk(100×1KiB) max=449µs, 8× concurrent bulk load max=1.8ms, and 2×-core CPU saturation +
  8× bulk load p99.9=11.5ms/max=32.2ms. Read: 80ms is not too tight (0 breaches saturated) and
  must not be lowered (saturated tail max reaches 2.5× below it — a "warm HTTP is fast" cut to
  ~40ms would breach under saturation). 500ms meta keeps 15× headroom over the saturated max.
  The election-timeout invariant holds (`TestRaftRPCTimeout_BelowElectionTimeout`: 80ms < 150ms).
  The measurement tool is committed and reusable; re-run on Linux only if R1 throughput work
  reopens the question.
