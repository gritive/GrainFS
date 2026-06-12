# TODO

- [Phase 8 N4 follow-up — load-gated retune, deferred]
  `raftRPCTimeout` (80ms, `internal/cluster/raft_rpc.go`) and `metaRaftRPCTimeout` (500ms,
  `meta_raft_rpc.go`) are kept at their proven values. N4's spec said "retune for warm pooled HTTP POST
  + one retry", but the right value is an R1 throughput question that is **not measurable on macOS**
  (the QUIC→TCP epic established Linux is where the perf signal lives). The election-timeout invariant
  already holds (`TestRaftRPCTimeout_BelowElectionTimeout`: 80ms < 150ms; meta 500ms < 750ms). Retune
  only with a Linux load measurement; retuning on a guess is what N4 deliberately avoided.
