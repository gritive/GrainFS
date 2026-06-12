# TODO

- [Phase 8 N4 follow-up — load-gated retune, deferred]
  `raftRPCTimeout` (80ms, `internal/cluster/raft_rpc.go`) and `metaRaftRPCTimeout` (500ms,
  `meta_raft_rpc.go`) are kept at their proven values. N4's spec said "retune for warm pooled HTTP POST
  + one retry", but the right value is an R1 throughput question that is **not measurable on macOS**
  (the QUIC→TCP epic established Linux is where the perf signal lives). The election-timeout invariant
  already holds (`TestRaftRPCTimeout_BelowElectionTimeout`: 80ms < 150ms; meta 500ms < 750ms). Retune
  only with a Linux load measurement; retuning on a guess is what N4 deliberately avoided.

- [known flake — diagnosed, NOT bounded-reproducible; Phase 8-independent, pre-existing]
  `internal/cluster` package: rare intermittent `object not found` on GET under repeated/concurrent
  load. Observed on `TestMultiNodeStreamingPUT_HTTP_ParityShardFailure_CommitsAndReads` and
  `TestMultiNodeStreamingPUT_K3_RoundTrip` (the multi-node in-process GET reconstruction path) when
  run under `-count` repeat or full-package parallel load. **Confirmed pre-existing and unrelated to
  the abort-truncation trailer fix:** symmetric measurement — base (no fix) and fix both pass 0/10 at
  isolated `-count=1` and both FAIL at `-count=20`; the fix does not change the rate. Likely the same
  resource-contention class as the placement-monitor flake below (CPU/FD starvation tripping a
  deadline/probe or a cross-test shared-resource window). CI runs `-count=1` per package, where it is
  reliably green.

- [known flake — diagnosed, NOT bounded-reproducible; Phase 8-independent, pre-existing]
  `internal/cluster` package: a rare intermittent failure under heavy concurrent
  load (observed once during a full `make test-unit` as
  `TestShardPlacementMonitor_RepairsMissingSegmentShard_EndToEnd`, fast-fail ~0.14s).
  **Investigation outcome (investigate skill, Iron Law — no guess-fix without a
  captured root cause):** confirmed PRE-EXISTING and unrelated to the Phase 8 transport
  work (still flaked with `-skip TestHTTPGroupCluster`; passes in isolation
  `-count=5`). The named test is fully local ("self" addressing, `t.TempDir()`, no
  network, no fixed ports, no `t.Parallel`) — the `forward: no reachable peer` /
  `ec rewrap ... assert.AnError` lines in the failure output were OTHER tests'
  interleaved logs from the concurrent multi-package run, not this test's own path.
  Could NOT reliably reproduce to capture the actual failing assertion: 4 dedicated
  `go test ./internal/cluster/` runs + 1 full `make test-unit` all PASSED. Likely a
  resource-contention sensitivity (CPU/FD starvation when many packages run
  concurrently) tripping a deadline/probe or a cross-test shared-resource window.
  **Next step (dedicated repro campaign, out of bounded scope):** loop the full suite
  to catch it with `-v`, e.g. `for i in $(seq 1 50); do make test-unit 2>&1 | tee
  /tmp/run-$i.log; done` then grep the failing run's `--- FAIL` block for the exact
  assertion, and root-cause from there. Tracked as a known flake, not an active
  blocker for the Phase 8 feature work.
