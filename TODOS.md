# TODO

- [Phase 8 N4 follow-up — load-gated retune, deferred]
  `v2RaftRPCTimeout` (80ms, `internal/cluster/raft_rpc.go`) and `v2MetaRPCTimeout` (500ms,
  `raftv2_meta.go`) are kept at their proven values. N4's spec said "retune for warm pooled HTTP POST
  + one retry", but the right value is an R1 throughput question that is **not measurable on macOS**
  (the QUIC→TCP epic established Linux is where the perf signal lives). The election-timeout invariant
  already holds (`TestRaftRPCTimeout_BelowElectionTimeout`: 80ms < 150ms; meta 500ms < 750ms). Retune
  only with a Linux load measurement; retuning on a guess is what N4 deliberately avoided.

- [M5 raft v1→v2 migration tail — separate behavior-neutral cleanup slice, NOT Phase 8]
  The `v2`-prefixed raft naming (`v2EncodeRPC`/`v2DecodeRPC`/`v2RPCType*` in `raftv2_codec.go`,
  `v2RaftRPCTimeout`/`v2RaftSnapshotTimeout` in `raft_rpc.go`, `v2Meta*` in `raftv2_meta.go`, the
  `raftv2_*.go` filenames, and the `PR 30b drops the type alias and renames the v2 file` /
  `PR 30b deletes v1 and renames this file` notes in `meta_transport.go`/`raftv2_meta.go`) is
  leftover from the M5 raft v1→v2 migration, whose v1 package (`internal/raft/quic_rpc.go`) is already
  deleted. Phase 8 N4 cleaned the *transport* freeze comments (byte-identical-to-v1, frozen-until-PR-30)
  but intentionally left the `v2*` symbol/file rename alone (M5 scope, not Phase 8; a whole-family
  rename is its own behavior-neutral slice). Do the `v2* → *` rename as a dedicated M5-completion PR.

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

- [pre-existing deadlock — root cause captured, transport-independent, RUN-verified on clean master]
  **Joiner's legacy data-raft `node.Start()` is wired AFTER invite-join Phase-2, so a
  voter-add during Phase-2 deadlocks the join.** Only triggered when `--node-id ==
  --raft-addr` (the config the 5-node EC e2e uses, `tests/e2e/cluster_ec_test.go:80`).
  **Root cause (captured, not guessed):** during invite-join Phase-2 the leader runs
  `addJoinedNodeToLegacyDataRaft` (`internal/serveruntime/boot_phases_forwarders.go:295`),
  which calls `node.AddVoterCtx(ctx, nodeID, addr)` — but ONLY when `nodeID == addr`
  (line 287 guard). `AddVoterCtx` makes the leader replicate AppendEntries to the
  joiner's data-raft. The joiner's data-raft actor loop (`internal/raft/node.go` `run()`,
  started by `node.Start()`) is not started until `bootStorageRuntime`
  (`boot_phases_storage_runtime.go:261`), which runs AFTER `bootWALAndForwardersPart1`
  (the phase containing invite-join Phase-2). So the joiner receives AppendEntries before
  its actor drains `cmdCh` → `HandleAppendEntries` (node.go:438) blocks forever on the
  reply channel → leader's AddVoter times out (60s) → join fails → `waitForPortsParallel`
  fails (`cluster_ec_test.go:127`).
  **VERIFIED PRE-EXISTING:** built + ran the EC reconstruct test on clean pre-Phase-8
  master (commit 8c768d92, the canonical Phase-7 tip) — it FAILS identically at
  `cluster_ec_test.go:127`. Confirmed transport-independent: fails the same way on
  `--transport tcp` and `--transport http`. This was long mis-filed in memory as a
  "macOS resource flake" (S5c-3 entry); it is in fact a boot-ordering deadlock with a
  timing-sensitive trigger under the `node-id == raft-addr` config (reproduced on clean
  master + both transports; not claimed 100%-deterministic, but root-caused, not random).
  **Fix direction (separate work from the transport rebuild):** start the joiner's
  legacy data-raft actor BEFORE invite-join Phase-2, or add the joiner as a learner
  (not a direct voter) and promote only after its actor is running — matching the
  documented JoinMode learner path (`types.go:286`) instead of the
  `addJoinedNodeToLegacyDataRaft` direct `AddVoterCtx`. NOT fixed by any transport change.
