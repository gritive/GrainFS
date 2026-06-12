# TODO

- [pre-existing HTTP data-plane gap — surfaced by the TCP-transport removal, live since the flip]
  **A shard write aborted mid-stream commits a TRUNCATED shard on the peer over the HTTP
  transport.** The HTTP/1.1 chunked client abort (`remoteSealedShardSink.Abort()` →
  `pw.CloseWithError`) surfaces to the Hertz SERVER's `RequestBodyStream()` as a clean EOF, not a
  read error — so `ShardService.HandleWriteBody` (`internal/cluster/shard_service.go:888`) renames
  its tmp file to the final `shard_N` path and ACKs success, committing the partial bytes. Over the
  (now removed) TCP transport the abort RST-truncated the conn → the server read errored → the tmp
  was cleaned up (`cleanup()` at shard_service.go:1166) → no shard file. **Pre-existing since the
  TCP→HTTP default flip (#735)** — `TestRemoteSealedShardSink_AbortDoesNotCommit` was TCP-only and
  never exercised the HTTP path; the N2/N3 teardown (renaming it to the HTTP transport) revealed it,
  did not cause it. **EC-masked, not silent corruption:** a truncated shard fails AEAD on read and
  is reconstructed from the other shards, and the orphan tmp/shard is scrubber-GC'd; the
  object-level commit safety still holds (`TestMultiNodeStreamingPUT_DataShardFailure_NoCommit`
  passes over HTTP, because a transport-ERROR failure is a different path from a mid-stream abort).
  **Fix direction (separate increment):** the sealed shard is a finite buffer, so the sender knows
  its length — carry the expected sealed length in the shard-write request envelope
  (`BuildSealedShardWriteRequest`) and have `HandleWriteBody` reject (cleanup, error response) a body
  shorter than that before the tmp→final rename. `TestRemoteSealedShardSink_AbortDoesNotCommit` is
  `t.Skip`-ped with this reference until then.


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
