# TODO

- [pre-existing flake, Phase 8-independent] `internal/cluster` package flakes under
  the full parallel `go test` run (not in isolation). Observed:
  `TestShardPlacementMonitor_RepairsMissingSegmentShard_EndToEnd` failing with
  `forward: no reachable peer` (a transient peer-reachability/timing issue in the
  EC shard-repair end-to-end path). Confirmed PRE-EXISTING and unrelated to the
  Phase 8 transport work: the package still flakes with `-skip TestHTTPGroupCluster`
  (my new HTTP test excluded), and the test passes deterministically in isolation
  (`-run ... -count=5`). Investigate the parallel-run flake (likely a too-short
  reachability/deadline window or a port/resource race in the placement-monitor test
  harness) and stabilize it. Use `make test-unit` repeated runs to reproduce.
