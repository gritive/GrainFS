# Zero Config EC Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove manual EC profile configuration from the public `grainfs serve` CLI and make object storage EC profiles derive from the actual object write-node set.

**Architecture:** `AutoECConfigForClusterSize` becomes the single zero-config profile policy. Server startup wires that policy into backends without operator-provided `k/m`, and object index metadata records the profile derived from the selected placement group voter count. Tests and benchmarks express desired profiles through node count, not `--ec-data` or `--ec-parity`.

**Tech Stack:** Go 1.26+, Cobra CLI, Badger-backed cluster metadata, `GrainFS` cluster coordinator, Go e2e harness, shell benchmark scripts.

---

## File Structure

- Modify `internal/cluster/ec.go`: rename default EC constants to auto-policy constants and implement `7 -> 5+2`, `8+ -> 6+2`.
- Modify `internal/cluster/ec_test.go`: cover the full zero-config profile table.
- Modify `cmd/grainfs/serve.go`: remove public `--ec-data` and `--ec-parity` flags.
- Modify `cmd/grainfs/serve_config.go`: remove EC flag parsing.
- Modify `cmd/grainfs/serve_cluster_key_test.go`: remove test-only EC flag registration and add public flag absence coverage.
- Modify `internal/serveruntime/config.go`: remove `ECData`, `ECParity`, and `ECExplicit`.
- Modify `internal/serveruntime/run.go`: remove explicit EC branch and configured EC log fields.
- Modify `internal/cluster/cluster_coordinator.go`: compute object-index `ECData`/`ECParity` from the selected placement group's voter count.
- Modify `internal/cluster/cluster_coordinator_test.go`: add regression coverage for group-derived object-index EC metadata.
- Modify `tests/e2e/cluster_harness_test.go`: remove EC option fields and reject removed flags in `ExtraArgs`.
- Modify e2e tests under `tests/e2e/`: delete `ECData`/`ECParity` options and explicit EC CLI args; adjust node counts where the test needs a specific profile.
- Modify benchmark scripts under `benchmarks/`: delete runnable `--ec-data`/`--ec-parity` arguments and `EC_DATA`/`EC_PARITY` environment knobs.
- Modify `README.md`: remove EC flags from CLI docs and document the auto profile table plus three-node production minimum.

## Data Flow

```text
serve CLI
  -> buildClusterConfig()
      -> serveruntime.Config without manual EC fields
          -> serveruntime.Run()
              -> effectiveEC = AutoECConfigForClusterSize(object write-node count)
                  -> distBackend.SetECConfig(effectiveEC)
                  -> normal shard groups seeded with effectiveEC.NumShards()

PUT object
  -> ClusterCoordinator.routeObjectWrite()
      -> SelectObjectPlacementGroup(bucket, key, groups, coordinatorEC)
          -> selected group PeerIDs
              -> data group backend writes shards with group EC config
              -> commitObjectIndex() stores AutoECConfigForClusterSize(len(group.PeerIDs))
```

## Task 1: Update Zero-Config EC Policy

**Files:**
- Modify: `internal/cluster/ec.go`
- Modify: `internal/cluster/ec_test.go`

- [ ] **Step 1: Write the failing auto-profile table test**

Replace `TestAutoECConfigForClusterSize` in `internal/cluster/ec_test.go` with:

```go
func TestAutoECConfigForClusterSize(t *testing.T) {
	tests := []struct {
		nodes int
		want  ECConfig
	}{
		{0, ECConfig{}},
		{1, ECConfig{DataShards: 1, ParityShards: 0}},
		{2, ECConfig{DataShards: 1, ParityShards: 1}},
		{3, ECConfig{DataShards: 2, ParityShards: 1}},
		{4, ECConfig{DataShards: 2, ParityShards: 2}},
		{5, ECConfig{DataShards: 3, ParityShards: 2}},
		{6, ECConfig{DataShards: 4, ParityShards: 2}},
		{7, ECConfig{DataShards: 5, ParityShards: 2}},
		{8, ECConfig{DataShards: 6, ParityShards: 2}},
		{9, ECConfig{DataShards: 6, ParityShards: 2}},
		{32, ECConfig{DataShards: 6, ParityShards: 2}},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, AutoECConfigForClusterSize(tt.nodes))
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/cluster -run TestAutoECConfigForClusterSize -count=1
```

Expected: FAIL for `7`, `8`, `9`, and `32`; current code returns `4+2` for all `>=6`.

- [ ] **Step 3: Rename EC auto constants and implement the new policy**

In `internal/cluster/ec.go`, replace the current default constants with:

```go
const (
	// MaxAutoDataShards caps zero-config data shard fan-out. Parity remains
	// fixed at two for normal clusters; larger durability policies belong in a
	// future expert/archival policy, not the startup CLI.
	MaxAutoDataShards = 6
	AutoParityShards  = 2
	// maxECPooledReadObjectSize caps the all-data-present read prefetch path so
	// large object GETs keep the previous streaming memory profile.
	maxECPooledReadObjectSize = 128 << 20
)
```

Replace `AutoECConfigForClusterSize` with:

```go
// AutoECConfigForClusterSize returns the zero-config EC profile for a cluster
// size. Operators do not provide k/m at startup; node count selects the profile.
func AutoECConfigForClusterSize(nodes int) ECConfig {
	switch {
	case nodes <= 0:
		return ECConfig{}
	case nodes == 1:
		return ECConfig{DataShards: 1, ParityShards: 0}
	case nodes == 2:
		return ECConfig{DataShards: 1, ParityShards: 1}
	case nodes == 3:
		return ECConfig{DataShards: 2, ParityShards: 1}
	default:
		data := nodes - AutoParityShards
		if data > MaxAutoDataShards {
			data = MaxAutoDataShards
		}
		return ECConfig{DataShards: data, ParityShards: AutoParityShards}
	}
}
```

Also update the `EffectiveConfig` comment to remove explicit-profile language:

```go
// EffectiveConfig returns target when it fits the supplied node set. Startup
// passes an auto-selected target; tests may still inject explicit configs with
// SetECConfig to exercise low-level EC behavior.
func EffectiveConfig(n int, target ECConfig) ECConfig {
	if !target.IsActive(n) {
		return ECConfig{}
	}
	return target
}
```

- [ ] **Step 4: Run focused tests and verify they pass**

Run:

```bash
go test ./internal/cluster -run 'TestECConfig_IsActive|TestAutoECConfigForClusterSize' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add internal/cluster/ec.go internal/cluster/ec_test.go
git commit -m "refactor: derive ec profile from cluster size"
```

## Task 2: Remove Manual EC CLI And Runtime Config

**Files:**
- Modify: `cmd/grainfs/serve.go`
- Modify: `cmd/grainfs/serve_config.go`
- Modify: `cmd/grainfs/serve_cluster_key_test.go`
- Modify: `internal/serveruntime/config.go`
- Modify: `internal/serveruntime/run.go`

- [ ] **Step 1: Add the failing public flag absence test**

In `cmd/grainfs/serve_cluster_key_test.go`, add this test after `newClusterTestCmd`:

```go
func TestServeCmd_RemovesManualECFlags(t *testing.T) {
	require.Nil(t, serveCmd.Flags().Lookup("ec-data"))
	require.Nil(t, serveCmd.Flags().Lookup("ec-parity"))
}
```

Add the missing import:

```go
	"github.com/stretchr/testify/require"
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./cmd/grainfs -run TestServeCmd_RemovesManualECFlags -count=1
```

Expected: FAIL because both flags are still registered.

- [ ] **Step 3: Remove test-only EC flag registration**

In `cmd/grainfs/serve_cluster_key_test.go`, delete these two lines from `newClusterTestCmd`:

```go
	cmd.Flags().Int("ec-data", 4, "")
	cmd.Flags().Int("ec-parity", 2, "")
```

- [ ] **Step 4: Remove public EC flags**

In `cmd/grainfs/serve.go`, delete:

```go
	serveCmd.Flags().Int("ec-data", cluster.DefaultDataShards, "target max data shards k; actual k scales with node count (EffectiveConfig, 3+ nodes)")
	serveCmd.Flags().Int("ec-parity", cluster.DefaultParityShards, "target max parity shards m; actual m=max(1,round(n×m/(k+m)))")
```

If the only remaining use of the `cluster` import in `serve.go` was for these constants, remove that import. If `serve.go` still uses `cluster` elsewhere, keep it.

- [ ] **Step 5: Remove EC fields from runtime config**

In `internal/serveruntime/config.go`, replace the storage section:

```go
	// Storage / EC
	DirectIO       bool
	MeasureReadAmp bool
	ShardCacheSize int64
	ECData         int
	ECParity       int
	ECExplicit     bool
	PackThreshold  int
```

with:

```go
	// Storage
	DirectIO       bool
	MeasureReadAmp bool
	ShardCacheSize int64
	PackThreshold  int
```

- [ ] **Step 6: Remove EC flag parsing**

In `cmd/grainfs/serve_config.go`, delete:

```go
	cfg.ECData, _ = cmd.Flags().GetInt("ec-data")
	cfg.ECParity, _ = cmd.Flags().GetInt("ec-parity")
	cfg.ECExplicit = cmd.Flags().Changed("ec-data") || cmd.Flags().Changed("ec-parity")
```

- [ ] **Step 7: Remove explicit EC branch from startup**

In `internal/serveruntime/run.go`, replace:

```go
	clusterSize := 1 + len(peers)
	effectiveEC := cluster.AutoECConfigForClusterSize(clusterSize)
	if cfg.ECExplicit {
		effectiveEC = cluster.ECConfig{DataShards: cfg.ECData, ParityShards: cfg.ECParity}
		if !effectiveEC.IsActive(clusterSize) {
			return fmt.Errorf("explicit EC profile %d+%d requires %d nodes, cluster has %d",
				cfg.ECData, cfg.ECParity, effectiveEC.NumShards(), clusterSize)
		}
	}
	if !effectiveEC.IsActive(clusterSize) {
		return fmt.Errorf("no effective EC profile for cluster size %d", clusterSize)
	}
```

with:

```go
	clusterSize := 1 + len(peers)
	effectiveEC := cluster.AutoECConfigForClusterSize(clusterSize)
	if !effectiveEC.IsActive(clusterSize) {
		return fmt.Errorf("no effective EC profile for cluster size %d", clusterSize)
	}
```

Replace the EC startup log block:

```go
	log.Info().
		Bool("explicit", cfg.ECExplicit).
		Int("configured_k", cfg.ECData).
		Int("configured_m", cfg.ECParity).
		Int("effective_k", effectiveEC.DataShards).
		Int("effective_m", effectiveEC.ParityShards).
		Bool("active", effectiveEC.IsActive(len(allNodes))).
		Int("cluster_size", len(allNodes)).Msg("cluster EC configured")
```

with:

```go
	log.Info().
		Str("mode", "auto").
		Int("effective_k", effectiveEC.DataShards).
		Int("effective_m", effectiveEC.ParityShards).
		Bool("active", effectiveEC.IsActive(len(allNodes))).
		Int("cluster_size", len(allNodes)).Msg("cluster EC configured")
```

- [ ] **Step 8: Run focused tests and compile packages**

Run:

```bash
go test ./cmd/grainfs ./internal/serveruntime -run 'TestServeCmd_RemovesManualECFlags|TestRunCluster_EmptyClusterKey_ReturnsError|TestSeedShardGroupVoters_NonZeroGroupUsesEffectiveECWidth' -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

Run:

```bash
git add cmd/grainfs/serve.go cmd/grainfs/serve_config.go cmd/grainfs/serve_cluster_key_test.go internal/serveruntime/config.go internal/serveruntime/run.go
git commit -m "refactor: remove manual ec serve flags"
```

## Task 3: Store Object Index EC Metadata From Placement Group Width

**Files:**
- Modify: `internal/cluster/cluster_coordinator.go`
- Modify: `internal/cluster/cluster_coordinator_test.go`

- [ ] **Step 1: Add the failing regression test**

In `internal/cluster/cluster_coordinator_test.go`, add this test near the existing object-index tests:

```go
func TestClusterCoordinator_CommitObjectIndexUsesPlacementGroupECProfile(t *testing.T) {
	proposer := &recordingObjectIndexProposer{}
	c := NewClusterCoordinator(nil, nil, nil, nil, "edge-node").
		WithObjectIndexProposer(proposer)
	c.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})

	obj := &storage.Object{
		Key:          "k",
		Size:         5,
		ContentType:  "text/plain",
		ETag:         "etag",
		LastModified: 123,
		VersionID:    "v1",
	}
	group := ShardGroupEntry{
		ID:      "group-7",
		PeerIDs: []string{"n1", "n2", "n3", "n4", "n5"},
	}

	require.NoError(t, c.commitObjectIndex(context.Background(), "bucket", "k", obj, group, false))
	require.Len(t, proposer.entries, 1)
	require.Equal(t, uint8(3), proposer.entries[0].ECData)
	require.Equal(t, uint8(2), proposer.entries[0].ECParity)
	require.Equal(t, []string{"n1", "n2", "n3", "n4", "n5"}, proposer.entries[0].NodeIDs)
}
```

This uses the existing `recordingObjectIndexProposer` helper in the same file.

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/cluster -run TestClusterCoordinator_CommitObjectIndexUsesPlacementGroupECProfile -count=1
```

Expected: FAIL because current code records coordinator-local `1+0`.

- [ ] **Step 3: Add a helper for group-derived object index profile**

In `internal/cluster/cluster_coordinator.go`, add this helper near `commitObjectIndex`:

```go
func objectIndexECConfigForGroup(group ShardGroupEntry) ECConfig {
	return AutoECConfigForClusterSize(len(group.PeerIDs))
}
```

- [ ] **Step 4: Use the helper in `commitObjectIndex`**

Replace the `ECData`/`ECParity` lines in `commitObjectIndex`:

```go
			ECData:           uint8(c.ecConfig.DataShards),
			ECParity:         uint8(c.ecConfig.ParityShards),
```

with:

```go
			ECData:           uint8(objectIndexECConfigForGroup(group).DataShards),
			ECParity:         uint8(objectIndexECConfigForGroup(group).ParityShards),
```

If you prefer avoiding duplicate helper calls, assign first:

```go
	cfg := objectIndexECConfigForGroup(group)
	entry := ObjectIndexEntry{
		// existing fields...
		ECData:   uint8(cfg.DataShards),
		ECParity: uint8(cfg.ParityShards),
		// existing fields...
	}
```

- [ ] **Step 5: Run focused coordinator tests**

Run:

```bash
go test ./internal/cluster -run 'TestClusterCoordinator_CommitObjectIndexUsesPlacementGroupECProfile|TestClusterCoordinator_ListObjects_UsesObjectIndexAcrossPlacementGroups|TestClusterCoordinator_DeleteObjectVersion_RemovesObjectIndex' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add internal/cluster/cluster_coordinator.go internal/cluster/cluster_coordinator_test.go
git commit -m "fix: record object index ec profile from placement group"
```

## Task 4: Remove EC Knobs From Shared E2E Harness

**Files:**
- Modify: `tests/e2e/cluster_harness_test.go`

- [ ] **Step 1: Add failing harness validation tests**

Add these tests after `normalizeE2EClusterOptions` in `tests/e2e/cluster_harness_test.go`:

```go
func TestNormalizeE2EClusterOptionsRejectsRemovedECFlags(t *testing.T) {
	for _, arg := range []string{"--ec-data=2", "--ec-data", "--ec-parity=1", "--ec-parity"} {
		t.Run(arg, func(t *testing.T) {
			require.PanicsWithValue(t,
				fmt.Sprintf("removed EC profile flag %q: use Nodes to select the zero-config profile", arg),
				func() {
					normalizeE2EClusterOptions(e2eClusterOptions{ExtraArgs: []string{arg}})
				})
		})
	}
}

func TestNormalizeE2EClusterOptionsAllowsNonECExtraArgs(t *testing.T) {
	opts := normalizeE2EClusterOptions(e2eClusterOptions{
		ExtraArgs: []string{"--vlog-warn-ratio=0.001"},
	})
	require.Equal(t, []string{"--vlog-warn-ratio=0.001"}, opts.ExtraArgs)
}
```

`fmt` and `require` are already imported in this file.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
go test ./tests/e2e -run 'TestNormalizeE2EClusterOptionsRejectsRemovedECFlags|TestNormalizeE2EClusterOptionsAllowsNonECExtraArgs' -count=1
```

Expected: FAIL because the guard does not exist.

- [ ] **Step 3: Remove EC fields from harness structs**

In `e2eClusterOptions`, delete:

```go
	ECData        int
	ECParity      int
```

In `e2eCluster`, delete:

```go
	ecData        int
	ecParity      int
```

In `tryStartE2ECluster`, delete:

```go
			ecData:        opts.ECData,
			ecParity:      opts.ECParity,
```

- [ ] **Step 4: Add the ExtraArgs guard**

In `tests/e2e/cluster_harness_test.go`, add:

```go
func rejectRemovedECExtraArgs(args []string) {
	for _, arg := range args {
		if arg == "--ec-data" || strings.HasPrefix(arg, "--ec-data=") ||
			arg == "--ec-parity" || strings.HasPrefix(arg, "--ec-parity=") {
			panic(fmt.Sprintf("removed EC profile flag %q: use Nodes to select the zero-config profile", arg))
		}
	}
}
```

Call it near the start of `normalizeE2EClusterOptions`:

```go
func normalizeE2EClusterOptions(opts e2eClusterOptions) e2eClusterOptions {
	rejectRemovedECExtraArgs(opts.ExtraArgs)
	if opts.Nodes == 0 {
		opts.Nodes = 2
	}
	// existing defaults...
}
```

- [ ] **Step 5: Remove EC arg appending**

Delete this block from `startNode` in `tests/e2e/cluster_harness_test.go`:

```go
	if c.ecData > 0 || c.ecParity > 0 {
		args = append(args,
			"--ec-data", fmt.Sprintf("%d", c.ecData),
			"--ec-parity", fmt.Sprintf("%d", c.ecParity),
		)
	}
```

- [ ] **Step 6: Run focused harness tests**

Run:

```bash
go test ./tests/e2e -run 'TestNormalizeE2EClusterOptionsRejectsRemovedECFlags|TestNormalizeE2EClusterOptionsAllowsNonECExtraArgs' -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add tests/e2e/cluster_harness_test.go
git commit -m "test: enforce zero-config ec in e2e harness"
```

## Task 5: Update E2E Tests To Use Node Count Instead Of EC Flags

**Files:**
- Modify: `tests/e2e/cluster_ec_test.go`
- Modify: `tests/e2e/degraded_test.go`
- Modify: `tests/e2e/cluster_scrubber_test.go`
- Modify: `tests/e2e/ec_shardcache_eval_test.go`
- Modify: `tests/e2e/ec_scrub_trigger_e2e_test.go`
- Modify: `tests/e2e/cluster_incident_test.go`
- Modify: `tests/e2e/heal_receipt_api_test.go`
- Modify: `tests/e2e/vlog_watcher_e2e_test.go`
- Modify: `tests/e2e/multiraft_sharding_test.go`

- [ ] **Step 1: Run compile check and capture current failures**

Run:

```bash
go test ./tests/e2e -run TestDoesNotExist -count=1
```

Expected: FAIL to compile because removed harness fields are still referenced by tests.

- [ ] **Step 2: Replace harness `ECData/ECParity` options with node counts**

Apply these mechanical replacements:

```text
e2eClusterOptions{Nodes: 1, ECData: 1, ECParity: 0, ...}
  -> e2eClusterOptions{Nodes: 1, ...}

e2eClusterOptions{Nodes: 2, ECData: 1, ECParity: 1, ...}
  -> e2eClusterOptions{Nodes: 2, ...}

e2eClusterOptions{Nodes: 3, ECData: 2, ECParity: 1, ...}
  -> e2eClusterOptions{Nodes: 3, ...}
```

For any test that used `ECData: 3, ECParity: 2`, set `Nodes: 5` if it needs `3+2`.

For any test that used `ECData: 4, ECParity: 2`, set `Nodes: 6` if it needs `4+2`.

- [ ] **Step 3: Remove direct `--ec-data/--ec-parity` args**

Delete all explicit EC args from `exec.Command(... "serve", ...)` calls:

```go
fmt.Sprintf("--ec-data=%d", ecData),
fmt.Sprintf("--ec-parity=%d", ecParity),
```

and:

```go
"--ec-data", "2",
"--ec-parity", "1",
```

and:

```go
"--ec-data", "0",
"--ec-parity", "0",
```

If a test still logs `ecData+ecParity`, replace that expression with the node-count-derived shard width it actually exercises. Example:

```go
t.Logf("topology test: leader node %d at %s (N=%d, auto EC width=%d)", leaderIdx, httpURL(leaderIdx), numNodes, cluster.AutoECConfigForClusterSize(numNodes).NumShards())
```

Add `github.com/gritive/GrainFS/internal/cluster` only if the file does not already import it.

- [ ] **Step 4: Run e2e package compile check**

Run:

```bash
go test ./tests/e2e -run TestDoesNotExist -count=1
```

Expected: PASS with "testing: warning: no tests to run" or equivalent. No compile errors.

- [ ] **Step 5: Run a small representative e2e subset**

Run:

```bash
go test ./tests/e2e -run 'TestE2E_VlogWatcher_MetricsLive|TestE2E_ECScrubTrigger' -count=1
```

Expected: PASS or SKIP only for environment-specific reasons already used by the suite. Any `unknown flag: --ec-data` failure means Step 3 missed a runnable argument.

- [ ] **Step 6: Commit**

Run:

```bash
git add tests/e2e
git commit -m "test: use node count for e2e ec profiles"
```

## Task 6: Remove EC Flags From Benchmarks

**Files:**
- Modify: `benchmarks/bench_cluster.sh`
- Modify: `benchmarks/bench_nbd_cluster_profile.sh`
- Modify: `benchmarks/bench_nfs_cluster_profile.sh`
- Modify: `benchmarks/bench_iceberg_table_cluster.sh`
- Modify: `benchmarks/bench_topology_get_profile.sh`

- [ ] **Step 1: Run scan and verify benchmark scripts still contain runnable EC flags**

Run:

```bash
rg -n -- '--ec-data|--ec-parity|EC_DATA|EC_PARITY' benchmarks
```

Expected: matches in the benchmark scripts listed above.

- [ ] **Step 2: Remove hard-coded EC serve args**

In `benchmarks/bench_cluster.sh`, `benchmarks/bench_nbd_cluster_profile.sh`, `benchmarks/bench_nfs_cluster_profile.sh`, and `benchmarks/bench_iceberg_table_cluster.sh`, delete:

```bash
    --ec-data 2 \
    --ec-parity 1 \
```

- [ ] **Step 3: Remove `EC_DATA` and `EC_PARITY` knobs from topology benchmark**

In `benchmarks/bench_topology_get_profile.sh`, delete:

```bash
EC_DATA="${EC_DATA:-}"
EC_PARITY="${EC_PARITY:-}"
```

Delete this block:

```bash
    if [[ -n "$EC_DATA" || -n "$EC_PARITY" ]]; then
      if [[ -z "$EC_DATA" || -z "$EC_PARITY" ]]; then
        echo "[error] EC_DATA and EC_PARITY must be set together" >&2
        exit 1
      fi
      args+=(--ec-data "$EC_DATA" --ec-parity "$EC_PARITY")
    fi
```

Replace the summary line:

```bash
  echo "  ec     : target ${EC_DATA}+${EC_PARITY} (effective scales by assigned group voters)"
```

with:

```bash
  echo "  ec     : auto profile from NODE_COUNT / assigned group voters"
```

- [ ] **Step 4: Syntax-check changed scripts**

Run:

```bash
bash -n benchmarks/bench_cluster.sh
bash -n benchmarks/bench_nbd_cluster_profile.sh
bash -n benchmarks/bench_nfs_cluster_profile.sh
bash -n benchmarks/bench_iceberg_table_cluster.sh
bash -n benchmarks/bench_topology_get_profile.sh
```

Expected: no output and exit code 0.

- [ ] **Step 5: Verify runnable benchmark EC flags are gone**

Run:

```bash
rg -n -- '--ec-data|--ec-parity|EC_DATA|EC_PARITY' benchmarks
```

Expected: no matches.

- [ ] **Step 6: Commit**

Run:

```bash
git add benchmarks/bench_cluster.sh benchmarks/bench_nbd_cluster_profile.sh benchmarks/bench_nfs_cluster_profile.sh benchmarks/bench_iceberg_table_cluster.sh benchmarks/bench_topology_get_profile.sh
git commit -m "bench: use zero-config ec profiles"
```

## Task 7: Update README And Repository Scan Gate

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Confirm README still documents removed flags**

Run:

```bash
rg -n -- '--ec-data|--ec-parity' README.md
```

Expected: matches in the CLI option list.

- [ ] **Step 2: Remove EC flags from the README CLI option list**

Delete the README lines for:

```text
--ec-data
--ec-parity
```

- [ ] **Step 3: Add the auto EC profile table**

Add this section near the cluster startup documentation in `README.md`:

```markdown
### Automatic EC profiles

GrainFS selects the erasure-coding profile from the object write-node count.
Normal operation does not require `k/m` tuning.

| Nodes | Profile | Notes |
| --- | --- | --- |
| 1 | 1+0 | Single-node EC path, no redundancy |
| 2 | 1+1 | Transitional redundancy |
| 3 | 2+1 | Minimum production topology |
| 4 | 2+2 | Two-failure-domain redundancy |
| 5 | 3+2 | Parity fixed at 2 |
| 6 | 4+2 | Parity fixed at 2 |
| 7 | 5+2 | Parity fixed at 2 |
| 8+ | 6+2 | Data shard fan-out capped at 6 |

Three nodes is the minimum production topology. One- and two-node clusters are
valid bootstrap or transition states, not production HA guidance.
```

- [ ] **Step 4: Run repository scan for removed public EC args**

Run:

```bash
rg -n -- '--ec-data|--ec-parity|EC_DATA|EC_PARITY|ECExplicit|ECData|ECParity|DefaultDataShards|DefaultParityShards' README.md benchmarks tests/e2e cmd/grainfs internal/serveruntime internal/cluster/ec.go
```

Expected: no matches for public CLI/config remnants. Acceptable remaining matches are internal object metadata fields under `internal/cluster` outside `ec.go`, because those are explicitly out of scope.

- [ ] **Step 5: Commit**

Run:

```bash
git add README.md
git commit -m "docs: document automatic ec profiles"
```

## Task 8: Full Verification And Performance Gate

**Files:**
- No code edits unless verification exposes a missed reference.

- [ ] **Step 1: Run unit test verification**

Run:

```bash
go test ./cmd/grainfs ./internal/serveruntime ./internal/cluster -count=1
```

Expected: PASS.

- [ ] **Step 2: Run e2e compile verification**

Run:

```bash
go test ./tests/e2e -run TestDoesNotExist -count=1
```

Expected: PASS compile with no runnable tests.

- [ ] **Step 3: Build the binary**

Run:

```bash
make build
```

Expected: `bin/grainfs` is rebuilt successfully.

- [ ] **Step 4: Verify CLI help no longer exposes EC flags**

Run:

```bash
./bin/grainfs serve --help | rg -- '--ec-data|--ec-parity'
```

Expected: command exits 1 with no output.

- [ ] **Step 5: Run repository scan**

Run:

```bash
rg -n -- '--ec-data|--ec-parity|EC_DATA|EC_PARITY' README.md benchmarks tests/e2e cmd/grainfs
```

Expected: no matches.

- [ ] **Step 6: Run the 8-node 6+2 benchmark gate**

Run the new zero-config benchmark:

```bash
NODE_COUNT=8 SIZE_KB=65536 RANGE_BYTES=65536 RANGE_MODE=random VUS=8 DURATION=30s ./benchmarks/bench_topology_get_profile.sh
```

Expected:

- Benchmark starts without `--ec-data`/`--ec-parity`.
- Output reports auto EC profile.
- Record PUT and Range GET p95/p99.
- Compare against latest 6-node `4+2` baseline from `TODOS.md`:
  - prior range reference: p50 16.75ms, p95 34.14ms, p99 50.21ms for VUS=8 small range after stream half-close fix.
  - prior 1MiB full-width reference: p50 31.53ms, p95 117.85ms, p99 148.72ms after buffer pooling.

If 8-node `6+2` p95 or p99 is worse by more than 25% versus the matching baseline workload, stop and report the regression instead of shipping.

- [ ] **Step 7: Commit any verification-only doc updates**

If you recorded benchmark numbers in `TODOS.md` or `CHANGELOG.md`, commit them:

```bash
git add TODOS.md CHANGELOG.md
git commit -m "docs: record zero-config ec benchmark results"
```

If no files changed, skip this commit.

## NOT In Scope

- Do not remove `ECData`/`ECParity` fields from object metadata structs, FlatBuffers, object index records, repair, scrub, or placement resolver code.
- Do not remove `SetECConfig`; internal tests still use it to exercise low-level EC behavior.
- Do not add a hidden expert EC tuning path.
- Do not implement dynamic join voter expansion.
- Do not migrate or reshard existing objects as part of this change.

## Parallelization Strategy

| Step | Modules touched | Depends on |
| --- | --- | --- |
| EC policy | `internal/cluster` | — |
| CLI/runtime removal | `cmd/grainfs`, `internal/serveruntime` | EC policy |
| Object index metadata | `internal/cluster` | EC policy |
| E2E harness/tests | `tests/e2e` | CLI/runtime removal |
| Benchmarks/docs | `benchmarks`, `README.md` | CLI/runtime removal |
| Verification/perf gate | all touched modules | all implementation tasks |

Parallel lanes:

- Lane A: Task 1 -> Task 3, shared `internal/cluster`.
- Lane B: Task 2, `cmd/grainfs` + `internal/serveruntime`.
- Lane C: Task 4 -> Task 5, `tests/e2e`, starts after Task 2.
- Lane D: Task 6 -> Task 7, `benchmarks` + `README.md`, starts after Task 2.
- Final lane: Task 8 after A+B+C+D merge.

Launch Task 1 and Task 2 sequentially first because Task 2 references constants removed by Task 1. After Task 2, Tasks 3, 4, and 6 can run in parallel in separate worktrees if desired. Merge those, then run Task 5 and Task 7, then Task 8.

Conflict flags:

- Task 1 and Task 3 both touch `internal/cluster`; keep them sequential or coordinate carefully.
- Task 4 and Task 5 both touch `tests/e2e`; keep them sequential.

## Implementation Notes

- `group-derived EC profile` is the invariant: never use coordinator-local `ecConfig` for object index metadata when a selected placement group is available.
- Any test needing a profile should choose node count. Example: `Nodes: 5` means auto `3+2`; `Nodes: 6` means auto `4+2`.
- `go test ./tests/e2e -run TestDoesNotExist -count=1` is a compile check only. It should not start a cluster.
- If a historical changelog mentions `--ec-data`, leave it alone unless the final scan includes that file. The required scan intentionally excludes `CHANGELOG.md`.

