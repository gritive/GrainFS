# Dynamic Join E2E Cleanup Design

## Goal

Make dynamic join the default multi-node e2e cluster path, remove ad hoc join test scaffolding, and run the same user-facing service checks across meaningful node counts.

## Background

The e2e suite currently has two cluster harnesses:

- `startMRCluster` in `tests/e2e/multiraft_sharding_test.go`, which starts static `--peers` clusters.
- `startDynamicJoinCluster` in `tests/e2e/cluster_join_e2e_test.go`, which starts a seed node and then joins additional nodes with `--join`.

This split hides important behavior differences. Dynamic join nodes first become edge/control-plane nodes; until PR-E2 lands, they do not automatically become voters for every data group. Tests that validate public service behavior should run on dynamic clusters now. Tests that require full static voter topology must be isolated as legacy/static coverage or deferred until PR-E2.

## Design

Create one shared e2e harness in `tests/e2e/cluster_harness_test.go`.

The harness exposes:

- `startE2ECluster(t, opts)` as the default dynamic-join cluster starter.
- `ClusterModeDynamicJoin` as the default mode.
- `ClusterModeStaticPeers` only for explicitly legacy/static tests.
- Accessors for S3 clients, HTTP URLs, NFS ports, NBD ports, raft addresses, data directories, and process logs.

Dynamic mode starts node 0 as seed with `--raft-addr` and no `--peers`, waits for its HTTP port, then starts nodes 1..N with `--join <seed-raft-addr>`. Static mode preserves the existing all-peers startup behavior but moves it behind a clearly named option.

## Test Matrix

The suite should express tests by behavior and node count:

- `N=1`: singleton S3 smoke and default bucket startup.
- `N=2`: joined edge node can create buckets, put/get/head/list objects, and expose S3, Iceberg REST, NFS, and NBD on every node.
- `N=3`: any-node S3/Iceberg metadata behavior under a quorum-capable dynamic cluster.
- `N=5`: service and metadata consistency across more joined nodes. Data-group voter expansion remains out of scope until PR-E2.

Tests that require precomputed static voter membership, EC topology, or existing performance baselines stay on `ClusterModeStaticPeers` for now and must say so in their name or helper call.

## Static Peer Policy

Do not delete static peer product support in this cleanup. Static peer removal is a CLI and compatibility decision and should be a separate change. This cleanup only removes static peer usage as the default e2e path.

Static e2e coverage is retained for:

- Upgrade/backward-compatibility confidence.
- Tests that still depend on static data-group voter placement.
- Performance/scale benchmarks that are not yet meaningful under dynamic join.

## Migration Plan

Move `dynamicJoinCluster` out of `cluster_join_e2e_test.go` into the shared harness and replace it with `e2eCluster`.

Convert the new join service tests to use the shared harness:

- `TestE2E_JoinedNodeEdgeForwardsBeforeDataReady`
- `TestE2E_AllServicesAvailableOnJoinedNodes`
- `TestE2E_DefaultBucketOnlySeedCreates`

Then convert existing `startMRCluster` consumers in stages:

1. Service-level tests that should work through any node.
2. Iceberg any-node API tests.
3. Bucket assignment and cross-node dispatch tests where dynamic mode is sufficient.

Leave EC, scrubber, degraded, scale, and performance tests on static mode until PR-E2 provides dynamic voter expansion.

## Success Criteria

- There is one shared cluster harness for dynamic and static e2e startup.
- Dynamic join is the default for new multi-node e2e tests.
- Static peer usage is explicit and easy to grep.
- The same joined-node service checks can run for `N=2` and `N=3` without copy-pasted startup code.
- Existing verified tests still pass:
  - `go test ./internal/cluster ./cmd/grainfs`
  - Dynamic join service e2e tests
  - Static all-node services e2e
  - Singleton smoke/default bucket e2e

## Out Of Scope

- Removing static peer CLI support.
- Implementing PR-E2 data-group voter integration.
- Converting EC/performance/scale tests to dynamic join before PR-E2.
- Changing public API behavior beyond test harness cleanup.
