# Zero Config EC Profile Design

## Goal

Make erasure-coding profile selection fully zero-config. Operators should not
choose `k` or `m` for normal cluster startup; GrainFS derives the effective EC
profile from cluster size.

## Background

`grainfs serve` currently exposes `--ec-data` and `--ec-parity`. That makes the
first-run cluster experience ask operators to understand EC math before they can
run a durable cluster. It also creates an unnecessary mismatch between product
intent and tests: zero-config startup should mean node count determines the
profile.

The minimum production topology remains three nodes. One- and two-node clusters
are valid bootstrap or transition states, not production HA guidance.

## Auto Profile Policy

`AutoECConfigForClusterSize` is the only source of truth for zero-config object
storage profiles:

| Cluster nodes | Effective profile | Intent |
| --- | --- | --- |
| 1 | 1+0 | Single-node EC path, no redundancy |
| 2 | 1+1 | Transitional redundancy |
| 3 | 2+1 | Minimum production topology |
| 4 | 2+2 | Two-failure-domain redundancy |
| 5 | 3+2 | Better efficiency, parity fixed at 2 |
| 6 | 4+2 | Better efficiency, parity fixed at 2 |
| 7 | 5+2 | Better efficiency, parity fixed at 2 |
| 8+ | 6+2 | Cap data shard fan-out at 6 |

Parity stays capped at two for the zero-config default. That covers one failure
plus another failure during recovery, which is the right default for small and
medium clusters. Higher parity profiles can be reconsidered later as an expert
or archival policy, but they are not part of the normal startup surface.

## CLI And Config Surface

Remove `--ec-data` and `--ec-parity` from `grainfs serve`.

Remove the corresponding serve config fields:

- `ECData`
- `ECParity`
- `ECExplicit`

Server startup always derives the effective profile from the object write-node
set. In the current static peer startup path that is:

```text
effectiveEC = AutoECConfigForClusterSize(1 + len(peers))
```

If no active profile exists for the cluster size, startup fails. With the policy
above, that only means an invalid zero-node calculation or an internal wiring
bug.

Do not use an operator-provided EC profile as a substitute for missing data
group voter expansion. If dynamic join has not yet made a node part of an object
write group, that node must not widen the group's EC profile. Dynamic voter
expansion remains a separate lifecycle problem.

Object index metadata must record the EC profile derived from the selected
placement group's write-node set, not from the coordinator process's local peer
view. This keeps any-node writes safe when an edge node routes a PUT to a data
group whose voter set is wider than the edge node's local `peers` list.

Startup logs and status output should show the effective profile, not a
configured profile:

```text
ec_mode=auto
cluster_size=5
effective_k=3
effective_m=2
```

## Constants

Rename EC constants away from CLI defaults. The public meaning should be the
auto policy, not user-provided defaults.

Suggested names:

- `MaxAutoDataShards = 6`
- `AutoParityShards = 2`

The exact names can follow local Go style, but they should make it clear that
these values are part of automatic profile selection.

## Tests And Benchmarks

Tests and benchmarks must stop passing EC profile flags to `grainfs serve`.

Tests that need a specific public profile should express that through node
count:

- `1+0` tests run with one node.
- `1+1` tests run with two nodes.
- `2+1` tests run with three nodes.
- `4+2` tests run with six nodes.

Internal unit or integration tests may still call `SetECConfig` directly when
they need to validate low-level behavior independent of CLI startup.

Benchmark scripts should remove `EC_DATA` and `EC_PARITY` startup arguments.
When a benchmark requires a specific EC width, it should set `NODE_COUNT`
instead.

## Compatibility

This is a pre-release product simplification. Existing local scripts that pass
`--ec-data` or `--ec-parity` will fail until updated. That is intentional: the
public CLI should not preserve manual EC tuning before the first stable release.

Object metadata still records `ECData` and `ECParity` per object. Removing the
CLI flags does not remove per-object profile metadata; reads, repair, scrub, and
reshard code still need the stored profile to interpret existing objects.

## Success Criteria

- `grainfs serve --help` no longer shows `--ec-data` or `--ec-parity`.
- Server startup always uses `AutoECConfigForClusterSize`.
- Object index writes store `ECData` and `ECParity` derived from the selected
  placement group's voter count.
- The auto profile table above is covered by unit tests.
- E2E harnesses and benchmark scripts no longer pass removed EC flags.
- README documents automatic EC selection and the three-node production
  minimum.

## Out Of Scope

- Removing EC metadata fields from object records.
- Removing internal `SetECConfig` test hooks.
- Adding expert/manual EC tuning through another config path.
- Changing dynamic-join voter expansion behavior.
- Implementing migration or resharding policy changes beyond the new effective
  profile for newly written objects.
