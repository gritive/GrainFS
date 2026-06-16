# TODO

## Follow-ups

- **[EPIC] Retroactive EC-redundancy upgrade for objects written before the cluster grew.** Objects
  written while a cluster was genuinely single-node (genesis, no `--bootstrap-expect-nodes`) land in
  a single-peer (1+0) group; the placement-redundancy gate prevents NEW non-redundant writes once
  ≥2 nodes register and `ensureGenZero` self-heals routing for future writes, but the
  already-written objects' data stays on one node — a single-node loss after growth still loses
  them. The common multi-node patterns (form-then-write, `--bootstrap-expect-nodes`) are fully
  protected by the gate; this gap is the "start single-node then grow" path only. A clean fix is a
  **data-plane relocation / EC-upgrade-on-growth** sweep: detect objects whose placement group is
  non-redundant while the cluster has redundancy capacity, re-encode their data into a wide EC group
  preserving object identity (ETag/version), atomically swap the placement metadata, and GC the old
  shards. Note: `ClassifyObjectLayout` mis-classifies a 1-peer-group object as `LayoutCurrent` (it
  compares against the object's own group's desired EC, not cluster capacity), and the EC-upgrade
  *writer* (`ConvertObjectToEC`/`upgradeObjectEC`/reshard) was removed in #780 — so this needs that
  machinery rebuilt with relocate-to-a-different-group semantics. A re-PUT through the object plane
  is NOT a clean relocation (it changes the version/ETag). Sizable epic; tracked separately.
