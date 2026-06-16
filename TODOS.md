# TODO

## Follow-ups

- **Retroactive EC-redundancy upgrade for NON-LATEST versions in versioning-enabled buckets.** The
  background EC-redundancy-upgrade sweep (v0.0.608.0) relocates non-redundant (1+0) genesis objects
  into a redundant EC group, but is scoped to the **latest** version of each key: `CmdPutObjectMeta`
  CAS and the quorum-meta record both key on the latest, so relocating an old version would move the
  "latest" pointer backward and clobber the latest's placement. Genesis-single-node clusters almost
  always have versioning disabled (every object is its own latest/null version → fully covered), so
  this is a narrow gap: old `1+0` *non-latest* versions in a versioning-enabled cluster that started
  single-node. A clean fix needs a version-scoped placement swap that never touches the latest
  pointer (per-version quorum-meta key + per-version CAS). Lower priority than the latest-version
  path this epic shipped.
