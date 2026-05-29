# TODOs

## Current Priority Lens: Operator Trust

Design principle: safe defaults, explicit escape hatches, observable operations.

Operators should see recovery, drain, rebalance, and upgrade state without
guessing from logs. Keep measured deferred work in this file. Promote an item to
active work only after reading the code/API surface and writing a specific
engineering plan.

Planning reference: operator trust roadmap note from 2026-05-15.

## Now

Work these in order. Do not run them in parallel.

- [ ] **At-rest unification — R3 static-key retirement (last slice; cleanup, not migration)**
   - At-rest is **greenfield** — each format-changing slice bumps the on-disk format
     version and an older dir loud-fails on a newer binary (no in-place re-encrypt,
     no legacy ciphertext to support). Current format = **8**.
   - LANDED (reconciled 2026-05-29 against origin/master): the functional KEK→DEK
     migration is **complete** — every production at-rest consumer now seals through
     the `DataEncryptor`/DEK seam:
       - R1 (boot gate + logical-WAL/packblob/PUT-pipeline→DEK; `encryption.rotate-dek`
         gated) — PR #596 v0.0.393.0
       - R2 (IAM SA + BucketUpstream secrets→DEK, `DomainIAMCredential`, two-pass
         restore) — PR #605 v0.0.401.0
       - R-FSM (data-group FSM value sealing + data WAL→DEK) — PR #608
       - cluster-config secrets→DEK — PR #611 v0.0.408.0
       - static at-rest **boot-glue** D-cut (RawEncryptionKey / `--encryption-key-file` /
         LoadOrCreateEncryptionKeyWithRaw removed; node identity sealed under KEK) —
         PR #631 v0.0.428.0 + #619/#623/#627/#629
       - raft log store at-rest encryption (the separate-spec metadata copy; KEK-derived
         `raft-store.key.enc`, not the DEK) — PR #635 v0.0.432.0
       - datawal dek-generation persisted + namespace split — PR #637/#640 v0.0.434.0/.436.0
   - [P1 SECURITY] scrubber-repair plaintext hole — **FIXED 2026-05-29 (this worktree).**
     `DistributedBackend.WriteShard` now seals via `ShardService.EncodeEncryptedShardBuffer`
     (GFSENC3/DEK seam), so scrubber-repaired shards are DEK-encrypted at rest like normally-written
     shards. WriteShard errors when `shardSvc==nil` or path is not under a shard data dir (no
     plaintext fallback). Covered by `TestWriteShardSealsRepairedShardWithDEK` (production-shaped
     DEK backend) + the cluster e2e (`cluster_scrubber_test.go` asserts the repaired shard is
     ciphertext on disk). Follow-up: e2e covers cluster auto-repair only; no single-node scrub
     auto-repair e2e harness exists, so single-node repair-to-ciphertext relies on the unit test —
     add a single-node scrub-repair e2e when a trigger/shard-path helper lands.
   - [ ] **[P2] shard-pack repair shadowing (PRE-EXISTING, orthogonal to AAD-coherence).**
     `readShardIntegrity` prefers `ReadLocalShardFromPack`, but `WriteShard` writes a standalone
     `shard_N` FILE. When a *packed* shard entry is corrupt/stale, repair writes a file that the
     pack entry still shadows on the next read, so the repair does not take effect for pack-resident
     shards. The AAD-coherence slice changed only WHICH key binds the AAD, never the pack-vs-file
     read preference, so it neither introduces nor worsens the shadowing. Fix (its own slice): have
     repair rewrite the pack entry, or invalidate the pack slot before the file write.
   - [ ] **[P2] object-key path traversal in the EC shard write path (PRE-EXISTING).** `getShardPath`
     is `filepath.Join(dataDir, bucket, key, shard_N)`; an object key with enough `..` can resolve a
     shard file outside the shard data root. `getKey` (`server/object_api.go`) does not normalize
     URL-encoded keys, so such a key is reachable as metadata. The scrubber read/write path now has a
     `ShardPathUnderDataDir` containment guard (added with the AAD-coherence fix), but the NORMAL
     `putObjectEC`/`writeLocalShard` write still maps the raw key into the path without a containment
     check. Fix at the S3 key boundary (reject/normalize `..` segments) or add containment to
     `writeLocalShard`. Its own slice — touches the PUT path, not just the scrubber.
   - **Gate-fix slice — DONE 2026-05-29 (ShardService.encryptor retirement, this worktree).**
     The vestigial `ShardService.encryptor != nil` runtime gates are gone: the putPipeline
     dispatch gate (`backend.go`) now keys off an explicit `putPipelineEnabled` bool (boot wires
     it `false` = dormant pending F1; only the integration test enables it), and the WAL
     raw-payload sizing now passes a constant `false`. The static `encryptor` field + `WithEncryptor`
     option were deleted and all ~97 test fixtures migrated to the DEK keeper (full WithEncryptor→DEK
     migration). Behavior-preserving in prod (prod always had `encryptor==nil`).
     - **Still DEFERRED: putPipeline prod REACTIVATION (F1 durability).** putPipeline acks on early
       K-shard quorum (`putpipeline/pipeline.go:240`) + defers fsync to a batched WAL commit
       (`drive.go` `skipFsync`, `commit.go:120`), whereas the spooled path flushes the data WAL
       synchronously before the raft metadata propose. Verify PutShard does not let metadata commit
       before shard durability before enabling in prod. Its own slice.
   - [ ] **R3 static-residue deletion (remaining residue; AFTER any F1 reactivation).** The
     ShardService static `encryptor` + `WithEncryptor` are DONE (above). Remaining dead/legacy
     static residue still to remove in its own slice(s): `storage.EncryptorAdapter`/
     `NewEncryptorAdapter` (still used by `fsm_values.go`, `pitr.go`, `packblob/blob.go`,
     `storage/local.go` — migrate those first), `putpipeline/pipeline.go` `cfg.Encryptor` fallback,
     `NewManagerWithEncryptor` static `enc` param, `MetaFSM.encryptor`/`SetEncryptor`/`Encryptor`
     + `FSM.enc` (still read by `fsm_values.go` `dataEncryptor`/`openValue` fallback — retire after
     those callers move to DEK). NOTE: `storage/encrypted_badger.go` + `storage.LocalBackend` are
     NOT dead (many prod callers — earlier "no production caller" note was wrong; verify before any
     removal). ADR for cipher-unification + greenfield boundary; bumps format 8→9.
   - [ ] **Data-DEK rotation re-enable (separate, larger — keep gated for now).** Re-enable
     the `encryption.rotate-dek` trigger only after **all** ciphertext-bearing formats persist
     a non-zero `dek_gen` (datawal done #637; packblob gen still deferred to format v8+) AND
     all data lanes carry EC-style per-segment gen framing + roll-on-gen-change (today only EC
     has it); also close the EC mid-shard race (`eccodec/shardio.go:168`).
   - Full re-grounded design in the (gitignored) unified-at-rest-key spec
     (`docs/superpowers/specs/2026-05-28-unified-at-rest-key-hierarchy-design.md`) +
     D-cut bootstrap-envelope design (`...at-rest-dcut-bootstrap-envelope-design.md`).
     See [[project-grains-at-rest-two-key-systems]].

- [ ] **BadgerDB atomic auto-recovery design**
   - Trust risk: recoverable Badger state still requires manual intervention
     during an outage.
   - Signal: recovery plan, applied action, skipped action, and reason.
   - Verification: corrupted/partial Badger replay plus snapshot restore tests.
   - Boundary: design next; implement after recovery journal decision structs
     stabilize.

- [ ] **Rolling upgrade remaining safety slices**
   - Trust risk: mixed-version clusters can hide schema, config, or capability
     divergence behind a healthy surface.
   - Signal: compatibility gates, upgrade status, snapshot-config compatibility
     status, warning/safe rejection/degraded state.
   - Verification: `tests/compat/`, mixed-cluster tests, snapshot-config forward
     policy tests.

- [ ] **Object placement/index orphan and stale reconcile**
   - Trust risk: object-level placement creates a dual-write failure mode between
     data groups and the global object index.
   - Signal: reconcile report, repair/quarantine counters, non-silent failure.
   - Verification: index commit failure injection, orphan data, stale index,
     missing shard, corrupt shard tests.

- [ ] **Cluster health data-group Raft progress**
   - Trust risk: metaRaft can look healthy while a data group is leaderless or
     lagging.
   - Signal: per-group leader, term, lag, drain state, and transfer state.
   - Verification: induced lag, leader transfer, multi-group cluster health e2e.

- [ ] **NFSv4 and 9P auth/access-control correctness**
   - Promote only when `GrainFS` must support untrusted networks, multi-tenancy,
     compliance-driven file access audit, or S3 IAM parity for file protocols.
   - Signal: export-level policy, auth mode, denied access events.
   - Verification: cross-protocol auth tests and reserved `__meta/` namespace
     tests.

## Next

- [ ] **External PDP adapter — deferred slices** (Slice 1 SHIPPED: local-unix-socket-only,
  disabled-by-default, chain/deny-override, fail-closed default + opt-in fail-open,
  admin + protocol-credential paths, Prometheus `grainfs_iam_pdp_*` + `iam.pdp` audit).
  Remaining:
    - Remote `https://` transport + bearer token (delivered via admin UDS + a
      `grainfs iam pdp` CLI, DEK-sealed like other IAM secrets) + mTLS + active SSRF
      egress filtering.
    - Decision cache (positive/negative TTL) + grace mode — **SHIPPED Slice 2**
      (`iam.pdp.cache`: ttl_allow/ttl_deny + LRU max_entries + grace_ttl;
      sharded TTL+LRU, stale-preserving lookup, cache cleared on any iam.pdp
      config change, failures never cached, cache-hit audit suppressed).
    - S3/Iceberg data-plane PDP enforcement (needs a `WithPolicyAuthorizer` interface
      seam + latency/cache design — **now builds on the Slice-2 cache/grace**).
    - **Singleflight** for concurrent duplicate cache misses (deferred from Slice 2;
      control-plane tolerates the stampede, the data-plane slice needs it).
    - **Event-driven cache invalidation** (on `iam.pdp` config / policy change) —
      Slice 2 is TTL-only.
    - Full GrainFS-only audit on the protocol-credential control plane (pre-existing
      gap; Slice 1 added only the PDP-outcome `iam.pdp` audit).
    - Admin peercred/UDS path is intentionally NOT PDP-gated (local socket trust).
    - **`target_sa` is empty for bearer/OIDC credential ops.** The decorator only
      sets `context.target_sa` when the actor principal is a service account; a
      bearer actor minting/rotating/revoking a credential for some SA does not
      expose the target SA to the PDP (the handler has `saID` but
      `AuthorizePrincipal` does not receive it). To close: thread the target SA from
      `authorizeProtocolCredential` into the decorator via request context (e.g. a
      `WithActorTarget` ctx value) — expands beyond the decorator-only boundary into
      `handlers_credentials.go`. Documented as a Slice-1 limitation (spec P2a).
  Spec: `docs/superpowers/specs/2026-05-28-oidc-federated-iam-boundary-design.md`
  "External PDP Adapter — Slice 5 Detailed Design".

- [ ] **Zero-CA cutover/revocation follow-ups** (2026-05-29 re-review of the merged
  revocation + complete-cutover slices; zero-CA is greenfield so none of these are
  migration concerns):
    - **[P3] Orphaned join-redirect TODOs (operability)**. A joiner that contacts a
      follower gets `JoinStatusNotLeader` but no leader address to retry against:
      `internal/cluster/meta_join.go:234` `TODO(W7b/W9)` (return
      leader_join_addr/leader_join_spki from member state) and
      `internal/serveruntime/invite_admin.go:96` `TODO(W7b)` (FSM-resolve redirect to
      auto-forward the invite-admin call to the leader). Not a security gap; a
      usability rough edge for multi-node join.
      NOTE: the redirect MUST be "joiner re-dials the leader directly", NOT
      "follower byte-proxies the join stream" — channel binding (RFC 5705
      exporter) is session-local, so a proxy gives joiner and leader different
      exporters and every bound join fails verification.
    - **[P3] Revoke cleans meta-raft voters only, not data-group voters**.
      `MetaRaft.RevokeNode` (`internal/cluster/revoke_node.go:44`) calls
      `m.node.RemoveVoter` on the meta-raft alone; per-data-group raft membership is
      untouched. Full eviction therefore relies on the transport backstop
      (`ClosePeer` drops the live mux + SPKI denylist + cluster-key drop excludes the
      revoked SPKI from the accept-set). Precondition: before the cluster key is
      dropped the base PSK is still accepted, so a revoked node could in principle
      re-dial and resume as a lingering data-group voter. Verify whether revoke
      should also remove the node from data-group voter sets, or document that full
      eviction is only guaranteed post-drop.

- [ ] **Auth redesign §1 Foundation post-ship cleanup** (v0.0.260.0 review-forever
  Pass 1 INFO findings — non-blocking, ship after §2/§3 to keep blast radius small):
    - Maintainability M#2: extract `peelTrailer(data, magic, trailerLen)` helper in
      `meta_fsm.go` (3 structurally identical DKVS/GCFG/IAM peel blocks).
    - Maintainability M#4: rename `encrypt.LoadFromFSM` → `NewDEKKeeperFromVersions`
      (caller-context leaking into callee package name).
    - Maintainability M#6: extract common pattern from `applyConfigPut` /
      `applyConfigDelete` (nil-guard + decode + dispatch).
    - Maintainability M#7: drop the `safe bool` param on `DEKKeeper.Prune` —
      callee should expose `PruneUnchecked` and the FSM guard does its own
      ref-count check before calling.
    - Maintainability M#8: drop the `snapshotMetaFSMToBytes` /
      `restoreMetaFSMFromBytes` one-line wrappers in `config_snapshot_test.go`.
    - Maintainability M#10: register `encryption.rotate-dek` /
      `prune-dek-version` with `OnTrigger: nil` instead of no-op closures.
    - Testing F#11: end-to-end `ProposeConfigPut → Apply → cfgStore.Set → hook`
      test through a real `MetaRaft` (only unit-level apply path covered today).
    - Testing F#12: `applyConfigPut` with a corrupt inner FlatBuffer payload
      (decode-error gate currently untested).
    - Performance P#8: `Snapshot()` acquires 3 separate locks (FSM mu, cfgStore mu,
      DEKKeeper mu) in sequence — document the single-writer guarantee or fold
      into one acquisition if snapshot latency becomes measurable.
    - Tracking: `docs/superpowers/plans/2026-05-19-auth-redesign.md` §10 R-notes
      + §11 decisions log.

- [ ] **§9 Follow-ups** (from F#41/F#41b implementation):
    - **F#42 (Pass-1 MEDIUM-1)**: Phase 0 anon Allow paths emit no audit row
      (`request_authz.go:148` gates recordAllow on `AuthEnabled()`). Phase 2
      anon-to-default IS audited via `AnonAllow` flag. Phase 0 long-lived state
      needs a forensic trail — anon writes to /default during Phase 0 should
      produce at least a single coarse audit row per request. Trade-off: audit
      table volume vs. forensic capability.
    - **F#44 (Pass-1 LOW-2)**: T73 cluster prober runs ~12 samples vs ~11811 on
      single-node (QUIC forward saturates). Parallelize cluster prober (5-10
      goroutines) to lift sample density, OR document the cluster-density gap
      so a future maintainer doesn't assume burst coverage.
      `tests/e2e/phase_transition_test.go:170-201`.
- [ ] **Auth redesign DX follow-ups** (from `docs/superpowers/specs/2026-05-19-auth-redesign.md`
  Codex review, medium+cosmetic tier). All single-PR-sized, ship after the main
  redesign lands:
    - [ ] `grainfs iceberg secret duckdb --warehouse X --sa Y` and `grainfs
      iceberg catalog spark|trino --warehouse X --sa Y` — emit copy-paste config
      blocks per client. Reduces per-warehouse-token UX tax.
    - [ ] `grainfs iam jwt-key rotate --auto-prune-after-ttl` — scheduler-friendly
      one-shot rotation that runs prune after the TTL window elapses. Optional
      systemd timer / k8s CronJob example in operator docs.
    - [ ] `grainfs doctor auth --warehouse X --sa Y` — health check that walks
      the entire auth path (TLS reachability, socket access, SA exists, policies
      attached, JWT key state, bucket exists, client URL base sane) and prints
      the first broken link.
    - [ ] `docs/users/minio-muscle-memory-map.md` — `mc admin policy info` →
      `grainfs iam policy get`, `mc admin user add` → `grainfs iam sa create`,
      `aws s3api put-bucket-policy` → `grainfs iam bucket policy put` (admin
      UDS), `mc console` → (no equivalent yet). Per-row "why different" column.
    - [ ] Policy template library at `docs/users/iam-policy-templates/`:
      `readonly-bucket.json`, `readwrite-bucket.json`, `write-prefix.json`,
      `read-prefix-from-cidr.json`, `iceberg-readwrite-warehouse.json`. Each
      template is a starting JSON with comments + a sed-friendly bucket name
      placeholder. Wired into `grainfs iam policy create --from-template <name>
      --bucket <b>`.
    - [ ] Adjust spec/docs language: "AWS IAM JSON subset" everywhere instead of
      "AWS-IAM-compatible." Banner block in `docs/users/iam-policy-from-aws.md`
      enumerating the unsupported constructs (`NotAction`, policy variables,
      most condition keys, inline policies, account principals).
    - [ ] Built-in policy attach warning surface (covered in spec, but TODO is
      the docs sentence — explain in `docs/users/iam-policy-from-aws.md` why
      `readwrite` is "global-readwrite" semantically and link to the per-bucket
      template.
    - [ ] `grainfs doctor snapshot` — preflight check that operators run on an
      old binary before installing a new one. Walks the meta-FSM snapshot,
      reports schema version + compatibility with a target binary version.
      Mitigates the "fail-loud cluster won't start" surprise for private
      cluster evaluators.
    - [ ] `docs/operators/containerization.md` — exact chmod/chown/socket-dir
      behavior for admin UDS under rootless containers, systemd units, mounted
      sockets, sidecars, CI users. Failure messages spelled out so operators
      can grep them.
    - [ ] Error-message SA-ID exposure decision — keep current "SA <id> lacks
      ..." or replace external surface with `access_key_suffix` (last 4 chars),
      moving full SA id to audit log only. Needs explicit ADR.
    - [ ] External Iceberg client first-tier-only docs — keep warp + DuckDB as
      tested examples; demote Trino/Spark/PyIceberg/Flink to "configuration
      pattern, not e2e verified" until each gets its own e2e cell. Avoids stale
      docs claiming compatibility we haven't measured.
- [ ] **NFS `rdattr_error` required gap**: implement READDIR per-entry
  attribute error semantics.
- [ ] **pynfs/nfstest conformance matrix**: publish nightly/basic suite results
  as an operator-readable pass/fail matrix.
- [ ] **Hot reload drift detection**: detect disk/runtime config mismatch after
  config reload.
- [ ] **Migration mirror/cutover correctness**: settle mirror, cutover, and
  status semantics before dashboard polish.
- [ ] **Bucket/object-lock/retention governance design**: promote only after the
  semantic mapping is clear for version-level retention records,
  governance/compliance delete and overwrite behavior, legal hold permissions,
  lifecycle expiration interaction, and NFS retention attribute alignment.
- [ ] **Scrub dedup 영구성 정책**: 현재 `scrubber.Director.dedup`은 영구.
  운영 의도 확인 후 done/cancel 시 cleanup 명령 추가 (ADR 또는
  grill-with-docs). 참조: `docs/architecture/scrubber-director-actor.md`
  결정 5.
- [ ] **공통 JobActor 추상화 검토**: `scrubber`/`lifecycle`/`migration`
  worker가 공유 가능한 lifecycle 패턴. design doc의 JobActor 컨셉 참조.
- [ ] **Scrub Register boot phase constructor 옵션화**: `BlockSource`/`Verifier`를
  `NewDirector` 옵션으로 주입해 `Register`/`started.Bool` 가드 제거.
  boot phase 의존 그래프 재배치 동반.
- [ ] **Cluster local pre-check audit (post-SetBucketVersioning fix)**:
  `DistributedBackend`에 `b.HeadBucket`/`b.HeadObject` 후 `b.propose`
  패턴이 15곳 남아 있다 (`SetObjectACL`, multipart paths, etc.).
  follower가 meta-Raft bucket assignment를 받았지만 data-Raft
  `CmdCreateBucket`을 아직 apply하지 않은 짧은 윈도우에서
  같은 false-`NoSuchBucket`을 반환한다.
  `ClusterCoordinator` 단에 cluster-aware pre-check + base에
  `*ProposeOnly` entrypoint 패턴을 일관 적용. 참조 커밋:
  `fix(s3auth/cluster): warp versioned workload passes on a 4-node
  cluster` (`benchmark` branch).
- [ ] **Capability evidence warmup wiring**: `/v1/cluster/capabilities` now
  exposes gate evidence on the admin UDS, so the original ready-probe gap is
  closed. Remaining work is to replace fixed benchmark/operator sleeps
  (`CLUSTER_WARMUP_SLEEP=45` in `docs/reference/benchmarks.md`) with polling
  against that endpoint, then re-check whether gossip latency still needs
  interval tuning or raft-committed evidence.

## Deferred Until Triggered

- [ ] **KEK-envelope C-prune-followup: `SegmentRef.dek_gen` done right + with consumer**.
  Deferred from the D-seg-ec-activate slice (v0.0.368.0). Recording the sealing DEK
  generation in segment metadata was cut because the only cheap source
  (`keeper.Active()` at segment-write time) is not guaranteed to equal the actual
  per-shard seal gen (gen-pinning is per-shard-stream; rotation-mid-write / remote-node
  differences), so a recorded value could be silently wrong — a footgun for a prune
  consumer that trusts it to drop DEK generations (wrong-low → prune a still-referenced
  DEK → unreadable data). Authoritative per-shard gen already lives in the GFSENC3 header.
  When reopened: thread the REAL seal gen out of the shard write path through
  `storage.SegmentRef` + `storagepb.SegmentRef` + `clusterpb.SegmentRef` +
  `clusterpb.SegmentMetaEntry` + PutObjectMeta/CompleteMultipart codecs +
  `segmentMetaEntriesToRefs`, AND build the prune consumer that reads it (cross-checking
  the GFSENC3 header gen). Bundle both so the recorded value is correct before anything trusts it.
- [ ] **KEK-envelope: write-path `ErrDEKGenUnknown` → retriable 503**. On the EC-shard
  PUT path, `cpupool.go` → `commit.go` collapses a per-shard `encrypt.ErrDEKGenUnknown`
  (gen not yet local) into a generic "K shards unreachable" error → likely a 500 on S3
  PUT. `WaitDEKReady` (run.go:227) gates serving so a normal write never hits an empty
  keeper, making this unreachable on a serving node today. Reopen as a hardening pass:
  detect `errors.Is(err, encrypt.ErrDEKGenUnknown)` in the commit coordinator and map it
  to a retriable 503 (not 500). The READ side already classifies it as transient (slice C).
- [ ] **InstallSnapshot Restore failure should fatal-halt, not log-and-advance**.
  `meta_raft.go` apply-loop `LogEntrySnapshot` case logs a `Restore` error then
  advances `lastApplied` to the entry index regardless. A joiner that receives an
  InstallSnapshot it cannot open (now reachable via D-snap envelope-open failure:
  unknown KEK version, wrong cluster, or corruption) continues with un-restored FSM
  state but an advanced applied index → silent divergence. Pre-existing pattern;
  D-snap adds a new crypto failure mode to it. Surfaced by /review adversarial pass
  (2026-05-28). Fix: treat envelope-open / Restore failure on InstallSnapshot as a
  fatal halt (mirror the existing `ErrFSMKEKFatal` path) rather than log-and-advance.
- [ ] **Phase D-snap D-cut (object)**: remove `snapshot.openSnapshotBlob` legacy
  plaintext passthrough and add a boot-time scan that refuses startup if any
  plaintext snapshot file remains. The `grainfs_snapshot_legacy_plaintext_reads_total`
  counter is a runtime signal, not sufficient alone. Mirrors meta-FSM D-cut.
- [ ] **KEK prune-refusal: absolute closure of the in-flight snapshot-write window [P3]**.
   The prune guard scans retained `.json.zst` + in-flight `.json.zst.tmp` and uses the
   APPLIED raft index for attestation freshness, which closes the race to a
   sub-millisecond in-memory-seal window (a `Create()` that captured the retiring KEK
   version but has not yet written its `.tmp` while the same node has already applied
   retire). For absolute closure, `snapshot.Manager.Create` could acquire a short
   `KEKLeaseTracker` lease on the sealed version across seal+rename, so in-flight writes
   surface as `lease_count > 0`. Very low priority — the current window is practically
   unreachable.
- [ ] **KEK-envelope: cluster e2e join + snapshot-restore object reads**. The
   D-seg-ec-activate e2e added rotate-survives + follower-read-no-quarantine (both green
   on a live 3-node cluster). Join-after-bootstrap and snapshot-restore-boot object-read
   specs were skipped because the e2e harness has no dynamic `AddNode` (4th node post-
  bootstrap) and no snapshot-restore-boot helper (it has `KillNode`/`RestartNode` only).
  Reopen: add an `AddNode`/post-bootstrap join helper + a snapshot-restore-boot helper to
  `tests/e2e/cluster_harness_test.go`, then add the two additive specs under the "KEK
  rotation lifecycle" Describe.
- **KEK-envelope: DataEncryptor `SealTo`/`OpenTo` buffer-reusing seam methods** — Seal half landed.
  - **Slice 1 (Seal side) — DONE 2026-05-29 (ship-pending PR).** Added `encrypt.AppendAAD`,
    `encrypt.DEKKeeper.SealWithAADTo` (both byte-identical wrappers; `BuildAAD`/`SealWithAAD`
    re-expressed as `...To(nil,...)`), `storage.DataEncryptor.SealTo` (+ EncryptorAdapter via the
    existing `SealValueAADTo`, DEKKeeperAdapter, TransientDataEncryptor→ErrTransientReadOnly) with a
    pooled AAD scratch (`seamAADPool`/`withSeamAAD`, closures verified non-escaping via `-gcflags=-m`).
    Migrated the **packblob `Append`** consumer (reintroduced `blobAppendSealedPool`, seals via
    `SealTo`). Bench-gated `BenchmarkAppendEncrypted` (64KiB): **B/op 75,400→1,497 (~50×)**, allocs/op
    14→9 (12 under -race), ~42→35µs/op. Alloc bound 15→13.
  - **Residual / follow-up [P3]:** the remaining 9 allocs/op are the **`AADField` construction** in
    `blobEntryAADFields` (`FieldUint64`/`FieldString`/`FieldUint16` each `make` a per-field data slice +
    the `[]AADField` slice). Eliminating them needs an `encrypt.AADField` append/pool variant
    (`AppendField...`-style) — touches every AAD builder, its own slice. Only then does Append drop
    below ~5 allocs/op. (Pre-#572's "5-alloc" baseline used a different positional-AAD scheme.)
  - **Remaining Seal/Open consumers (each its own slice; Open side needs lifetime analysis — Open
    plaintext escapes to callers, so pooling the `OpenTo` dst is a use-after-free hazard, NOT a
    mechanical pool reintroduction):**
    - packblob `Read` — Open side; needs `OpenTo` + lifetime analysis.
    - **spool Open (the #645 regression below)** — Open side; the #645 micro-bench regression is
      Open-side, so Slice 1 did NOT fix it. Add `OpenTo` + reintroduce per-record buffer reuse in
      `spool.go`'s `encryptedSpoolRecordWriter`/`Reader`.
    - ec / local (`eccodec/shardio.go`) Seal then Open — hot read path.
    - datawal (`scanRecords`) Seal then Open.
  - **#645 spool regression data (Open-side, still open):** `BenchmarkEncryptedSpoolWrite` 72→97
    allocs/op (1.04→8.1 MiB B/op); `BenchmarkEncryptedSpoolOpen` 64→87 allocs/op (2.0→16.1 MiB B/op);
    Open sec/op +~35% (1.72–1.88→2.37–2.43 ms/op). Write throughput flat (AEAD+IO-bound). Fix in the
    spool Open slice above. Each Open-side slice introduces `OpenTo` only with its first
    hazard-analyzed consumer (do not freeze `OpenTo` early). Bench ≥15s×3 (allocs/op AND B/op) per slice.
- [ ] **KEK-envelope D-wal: live DEK rotation segment rollover [P1]**.
  D-wal-data now opens production writer/recovery paths with `DEKKeeperAdapter`
  and new encrypted `internal/storage/datawal` segments probe-seal before header
  write so `dek_gen` records the actual active generation. Rotation is still
  deferred because a currently-open segment pins one generation in its header
  while `DEKKeeperAdapter.Seal` always uses the live active generation. Before
  enabling `encryption.rotate-dek`, add a rotation boundary: either roll/close
  active data WAL segments on DEK rotation or add a seal-under-specific-generation
  API so appends keep using the header-pinned generation until rollover.
  **Legacy-WAL caveat (codex, D-wal-legacy code gate):** `internal/storage/wal`
  writes via the async `AppendAsync` path — `lastSeq` advances at submit time and
  the background goroutine logs and DROPS on seal error. Apply the same
  probe-before-header rule plus a non-dropping rotation boundary there before
  moving legacy PITR WAL to a real gen>0 DEK sealer.
- [ ] **packblob `Compact` active-blob concurrency hardening [P2]**. Pre-existing race
  (surfaced by codex during D-seg-pack review, not introduced by it): `Compact` reads the
  source blob without `bs.mu` (`internal/storage/packblob/blob.go` ~440), only later locks
  to rotate if compacting the active blob (~522), then `os.Remove`s the old file (~542). A
  concurrent `Append` can land in the blob after the read pass but before rotation/removal,
  then `Compact` unlinks it. Reopen: forbid compacting the active blob, or lock+rotate
  before scanning. Add a concurrent Append-during-Compact regression test.
- [ ] **S3 Range GET residual p95/p99**: reopen when product/SLO requires p95
  below 25 ms and p99 below 35 ms, or when FUSE/s3fs/goofys random reads
  reproduce EC read amplification.
- [ ] **S3 Range GET 1 MiB full-width bottleneck**: reopen only with new pprof
  evidence for shard-side encrypted range decrypt or QUIC syscall cost.
- [ ] **NBD direct-write bottleneck**: reopen when direct I/O write SLOs matter
  or trace-off profiles confirm per-block `open/pwrite` as the p95 bottleneck.
- [ ] **NBD qemu/libnbd interop**: promote `OPT_INFO`/`OPT_GO`,
  `NBD_INFO_BLOCK_SIZE`, structured replies, block status, and extended headers
  only after a disposable qemu/libnbd smoke harness runs against a real GrainFS
  NBD port.
- [ ] **Iceberg REST high-concurrency Raft ceiling**: reopen when production
  catalog workloads miss SLOs or the consistency spec for reducing proposals is
  clear.
- [x] **Iceberg `/v1/config` secret over plaintext HTTP**: fixed by making
  the HTTP handler publish caller S3 credential overrides only for HTTPS
  requests. Plain HTTP still returns catalog defaults and `s3.endpoint`, but
  omits `s3.access-key-id`, `s3.secret-access-key`, and
  `s3.path-style-access`.
- [ ] **Iceberg Spark/Trino/PyIceberg client coverage**: promote only after
  real-client REST Catalog smoke tests define which client behaviors are
  supported versus DuckDB-only compatibility.
- [ ] **Volume CLI follow-ups**: export/import, policy, attach/detach, and rename
  need concrete server-side lifecycle requirements.
- [ ] **Scrub scope, EC scrub race, group dir cleanup, PeerHealth threshold**:
  reopen when their telemetry triggers fire.
- [ ] **Incident store scope index / `ScanObjects(bucket, keyPrefix)`**: reopen
  when measured margins fail or a concrete caller needs prefix scope.
- [ ] **Protocol credential remaining real-client smoke coverage**: protocol
  credential data-plane enforcement is wired for S3, Iceberg, NBD, NFS, and 9P
  server paths. S3 now has MinIO `mc` single-node and cluster real-client smoke
  coverage; Iceberg now has DuckDB REST Catalog single-node real-client smoke
  coverage. Follow up by adding Iceberg cluster real-client smoke once the
  static MR cluster fixture is stable, plus NBD/qemu-libnbd and NFS/9P
  credential mount smoke coverage before promoting broader compatibility claims.

## NFSv4 RFC 8881 Follow-Ups

### Required

- [ ] [nfs-audit] bit 11 `rdattr_error` [P0] [Skipped]: READDIR does not emit
  per-entry attribute errors; operation-level errors only. Owner: TBD.

### Recommended

- [ ] [nfs-audit] bit 12 `acl` [P2] [Skipped]: implement NFS ACL payloads if a
  policy requires them. Owner: TBD.
- [ ] [nfs-audit] bits 16/17 case flags [P2] [Skipped]: return
  case-sensitive/case-preserving booleans. Owner: TBD.
- [ ] [nfs-audit] bits 21/22/23 inode-style file counts [P2] [Skipped]: add only
  after defining synthetic capacity policy. Owner: TBD.
- [ ] [nfs-audit] bit 24 `fs_locations` [P2] [Skipped]: referrals and
  `NFS4ERR_MOVED` need a design. Owner: TBD.
- [ ] [nfs-audit] bits 36/37 owner/group [P1] [Partial]: design idmap or
  authenticated user mapping. Owner: TBD.
- [ ] [nfs-audit] bits 38/39/40 quota [P2] [Skipped]: design NFS quota
  accounting before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 42/43/44 filesystem capacity [P2] [Skipped]: select a
  capacity source before surfacing values. Owner: TBD.
- [ ] [nfs-audit] bit 48 `time_access_set` [P1] [Partial]: persist atime in the
  sidecar schema. Owner: TBD.
- [ ] [nfs-audit] bits 56/57 directory notifications [P2] [Skipped]: design
  notification behavior before implementation. Owner: TBD.
- [ ] [nfs-audit] bits 58/59 DACL/SACL [P2] [Skipped]: requires the ACL/audit
  model. Owner: TBD.
- [ ] [nfs-audit] bits 62-68 pNFS [P2] [Skipped]: out of scope unless `GrainFS`
  adds pNFS support. Owner: TBD.
- [ ] [nfs-audit] bits 69-73 retention metadata [P2] [Skipped]: map only after
  object-lock semantics are defined. Owner: TBD.
- [ ] [nfs-audit] bit 74 masked mode SETATTR [P2] [Skipped]: implement if
  client workloads require it. Owner: TBD.
- [ ] [nfs-audit] bit 76 charset capability flags [P2] [Skipped]: add after UTF-8
  policy is explicit. Owner: TBD.

## Chunking / Large-Object Follow-Ups

- [ ] **Cluster pull-through large-object parity [P1]**: `TestPullthroughE2E/
  Cluster4Node/LargeObject` (5 MiB random payload) returns bytes that differ
  from the upstream payload after the cache-miss GET; the cache-hit GET
  exhibits the same divergence. SingleNode passes the identical case. The
  symptom looks like truncation or a partial 2-pass streaming write to the
  cluster's local cache before the response is served. Test was originally
  single-only; promoting the e2e to TestBucketsE2E dual surfaced this gap as
  a now-failing Cluster4Node subtest — the failing assertion is the
  regression signal that unblocks closing the gap. Fix candidates: trace the
  pull-through 2-pass streaming write on cluster (where in EC distribute the
  body is consumed) and ensure the local cache write completes before the
  HTTP response body is closed.

- [ ] **PITR WAL replay carries segment metadata [P1]**: snapshot/restore now
  knows `Object.Segments`, but `storage/wal.Entry` still only carries scalar
  object metadata (`Bucket`, `Key`, `ETag`, `ContentType`, `Size`, `VersionID`).
  WAL-only PITR replay can therefore recreate a segment-backed object record
  without the segment refs and later report it stale or unreadable. Add a WAL
  format version that encodes segment refs, preserve old replay compatibility,
  and add PITR coverage for a chunked object created after the snapshot point.

- [ ] **Placement monitor: repair a corrupt LOCAL shard from peers instead of quarantining [P2]**:
  `scanRecord` now correctly classifies confirmed corruption (CRC/structural/AEAD) and
  still quarantines the **whole parent object** at object granularity — the on-corruption
  semantics were intentionally left unchanged in the transient-classification PR. But a
  single corrupt local shard is usually recoverable: with 4+2 EC and enough surviving
  peers, reconstruct the bad shard in place (same path the missing-shard repair already
  uses) and only quarantine if reconstruction fails. This downgrades most corruption
  events from object isolation to a silent local repair. (`internal/cluster/shard_placement_monitor.go`.)

- [ ] **Placement monitor: stream scan targets instead of buffering O(objects+segments) [P3]**:
  `Scan` buffers all `ECShardScanTarget`s before processing; ~1.5 GB peak for 1 M chunked
  objects × 10 segments each. Follow up if production scans show RAM pressure; streaming
  would hold the FSM read transaction open during local shard checks (its own tradeoff).

- [ ] **Placement monitor: scan non-latest object versions' segment/coalesced shards [P3]**:
  The placement monitor currently covers only the latest object version
  (`IterObjectMetas` semantics). Segment and coalesced EC shards belonging to
  non-latest versions are not proactively scanned; they rely on read-time EC
  reconstruction. Extend the monitor to iterate non-latest versions' segment refs
  as well, closing the between-boot gap for versioned objects with degraded
  older-version shards.

- [ ] **AppendObject real per-call MD5 chain [P2]**: `AppendObject` still stores
  segment xxhash checksums in `AppendCallMD5s` as a stopgap. Capture each append
  call payload's real MD5 at the API/storage boundary and persist that digest in
  both single-node and cluster apply paths so composite append ETags are S3-wire
  correct while segment checksums remain xxhash3 for internal integrity.

## AppendObject Follow-Ups

- [ ] **Forward buffer 512 MiB warp calibration [P1]**: production traffic
  pattern에 default `--cluster-append-forward-buffer-total-bytes` 512 MiB가
  적정한지 검증. `warp append --concurrent 32 --duration 60s --obj.size '1-16MiB'`
  실행, `grainfs_cluster_append_forward_buffer_rejected_total` ratio < 1%
  확인. 충족 못 하면 default 상향 또는 memory-budget tuning을 separate PR로
  분리. PR description에 measurement result 또는 미실행 사유 명시.

- [ ] **EC shard orphan cleanup [P2]**: PR #425 (raw segment side)에서 미해결.
  coalesce 도중 EC 쓰기 후 propose 실패 시 남는 EC shard dir
  (`<shardRoot>/<bucket>/<userKey>/coalesced/<id>/coalesced/<id>/shard_<i>`)
  cleanup. 기존 `OrphanWalkable.WalkOrphanShards` 인터페이스는 plain EC만
  cover — `ScanObjects`가 `lat:` 인덱스로 coalesced shardKey를 yield하지 않아
  known set 구축이 안 됨. Storage layout (shardRoot vs data root) + coalesced
  shard tracking mechanism 조사 → `OrphanWalkable` 확장 또는 별도 sweep
  메커니즘 도입.

- [ ] **Coalesce recoalesce depth audit [P2]**: design open question — 새
  raw segment가 다시 threshold 도달 시 또 coalesce하면 `coalesced[]`에 entry가
  계속 누적된다. `MaxCoalescedEntries=1024` cap 외에 measurement-driven 정책
  (max depth, periodic 통합) 검토. v0.0.253.0 Hardening이 추가한
  `AppendCoalescedEntriesAtCap` counter로 production cap-reach 빈도 baseline
  수집 후 판단.

- [ ] **`AwaitWriteFromNonOwner` harness EC-aware 강화 [P2]**: 현재
  AwaitWriteFromNonOwner는 healthy-cluster path 전용 — EC stripe width ==
  cluster size 환경에서 owner kill 후 호출하면 모든 PUT이 ServiceUnavailable.
  PR #424 T24 OwnerKillSurvives는 직접 `/api/cluster/status` `leader_id`
  폴링으로 우회. EC degraded write을 인식하는 helper 또는 별도 RotationSettled
  helper 분리 검토.

## Pre-existing Test Failures (Phase B3 무관)

- [ ] **Cluster4Node HeadObject가 expired object에 `MethodNotAllowed` 반환 [P1]**:
  `tests/e2e/lifecycle_expiration_test.go:111-118` TagFilter spec에서 expire 후
  HeadObject가 S3 표준 `NotFound` 대신 `MethodNotAllowed`를 반환. 기존 testify
  `assert.Equal` soft-fail이 가렸던 동작 — Ginkgo gomega.Expect는 hard fail이라
  노출됨. 현재 Or(NotFound, MethodNotAllowed) matcher로 마스킹. 원인 후보:
  `internal/cluster/backend.go` `deleteObjectWithMarker` 가 non-versioned bucket
  expiration 후에도 delete-marker semantics 적용. S3-parity 위해 NotFound 일관화
  필요.

- [ ] **Cluster tests create QUIC transports without closing them [P2]**:
  ~60 call sites in `internal/cluster/*_test.go` create
  `transport.MustNewQUICTransport(...)` but never `Close()` it, so quic-go's
  `Transport.listen` / `sendQueue.Run` / `Conn.run` goroutines live until process
  exit. They accumulate across the cluster test binary and are the goroutines that
  `ec_fix_integration_test.go`'s process-global `goleak.VerifyNone` was catching
  (now scoped via `goleak.IgnoreCurrent` baseline). Hygiene cleanup: add a
  `newTestTransport(t)` helper that registers `t.Cleanup(tr.Close)` and migrate the
  call sites. Mechanical but large; not required for correctness.

- [ ] **`TestBlobStoreAppendNoCompressKeepsAllocationBound` race-mode fail [P2]**:
  baseline에서도 동일하게 fail (allocations=4 vs ≤1 expected). `-race` 빌드에서
  추가 alloc churn. packblob 패키지 별도 작업.

- [ ] **Reverify full `make test-e2e` baseline [P1]**: old snapshot listed
  backup_restic, multipart (`capability multipart_listing_v1 rejected`),
  cluster_incident, quarantine_incident, dynamic_join_services,
  cluster_scrubber, no_peers, encryption_at_rest, iam_scoped_key. Since the
  capability evidence endpoint now exists, rerun the baseline and split true
  remaining failures from stale bootstrap/warmup artifacts.

## Conformance Follow-Ups

- [ ] [nfs-conformance] pynfs-nightly [P1]: run pynfs basic suite on a scheduled
  Linux/Colima host and review `results/summary.json`.
- [ ] [nfs-conformance] nfstest-runner [P2]: add nfstest after pynfs stabilizes.
## Storage And Volume Backlog

- [ ] **Volume pool quota operator surface**: `volume.ManagerOptions.PoolQuota`
  and planner/unit coverage exist; expose/configure the cross-volume physical
  capacity pool through the server/CLI path before treating it as an operator
  feature.
- [ ] **Multi-tenancy**: account/namespace isolation above IAM.
- [ ] **Quota**: capacity limits per service account or team.
- [ ] **Volume export/import**: backup/restore for volume data.
- [ ] **Volume policy**: per-volume pool quota, encryption key, and EC
  profile overrides.
- [ ] **Volume attach/detach**: runtime NBD exposure toggles.
- [ ] **Volume rename**: migrate volume block key prefixes.

> Volume dedup, snapshot, clone, rollback, and copy-on-write were removed in
> v0.0.346.0 (plain block device). Items above that previously assumed those
> features are descoped accordingly and depend on the eventual volume redesign.
- [ ] **`__grainfs_volumes` EC policy**: revisit forced replication for volume
  blocks after EC scrub and migration strategy are ready.

## Migration Backlog

- [ ] NFS virtual overlay.
- [ ] NBD block proxying.
- [ ] Bucket-level server-side injection:
  - Phase 1 credentials shipped in v0.0.123.0.
  - Phase 2 import hook shipped in v0.0.171.0.
  - Remaining work: mirror mode, cutover verb, progress tracking, upstream
    `List`/`Head`/`CopyObject`, CLI integration, cancellation on leadership loss,
    and counter continuity after leader flip.

## Parked

- [ ] **Redesign disaster-recovery surface.** `grainfs recover` and `grainfs doctor` were removed in v0.0.343.0 (shipped partial/misleading). Before reintroducing: (1) define failure domains explicitly (rack/AZ/region); (2) name commands by what they actually do (`recover metadata` vs `recover data`, never bare `recover cluster`); (3) `doctor` must do real integrity checks, not directory-existence stubs; (4) RPO + data-vs-metadata semantics must be in help text. Until then the `recover` verb stays unbound.
- [ ] Redis protocol.
- [ ] TSDB.
- [ ] 9P/NFS shared write-back layer.
- [ ] Blame Mode v2.
- [ ] PagerDuty native webhook mapping.
- [ ] go-billy Direct File I/O / O_DIRECT.
- [ ] Hot/cold auto tiering.
- [ ] I/O-based auto rebalancing.
- [ ] io_uring.
- [ ] SPDK.
- [ ] SIMD.
- [ ] SoA layout.
- [ ] Control-plane/data-plane split.
- [ ] fix(storage/packblob): extend versioning bypass to Suspended state (currently only Enabled bypasses fast path; Suspended buckets on single-node still pack-write under (bucket,key) without versionId="null"). Add e2e cases for Suspended → PUT/DELETE/HEAD by versionId.
- [ ] feat(scrubber): multi-node/multi-group segment GC fan-out. Orphan-segment GC currently (Plan 3.5) activates only on group-0's distBackend AND, in a cluster, runs only on the raft leader (CaughtUp uses node.ReadIndex → followers get ErrNotLeader → fail-closed skip). Result: single-node is complete; in a multi-node cluster, segments on non-leader nodes' local disks and in non-group-0 data-groups are never reclaimed → latent disk growth. Proper design needs leader-coordinated (or per-node-with-freshness-barrier) deletion across all groups — mirror the EC scrub ecResolver fan-out (boot_phases_scrubber.go) and decide who deletes follower-local raw segments. SegmentOrphanLog already namespaces by groupID. Blocked-by: Plan 3.5 (object-segment-gc-activation) land.

## Completed

- [x] **KEK-prune-refusal for object snapshots [P1]**
  - `grainfs encrypt kek prune` now refuses if any voter has a retained object
    snapshot (`<data>/snapshots/snapshot-*.json.zst`) sealed under the target
    version. The error names the node and count. Implemented via per-voter
    `SnapshotRefCount` attestation (`snapshot.CountSnapshotsSealedUnderKEK`),
    leader refusal in `ProposeKEKPrune`, and FSM apply re-check.
  - **Completed:** vNEXT (2026-05-28)

- [x] **§9 Follow-up: tighten L1 anonymous allow to default bucket scope**
  - Removed the global `iam.anon-enabled` anonymous bypass. Anonymous S3 access
    now flows through default-bucket implicit policy or explicit bucket policy.
  - **Completed:** v0.0.375.0 (2026-05-28)
