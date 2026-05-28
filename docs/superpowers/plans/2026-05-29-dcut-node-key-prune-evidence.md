# D-Cut Node-Key KEK Prune Evidence Slice

> **For agentic workers:** keep this PR to Slice C from the D-cut bootstrap-envelope design. Do not remove `encryption.key` and do not bump the at-rest format boundary here.

**Goal:** publish each voter's `node.key.enc` KEK generation through the existing peer registry/self-register path and refuse KEK prune while any current voter still depends on the prune target.

**Source:** `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`, Slice C.

## Task 1: Extend Peer Registry Evidence

**Files:**
- Modify: `internal/cluster/clusterpb/cluster.fbs`
- Modify generated `internal/cluster/clusterpb/*` via `make fbs`
- Modify: `internal/cluster/peer_registry*.go`
- Modify: `internal/cluster/meta_fsm_snapshot.go`
- Modify tests under `internal/cluster`

- [x] Add `node_key_kek_gen:uint32` to `PeerEntry` and `MetaRegisterMemberCmd`.
- [x] Thread it through encode/decode, apply, snapshot, restore, and registry state.
- [x] Make updates monotone so an older self-register cannot regress a newer recorded node-key generation.

## Task 2: Thread Boot Evidence

**Files:**
- Modify: `internal/serveruntime/node_identity_boot.go`
- Modify: `internal/serveruntime/invite_join_boot.go`
- Modify: `internal/serveruntime/boot_state.go`
- Modify: `internal/serveruntime/boot_phases_self_register.go`
- Modify: `internal/serveruntime/boot_phases_raft.go`
- Modify tests under `internal/serveruntime`

- [x] Return the generation used by `ensureNodeIdentity` and `loadAndMigrateInviteNodeKey`.
- [x] Store it in `bootState`.
- [x] Include it in boot-time self-register and present-flip re-register.

## Task 3: KEK Prune Refusal

**Files:**
- Modify: `internal/cluster/kek_rotation_leader.go`
- Modify: `internal/cluster/meta_fsm_kek_apply.go`
- Modify tests under `internal/cluster`

- [x] Leader pre-check refuses prune if any current voter lacks evidence or has `node_key_kek_gen <= prune_version`.
- [x] FSM apply re-check performs the same deterministic guard against the stamped voter set.
- [x] Add tests for missing voter evidence, stale generation, and success after all voters report a surviving generation.

## Task 4: Verification And Ship

- [x] Run focused cluster/serveruntime tests.
- [x] Run `make test-unit`.
- [ ] Let `/ship` bump `VERSION`/`CHANGELOG.md`, commit, push, and open PR.

## GSTACK REVIEW REPORT

| Review | Trigger | Status | Findings |
| --- | --- | --- | --- |
| Plan/advisor review | feature-pipeline | scoped | The existing peer registry already carries per-node presentation evidence, so this slice should extend that path rather than introduce a new side channel. Prune refusal is applied in both leader pre-check and FSM apply for defense in depth. |
