# D-Cut Node-Key KEK Generation Slice

> **For agentic workers:** implement task-by-task. Keep the scope to node identity sealing; do not remove `encryption.key` or add prune-gate evidence in this PR.

**Goal:** Move `keys.d/node.key.enc` steady-state sealing from static `encryption.key` back to explicit KEK generation sealing, using `keys.d/node.key.gen` as the durable generation pointer. This is Slice B from `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`.

**Why now:** Slice A removed new invite bootstrap static-key delivery and PR #621 started persisting the Phase-1 node-key generation sidecar. The next blocker is that normal boot and invite Phase-2 still migrate node identities to static `encryption.key`, which keeps static key material on the production identity path and hides the generation needed for prune safety.

**Non-goals:**
- Do not remove `Config.RawEncryptionKey`, `LoadOrCreateEncryptionKeyWithRaw`, or `<dataDir>/encryption.key`.
- Do not change bootstrap payload wire format again.
- Do not add prune-refusal evidence; that is Slice C.
- Do not change presentation/accept-set cutover behavior.

## Task 1: Normal Node Identity Uses Active KEK Generation

**Files:**
- Modify: `internal/serveruntime/node_identity_boot.go`
- Modify: `internal/serveruntime/node_identity_boot_test.go`

- [x] **Step 1: Write failing tests**

Pin the new steady-state contract:

- When `node.key.enc` is absent and a KEK store is wired, `ensureNodeIdentity` seals the generated identity under `kekStore.ActiveKEK()` and writes canonical `keys.d/node.key.gen` equal to `kekStore.ActiveVersion()`.
- When `node.key.enc` is already sealed under the active KEK and `node.key.gen` matches, `ensureNodeIdentity` reloads it without touching static `encryption.key`.
- When `node.key.enc` is sealed under an older retained KEK generation and `node.key.gen` records that generation, `ensureNodeIdentity` loads via the recorded generation, re-seals under the active KEK, and rewrites `node.key.gen` to the active generation.
- When `node.key.gen` is missing, malformed, or points at a pruned/unknown generation for a KEK-sealed key, `ensureNodeIdentity` errors and never regenerates or overwrites `node.key.enc`.
- Legacy static-key-sealed `node.key.enc` still loads and migrates once to the active KEK when a KEK store exists.

Verify:

```bash
go test ./internal/serveruntime -run 'TestEnsureNodeIdentity' -count=1
```

- [x] **Step 2: Implement KEK-generation load/reseal**

Change `ensureNodeIdentity`/`reloadNodeIdentity` so a wired `kekStore` owns the steady-state:

- require a non-empty KEK store for production KEK-gen mode;
- use active KEK generation for new seals;
- read `node.key.gen` before opening KEK-sealed identities;
- re-seal to active KEK if the recorded generation is not active;
- write `node.key.gen` after every successful KEK seal/reseal;
- keep static-key fallback only as a legacy migration path, immediately re-sealing to active KEK and writing the sidecar.

Do not delete the sidecar in the KEK-gen steady-state path.

## Task 2: Invite Phase-2 Stops Migrating Node Key To Static Key

**Files:**
- Modify: `internal/serveruntime/invite_join_boot.go`
- Modify: `internal/serveruntime/invite_join_boot_test.go`
- Modify: `internal/serveruntime/boot_phases_transport.go` if post-drop early load needs the same helper.

- [x] **Step 1: Write failing tests**

Pin invite behavior:

- `loadAndMigrateInviteNodeKey` loads using the recorded/resolved Phase-1 KEK generation and, when the active KEK differs, re-seals under the active KEK generation.
- Successful invite Phase-2 leaves `node.key.gen` present and updated to the active generation, not deleted.
- An encKey-sealed legacy node key is migrated to active KEK when the KEK store is available.
- A missing/malformed sidecar or pruned recorded generation fails without overwriting `node.key.enc`.

Verify:

```bash
go test ./internal/serveruntime -run 'TestLoadAndMigrateInviteNodeKey|TestNodeKeyGen' -count=1
```

- [x] **Step 2: Implement active-KEK invite close-out**

Replace the static-key migration helper contract with a KEK-generation helper:

- resolve the recorded Phase-1 generation from the invite state/sentinel/sidecar;
- load `node.key.enc` under that KEK;
- re-seal under `kekStore.ActiveKEK()` when needed;
- write the active `node.key.gen`;
- keep legacy encKey fallback only for old static-key-sealed identities.

Update comments so they no longer claim Phase-2 guarantees static-key sealing.

## Task 3: Boot Integration And Focused Verification

**Files:**
- Modify: `internal/serveruntime/boot_phases_raft.go`
- Modify tests only as needed.

- [x] **Step 1: Wire callers to the new helper contract**

Update callers so normal boot and invite Phase-2 pass enough information to select active KEK generation without broad boot-order changes.

- [x] **Step 2: Focused tests**

Run:

```bash
go test ./internal/serveruntime -run 'TestEnsureNodeIdentity|TestLoadAndMigrateInviteNodeKey|TestNodeKeyGen|TestInviteNodeKeySealKey|TestStageInviteSecrets|TestGateInviteJoin|TestPostDropInvite' -count=1
go test ./internal/cluster -run 'Test.*Bootstrap|Test.*Invite|Test.*Join|Test.*KEK' -count=1
```

## Task 4: Documentation And Ship

**Files:**
- Modify: `TODOS.md`
- Let `/ship` update `VERSION` and `CHANGELOG.md`.

- [x] **Step 1: Update TODO context**

Record Slice B as in progress/shipped under the D-cut node-key bullet. Keep prune evidence and final static-key removal open.

- [x] **Step 2: Unit suite**

Run:

```bash
make test-unit
```

- [ ] **Step 3: Ship**

Use `/ship` to merge base, run checks, bump version/changelog, push, and open a PR. Stop at the open PR.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Plan/advisor review | feature-pipeline | Slice B changes boot identity sealing and invite Phase-2 close-out | 1 | constrained | External advisor subagent could not be spawned because the agent thread limit was reached. Local gate review narrowed the slice to active-KEK node-key sealing only and explicitly deferred prune evidence and static-key removal. Key risk found and addressed: `node.key.gen == 0` can mean either valid KEK gen 0 or legacy post-drop static marker, so loaders now prefer KEK gen 0 but migrate static-key-sealed legacy identities when static decrypt succeeds. |
| Focused verification | feature-pipeline | Boot and invite paths are security-sensitive | 1 | pass | `go test ./internal/serveruntime -run 'TestEnsureNodeIdentity|TestLoadAndMigrateInviteNodeKey|TestNodeKeyGen|TestInviteNodeKeySealKey|TestStageInviteSecrets|TestGateInviteJoin|TestPostDropInvite|TestApplyPostDrop|TestLoadPostDrop' -count=1`, `go test ./internal/serveruntime -count=1`, and `go test ./internal/cluster -run 'Test.*Bootstrap|Test.*Invite|Test.*Join|Test.*KEK' -count=1` passed. |
