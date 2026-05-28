# Invite Node-Key Generation Sidecar Slice

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make zero-CA invite Phase-1 persist explicit evidence of the KEK generation used to seal `keys.d/node.key.enc`, without removing `encryption.key` from the bootstrap payload yet.

**Architecture:** The first attempted static-key removal slice was not independently shippable: `RunFromOptions` still requires `<dataDir>/encryption.key`, KEK prune lacks `node_key_kek_gen` evidence, and a strict `node.key.gen` loader would not be crash-safe. This smaller slice keeps the current static-key bootstrap and Phase-2 static-key migration intact, but adds the durable sidecar needed by the later KEK-gen identity slice. Because Phase-2 still migrates `node.key.enc` to the static key and deletes the sidecar, this PR does not introduce a new prune-brick window.

**Tech Stack:** Go, zero-CA invite join, serveruntime boot phases, KEKStore.

---

### Task 1: Persist Invite Phase-1 Node-Key KEK Generation

**Files:**
- Modify: `internal/serveruntime/invite_join_boot.go`
- Modify: `internal/serveruntime/invite_join_boot_test.go`

- [x] **Step 1: Write failing tests**

Add tests that pin:

- `writeNodeKeyGen` writes `keys.d/node.key.gen` in a parseable format.
- `readNodeKeyGen` rejects absent, malformed, and non-canonical sidecars.
- Phase-1 sealing persists the selected highest KEK generation before the resume barrier can be considered complete.

Verify:

```bash
go test ./internal/serveruntime -run 'TestNodeKeyGen|TestInviteNodeKeySealKey' -count=1
```

Expected before implementation: new helper tests fail.

- [x] **Step 2: Implement sidecar helpers and Phase-1 write**

Add small helper functions beside the invite node-key staging code. After `transport.SealNodeKey` succeeds in Phase-1, write `keys.d/node.key.gen` with the selected `st.nodeKeyKEKGen` and keep the existing sentinel field unchanged.

Verify: targeted tests pass.

### Task 2: Keep Phase-2 Migration Cleanup Explicit

**Files:**
- Modify: `internal/serveruntime/invite_join_boot.go`
- Modify: `internal/serveruntime/invite_join_boot_test.go`

- [x] **Step 1: Write failing tests**

Extend the existing `loadAndMigrateInviteNodeKey` tests to assert a stale `keys.d/node.key.gen` sidecar is deleted after successful re-seal under the static `encryption.key`.

Verify:

```bash
go test ./internal/serveruntime -run 'TestLoadAndMigrateInviteNodeKey' -count=1
```

Expected before implementation: if cleanup regresses, the test fails. Existing code already has `deleteNodeKeyGen`; this step locks the contract for the new sidecar.

- [x] **Step 2: Confirm cleanup path**

If the test exposes a gap, patch `loadAndMigrateInviteNodeKey` so migration removes the sidecar after re-seal. Do not change the static-key bootstrap contract in this slice.

Verify: targeted tests pass.

### Task 3: Documentation and PR

**Files:**
- Modify: `TODOS.md`
- Modify: `CHANGELOG.md`
- Modify: `VERSION`

- [x] **Step 1: Update TODO context**

Record this as the shipped prep slice under the D-cut node-identity bullet. Keep bootstrap-envelope removal, prune evidence, and final static-key removal open.

- [x] **Step 2: Run focused package tests**

```bash
go test ./internal/serveruntime -count=1
go test ./internal/cluster -run 'Test.*Bootstrap|Test.*Invite|Test.*Join' -count=1
```

- [x] **Step 3: Run unit suite**

```bash
make test-unit
```

- [ ] **Step 4: Ship**

Use the ship workflow to bump `VERSION`, update `CHANGELOG.md`, commit, push, and open a PR. Stop at the open PR; do not merge.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Engineering plan review | feature-pipeline | First plan was not independently shippable | 1 | revised | Advisor found three blockers: early boot still requires `encryption.key`, KEK prune lacks generation evidence, and a strict sidecar loader would not be crash-safe. This revised slice keeps static-key bootstrap/migration intact and only persists/cleans sidecar evidence, so it avoids those blockers while preparing the next slice. |
