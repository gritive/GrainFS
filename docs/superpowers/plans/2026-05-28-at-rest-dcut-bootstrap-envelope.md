# At-Rest D-Cut Bootstrap Envelope Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the static-key bootstrap and node-identity survival role with a KEK-generation based design that can be implemented safely in later code slices.

**Architecture:** This PR is a design and planning slice for the D-cut blocker in `TODOS.md`. It records the target bootstrap envelope, node identity sealing rule, prune-safety contract, and follow-on implementation slices before touching high-risk boot code.

**Tech Stack:** Go, FlatBuffers, serveruntime boot phases, zero-CA invite join, KEK/DEK envelope, Raft metadata.

---

### Task 1: Capture the D-Cut Design

**Files:**
- Create: `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`
- Modify: `TODOS.md`

- [x] **Step 1: Read current boot and join code**

Read these files and confirm the existing static-key coupling:

```bash
rg -n "RawEncryptionKey|BootstrapSecrets|node.key.enc|encryption.key" internal/serveruntime internal/cluster internal/transport TODOS.md
```

Expected: hits in `Config`, `bootstrap_secret_provider.go`, `invite_join_boot.go`,
`node_identity_boot.go`, `bootstrap_codec.go`, and `meta_join.go`.

- [x] **Step 2: Write the design document**

Create `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`
with:

```markdown
# At-Rest D-Cut Bootstrap Envelope Design

## Problem

The static `encryption.key` still carries boot-time responsibilities: invite
bootstrap delivery and prune-proof node identity sealing.

## Target Flow

The bootstrap envelope carries KEK generations and transport bootstrap data, not
the static key or DEK plaintext. Node identity is sealed under a KEK generation
with prune evidence that prevents deleting the only generation able to reopen a
voter's `node.key.enc`.
```

Expected: the doc distinguishes bootstrap envelope, node identity sealing, boot
ordering, compatibility, and implementation slices.

- [x] **Step 3: Update TODO context**

Add the design path under the D-cut bullets in `TODOS.md`, without checking off
the implementation bullets.

Expected: the TODO now points future workers at the design and still keeps D-cut
implementation open.

- [x] **Step 4: Verify docs are tracked intentionally**

Run:

```bash
git status --short
git check-ignore -v docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md docs/superpowers/plans/2026-05-28-at-rest-dcut-bootstrap-envelope.md || true
```

Expected: docs are ignored by the global ignore and must be added with
`git add -f`.

### Task 2: Plan Gate

**Files:**
- Modify: `docs/superpowers/plans/2026-05-28-at-rest-dcut-bootstrap-envelope.md`

- [x] **Step 1: Run plan review**

Run an engineering review over this plan and design.

Expected: no blocker remains around boot ordering, KEK prune safety, or
bootstrap-envelope scope.

- [x] **Step 2: Fold findings**

If review finds a valid issue, patch the design and this plan, then rerun the
review.

Expected: the plan ends with a review report showing a clean gate.

### Task 3: Verification and PR Prep

**Files:**
- Modify: `TODOS.md`
- Create: `docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md`
- Create: `docs/superpowers/plans/2026-05-28-at-rest-dcut-bootstrap-envelope.md`

- [x] **Step 1: Run baseline unit tests**

Run:

```bash
make test-unit
```

Expected: PASS. This PR is documentation-only, but the baseline confirms the
worktree started clean.

- [x] **Step 2: Review final diff**

Run:

```bash
git diff -- TODOS.md docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md docs/superpowers/plans/2026-05-28-at-rest-dcut-bootstrap-envelope.md
```

Expected: diff is limited to the design, plan, and TODO pointer.

- [x] **Step 3: Commit**

Run:

```bash
git add TODOS.md
git add -f docs/superpowers/specs/2026-05-28-at-rest-dcut-bootstrap-envelope-design.md docs/superpowers/plans/2026-05-28-at-rest-dcut-bootstrap-envelope.md
git commit -m "docs: design at-rest static key D-cut"
```

Expected: a single documentation/design commit with no generated trailers.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| Engineering plan review | feature-pipeline | Boot ordering, bootstrap scope, KEK prune safety | 1 | clean | Folded findings: keep FlatBuffers `encryption_key` as legacy-readable until format-7, include post-drop pre-Listen node-key loading in Slice B, and thread `node_key_kek_gen` through existing peer registry/self-register evidence. |
