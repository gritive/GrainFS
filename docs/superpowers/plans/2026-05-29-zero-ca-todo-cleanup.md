# Zero-CA TODO Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the completed Zero-CA revocation follow-up from `TODOS.md` so the task file tracks only active/deferred work.

**Architecture:** This is a documentation-only cleanup. The Zero-CA revocation slice has already shipped, and the current task rule is to remove completed items from `TODOS.md` rather than leave checked entries.

**Tech Stack:** Markdown task tracking.

---

### Task 1: Remove Completed Zero-CA TODO Entry

**Files:**
- Modify: `TODOS.md`

- [x] **Step 1: Delete the completed Zero-CA revocation block**

Remove this completed block from `TODOS.md` under `Deferred Until Triggered`:

```markdown
- [x] **Zero-CA revocation slice**.
  PR-2a delivered live present-flip; PR-2b delivered complete-cutover
  (`present-flip -> cluster-key drop`), connection recycle, post-drop
  invite-join without the shared transport PSK, CLI/admin wiring, and focused
  multi-node E2E coverage. SHIPPED in current branch: `cluster revoke-node`
  admin/CLI, `RevokeNode` orchestration, registry removal, durable SPKI
  denylist snapshot, Phase-1-only pending invite burn/denylist, voter removal,
  targeted QUIC `ClosePeer`, and focused E2E proving a post-drop revoked
  node-id cannot rejoin. Phase-1-only stale pending and same persisted identity
  reuse are covered at FSM/MetaRaft level because the e2e harness does not
  expose a stable stop-after-Phase-1 hook and completed invite-join data dirs
  intentionally classify as normal boot. KEK/DEK rotation remains gated on
  cluster-wide KEK distribution.
```

- [x] **Step 2: Verify no completed Zero-CA task remains in `TODOS.md`**

Run:

```bash
rg -n "\[x\].*Zero-CA|Zero-CA revocation slice" TODOS.md
```

Expected: no output.

- [x] **Step 3: Verify the markdown diff is clean**

Run:

```bash
git diff --check
```

Expected: no output.
