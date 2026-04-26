# Quick TODOs Design

**Date:** 2026-04-26  
**Status:** APPROVED  
**Scope:** 4 small independent improvements

---

## 1. TODOS.md Cleanup

Remove the `Preflight health check` item — shipped in commit `1a988fd` (#58).

**Change:** Delete that single bullet from `## Phase 20: Operations & Onboarding`.

---

## 2. NBD Server Cross-Platform

**Problem:** `internal/nbd/nbd.go` has `//go:build linux` but uses only standard Go (`net`, `encoding/binary`, `io`). The Linux restriction is left over from the client side (kernel `nbd-client`), not the server.

**Changes:**
- `internal/nbd/nbd.go` — remove `//go:build linux` tag
- `cmd/grainfs/serve_nbd_other.go` — delete (stub no longer needed)
- `cmd/grainfs/serve_nbd_linux.go` — rename to `serve_nbd.go`, remove `//go:build linux` tag
- `cmd/grainfs/serve.go:56` — update `--nbd-port` description: `"NBD server port (0 = disabled). Client-side nbd-client still requires Linux."`
- `internal/nbd/nbd_test.go`, `e2e_test.go`, `panic_recovery_test.go` — keep `//go:build linux` (require Linux kernel NBD client)

**Result:** `grainfs` binary compiles on macOS/Windows. NBD client usage still requires Linux.

---

## 3. Safe Defaults (`--no-auth` Warning)

**Problem:** Running `grainfs serve` without `--access-key`/`--secret-key` silently disables S3 authentication, which is dangerous in production.

**Change:** In `cmd/grainfs/serve.go`, after parsing auth flags, add:

```go
if accessKey == "" || secretKey == "" {
    log.Warn().Msg("S3 authentication disabled — set --access-key and --secret-key for production")
}
```

No flag changes. Warning only. Existing `--no-auth` behavior unchanged.

---

## 4. `grainfs volume recalculate` (Online/REST)

**Problem:** `AllocatedBlocks` in volume metadata can drift from the actual block object count after crashes or bugs. No repair tool exists.

### Backend — `internal/volume/volume.go`

Add constant (also update `Delete()` to use it):
```go
const maxBlockListLimit = 1_000_000
```

New exported method on `Manager`:

```go
// Recalculate counts actual block objects via ListObjects and rewrites AllocatedBlocks.
// Returns (before, after int64, err error).
func (m *Manager) Recalculate(name string) (int64, int64, error)
```

Implementation:
1. Lock `m.mu`
2. Lookup volume by name; error if not found
3. `objs, err := ListObjects(volumeBucketName, blockPrefix(name), maxBlockListLimit)`
4. If `err != nil`, return `0, 0, err` (do not write)
5. `after = int64(len(objs))`
6. `before = vol.AllocatedBlocks`
7. If `before == after`, return early (no write)
8. `vol.AllocatedBlocks = after`; `if err := PutObject(...); err != nil { return 0, 0, err }`
9. Return `before, after, nil`

Note: `before=-1` (untracked volume) is treated the same as any other drift — Recalculate initializes tracking.

### REST Endpoint — `internal/server/volume_handlers.go`

```
POST /volumes/:name/recalculate
```

Response `200 OK`:
```json
{"volume": "default", "before": 42, "after": 45, "fixed": true}
```

`fixed` is `true` when `before != after`. Errors: `404` if volume not found, `500` on backend error.

### CLI — `cmd/grainfs/volume.go` (new file)

```
grainfs volume recalculate <name> [--endpoint http://localhost:9000]
```

- `--endpoint` defaults to `http://localhost:9000`
- Prints: `recalculated "default": 42 → 45 (fixed)` or `recalculated "default": 45 → 45 (no change)`
- Exits non-zero on HTTP error

### Testing

Unit tests in `internal/volume/volume_test.go` (table-driven):
1. **Drift**: create volume, write 3 blocks, set `AllocatedBlocks=99`, call `Recalculate` → assert `before=99, after=3, err=nil`
2. **Not found**: `Recalculate("nonexistent")` → assert `err != nil`
3. **ListObjects error**: inject failing backend → assert error propagated, metadata not written
4. **Early return**: write 3 blocks, `AllocatedBlocks` already=3 → assert `before==after`, no write

No REST handler test needed (handler is a thin pass-through).

---

## Files Changed Summary

| File | Action |
|------|--------|
| `TODOS.md` | Remove Preflight health check item |
| `internal/nbd/nbd.go` | Remove `//go:build linux` |
| `cmd/grainfs/serve_nbd_other.go` | Delete |
| `cmd/grainfs/serve_nbd_linux.go` | Rename → `serve_nbd.go`, remove build tag |
| `cmd/grainfs/serve.go` | Add no-auth warning; update `--nbd-port` description |
| `internal/volume/volume.go` | Add `maxBlockListLimit` const + `Recalculate()` method; update `Delete()` to use const |
| `internal/server/volume_handlers.go` | Add `recalculateVolume` handler + register route in server.go |
| `cmd/grainfs/volume.go` | New file — `grainfs volume recalculate` CLI |
| `internal/volume/volume_test.go` | Add `Recalculate` unit tests (4 cases) |

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | — |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | — | — |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | CLEAN | 5 issues resolved, 0 critical gaps |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | — |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | — |

**VERDICT:** ENG CLEARED — ready to implement.
