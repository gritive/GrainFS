# Cluster Durability P0 Context Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the GrainFS storage boundary to context-first signatures without changing storage behavior.

**Architecture:** `storage.Backend` becomes the request-cancellation boundary used by S3, NFSv4, NBD, Iceberg migration, cluster coordination, cache, WAL, packblob, pull-through, recovery gate, and volume layers. This P0 slice is intentionally mechanical: pass existing request or service contexts where available, use `context.Background()` only at explicit process/test roots, and do not change replication, routing, MPU, EC, lifecycle, or metadata semantics.

**Tech Stack:** Go 1.26, standard `context`, existing storage/cluster/volume/server packages, `go test`, `make test`.

---

## Scope

This plan implements only P0 from `docs/superpowers/specs/2026-05-03-cluster-durability-architecture-design.md`.

Not included here:

- Partial IO Raft commands and forwarding.
- EC partial overwrite.
- Durable MPU staging.
- Behavioral cancellation beyond mechanical propagation.

Those become P1-P4 plans after this branch compiles and passes non-e2e tests.

## File Structure

### Modify

- `internal/storage/storage.go`  
  Add `context.Context` to `Backend`, `Truncatable`, and the new `PartialIO` optional interface.

- `internal/storage/local.go`, `internal/storage/multipart.go`  
  Add context parameters to local backend methods. Keep current behavior.

- `internal/storage/cache.go`, `internal/storage/swappable.go`, `internal/storage/recovery_gate.go`  
  Add context parameters and forward them to wrapped backends.

- `internal/storage/wal/backend.go`, `internal/storage/packblob/packed_backend.go`, `internal/storage/pullthrough/pullthrough.go`  
  Add context parameters and forward them to wrapped backends. Preserve cache invalidation and WAL behavior.

- `internal/cluster/backend.go`, `internal/cluster/cluster_coordinator.go`  
  Add context parameters to backend and coordinator methods. Use the received context for existing Raft proposal, forwarding, and IO calls when the callee already accepts context; leave deeper behavioral cancellation to P4.

- `cmd/grainfs/*.go`, `internal/server/*.go`, `internal/nfs4server/*.go`, `internal/vfs/*.go`, `internal/volume/*.go`, `internal/lifecycle/*.go`, `internal/scrubber/*.go`, `internal/migration/*.go`, relevant tests  
  Update call sites to pass an existing request/service/test context.

- `internal/storage/context_passthrough_test.go`  
  Add focused tests proving wrapper methods receive and forward the caller context.

## Task 1: Storage Interface And Wrapper Tests

**Files:**
- Modify: `internal/storage/storage.go`
- Create: `internal/storage/context_passthrough_test.go`

- [ ] **Step 1: Add a failing wrapper context test**

Create `internal/storage/context_passthrough_test.go`:

```go
package storage

import (
	"context"
	"io"
	"strings"
	"testing"
)

type contextRecorderBackend struct {
	ctx context.Context
}

func (b *contextRecorderBackend) CreateBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) HeadBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) DeleteBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) ListBuckets(ctx context.Context) ([]string, error) {
	b.ctx = ctx
	return nil, nil
}
func (b *contextRecorderBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.ctx = ctx
	return io.NopCloser(strings.NewReader("ok")), &Object{Key: key, Size: 2}, nil
}
func (b *contextRecorderBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	b.ctx = ctx
	return nil, nil
}
func (b *contextRecorderBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	b.ctx = ctx
	return &MultipartUpload{UploadID: "u", Bucket: bucket, Key: key}, nil
}
func (b *contextRecorderBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
	b.ctx = ctx
	return &Part{PartNumber: partNumber}, nil
}
func (b *contextRecorderBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	b.ctx = ctx
	return nil
}

func TestSwappableBackend_ForwardsContext(t *testing.T) {
	rec := &contextRecorderBackend{}
	sb := NewSwappableBackend(rec)
	ctx := context.WithValue(context.Background(), testContextKey{}, "caller")

	if _, err := sb.PutObject(ctx, "b", "k", strings.NewReader("x"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if rec.ctx != ctx {
		t.Fatalf("wrapped backend got %p, want %p", rec.ctx, ctx)
	}
}

type testContextKey struct{}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
go test ./internal/storage -run TestSwappableBackend_ForwardsContext -count=1
```

Expected: fail to compile because `Backend` and `SwappableBackend` do not accept context yet.

- [ ] **Step 3: Convert `storage.Backend` and optional interfaces**

In `internal/storage/storage.go`, import `context` and change the interfaces to:

```go
type Backend interface {
	CreateBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)

	PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error)
	HeadObject(ctx context.Context, bucket, key string) (*Object, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error)
	WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error

	CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
}

type Truncatable interface {
	Truncate(ctx context.Context, bucket, key string, size int64) error
}

type PartialIO interface {
	WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error)
	ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
	Truncate(ctx context.Context, bucket, key string, size int64) error
}
```

- [ ] **Step 4: Convert `SwappableBackend`**

In `internal/storage/swappable.go`, add `context.Context` to every method and forward the same context to `sb.Load()`:

```go
func (sb *SwappableBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return sb.Load().PutObject(ctx, bucket, key, r, contentType)
}
```

Apply the same pattern to every `Backend` method in the file.

- [ ] **Step 5: Verify the focused test now compiles or exposes the next wrapper**

Run:

```bash
go test ./internal/storage -run TestSwappableBackend_ForwardsContext -count=1
```

Expected: may still fail because other files in `internal/storage` have not been converted. Use the errors to drive Task 2.

## Task 2: Convert Storage Package Implementations

**Files:**
- Modify: `internal/storage/local.go`
- Modify: `internal/storage/multipart.go`
- Modify: `internal/storage/cache.go`
- Modify: `internal/storage/recovery_gate.go`
- Modify: `internal/storage/wal/backend.go`
- Modify: `internal/storage/pullthrough/pullthrough.go`
- Modify: `internal/storage/packblob/packed_backend.go`
- Modify: storage package tests with fake backends

- [ ] **Step 1: Convert `LocalBackend` signatures**

Add `context.Context` as the first parameter to all `LocalBackend` methods that implement `Backend`, `Truncatable`, or partial IO. For this P0 slice, add `_ = ctx` at the start of methods that do not otherwise use the context.

Example:

```go
func (b *LocalBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	_ = ctx
	// existing body unchanged
}
```

- [ ] **Step 2: Convert `CachedBackend` signatures and forwarding**

Add `context.Context` as the first parameter to `CachedBackend` methods. Pass it into `cb.backend`.

Example:

```go
obj, err := cb.backend.PutObject(ctx, bucket, key, r, contentType)
```

- [ ] **Step 3: Convert WAL backend signatures and forwarding**

Add `context.Context` as the first parameter to WAL methods. Pass it to wrapped backend calls after preserving the existing WAL record ordering.

Example:

```go
obj, err := b.inner.PutObject(ctx, bucket, key, r, contentType)
```

- [ ] **Step 4: Convert packblob and pull-through wrappers**

Add context parameters and pass them to wrapped backend calls. For pull-through upstream calls that do not yet accept caller context, leave them unchanged; that is outside `storage.Backend`.

- [ ] **Step 5: Convert recovery gate**

Add context parameters to write-blocking methods. Keep return behavior unchanged.

- [ ] **Step 6: Run storage package tests**

Run:

```bash
go test ./internal/storage/... -count=1
```

Expected: pass, or fail only on external call sites handled in later tasks.

- [ ] **Step 7: Commit storage package migration**

Run:

```bash
git add internal/storage
git commit -m "refactor: add context to storage backend boundary"
```

## Task 3: Convert Cluster Backend And Coordinator

**Files:**
- Modify: `internal/cluster/backend.go`
- Modify: `internal/cluster/cluster_coordinator.go`
- Modify: cluster tests and fakes

- [ ] **Step 1: Convert `DistributedBackend` signatures**

Add `context.Context` as the first parameter to all `DistributedBackend` methods implementing `storage.Backend`, `storage.Truncatable`, and `storage.PartialIO`.

Use the received `ctx` for existing calls that already accept context, including assignment proposals, forwarded calls, shard IO, and Raft proposals. If the existing code has a local timeout derived from `context.Background()`, derive it from `ctx` instead:

```go
proposalCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
```

- [ ] **Step 2: Convert `ClusterCoordinator` signatures**

Add `context.Context` as the first parameter to all coordinator methods implementing storage interfaces.

Forward the context to `c.base`, routed group backends, forward sender calls that accept context, and metadata calls that already accept context.

Example:

```go
func (c *ClusterCoordinator) CreateBucket(ctx context.Context, bucket string) error {
	return c.base.CreateBucket(ctx, bucket)
}
```

- [ ] **Step 3: Update cluster fakes and tests**

Update fake backends in `internal/cluster/*_test.go` to accept context. Use `_ = ctx` where the fake does not inspect it.

- [ ] **Step 4: Run focused cluster compile/test**

Run:

```bash
go test ./internal/cluster -run 'Test.*(Coordinator|Backend|Forward|Router|Multipart)' -count=1
```

Expected: pass or expose non-cluster call sites handled in Task 4.

- [ ] **Step 5: Commit cluster migration**

Run:

```bash
git add internal/cluster
git commit -m "refactor: pass context through cluster storage boundary"
```

## Task 4: Convert Protocol, Volume, Lifecycle, And Migration Call Sites

**Files:**
- Modify: `internal/server/*.go`
- Modify: `internal/nfs4server/*.go`
- Modify: `internal/vfs/*.go`
- Modify: `internal/volume/*.go`
- Modify: `internal/lifecycle/*.go`
- Modify: `internal/scrubber/*.go`
- Modify: `internal/migration/*.go`
- Modify: `cmd/grainfs/*.go`
- Modify: tests and fakes in those packages

- [ ] **Step 1: Update HTTP/S3 server handlers**

For each backend call inside request handlers, pass the request context:

```go
ctx := r.Context()
obj, err := h.backend.PutObject(ctx, bucket, key, body, contentType)
```

- [ ] **Step 2: Update NFSv4 and VFS call sites**

Where an operation already has no request context, use the service/root context if available. If no context exists in the method signature, use `context.Background()` at the file-system operation root and leave deeper behavioral cancellation to P4.

Example:

```go
rc, obj, err := f.fs.backend.GetObject(context.Background(), bucket, key)
```

- [ ] **Step 3: Update `volume.Manager`**

Add `context.Context` to internal helper calls where practical. For exported methods that cannot change without widening public API in this P0 slice, use `context.Background()` at the manager method root and pass that context through all backend calls in that method.

Example:

```go
func (m *Manager) ensureBucket() error {
	ctx := context.Background()
	return m.backend.CreateBucket(ctx, volumeBucketName)
}
```

- [ ] **Step 4: Update lifecycle, scrubber, migration, and command helpers**

Use existing worker/migration command contexts where present. For startup helpers such as default bucket creation, pass the existing startup context.

- [ ] **Step 5: Update tests and package fakes**

For fake backends, add context parameters and `_ = ctx` in each method. For test call sites, pass `context.Background()` or the existing test timeout context.

- [ ] **Step 6: Run package-wide compile loop**

Run:

```bash
go test ./cmd/grainfs ./internal/... -run '^$'
```

Expected: all packages compile.

- [ ] **Step 7: Commit call-site migration**

Run:

```bash
git add cmd internal
git commit -m "refactor: pass contexts at storage call sites"
```

## Task 5: Guards Against New Request-Path Background Contexts

**Files:**
- Modify: tests or add script-free shell verification only

- [ ] **Step 1: Scan request-path background contexts**

Run:

```bash
rg -n 'context\\.(TODO|Background)\\(' cmd internal | rg -v '(_test\\.go|serve\\.go|cluster_join\\.go|quic\\.go|raft\\.go|meta_transport_quic\\.go|quic_rpc\\.go|s3adapter\\.go|s3upstream\\.go)'
```

Expected: no new request-path uses introduced by this branch. Existing process roots and tests are allowed.

- [ ] **Step 2: Run full non-e2e tests**

Run:

```bash
make test
```

Expected: pass.

- [ ] **Step 3: Run targeted e2e smoke with built binary**

Run:

```bash
make build
GRAINFS_BINARY=$(pwd)/bin/grainfs go test ./tests/e2e -run '^(TestSmoke_DeploymentVerification|TestDefaultBucket_ExistsOnStartup)$' -count=1 -timeout 4m -v
```

Expected: pass.

- [ ] **Step 4: Commit verification cleanup if needed**

If verification required small compile-only test/fake updates, commit them:

```bash
git add cmd internal tests
git commit -m "test: verify context-first storage boundary"
```

## Self-Review

- Spec coverage: This plan covers P0 only. P1-P4 intentionally remain separate future plans.
- Placeholder scan: No `TBD`, open implementation blanks, or unbounded "handle later" steps in the P0 execution path.
- Type consistency: Every storage boundary method uses `context.Context` as its first parameter.
