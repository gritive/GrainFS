# Storage Operations Facade Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace server-side optional backend probing with a `storage.Operations` facade that preserves cache, WAL, recovery-gate, policy-cache, and fallback ordering.

**Architecture:** `storage.Backend` remains the low-level primitive. `storage.Operations` becomes the request-handler entry point for S3-facing storage capabilities and owns outer-first adapter lookup, safe fallbacks, typed unsupported-operation errors, and rollback behavior. `server.Server` receives a `ServerStorage` composition bundle so handlers use `Ops` while constructor-only subsystems still get snapshot, volume, and DB dependencies explicitly.

**Tech Stack:** Go 1.26.2, Hertz server handlers, Badger-backed storage, GrainFS `internal/storage`, `internal/server`, `internal/cluster`, and `internal/policy`.

---

## Scope Check

This plan covers one architectural slice: server handler capability probing removal. It touches policy code, storage facade code, server wiring, and tests because those are coupled by the existing handler probing. Snapshot and volume internals remain out of scope except for constructor wiring preservation.

## File Structure

- Create `internal/policy/policy.go`: bucket policy JSON model, parser, and evaluator moved from `internal/server`.
- Create `internal/policy/compiled.go`: compiled in-memory authorizer moved from `internal/server`.
- Create `internal/policy/store.go`: legacy parsed policy store if existing tests still need it.
- Move tests from `internal/server/policy*_test.go` to `internal/policy/*_test.go`; keep HTTP policy tests in `internal/server/server_test.go`.
- Create `internal/storage/operations_errors.go`: typed unsupported operation errors.
- Create `internal/storage/operations.go`: `Operations` struct, constructor, capability plan, lookup helpers, and ASCII decision diagram.
- Create `internal/storage/operations_acl.go`: `PutObjectWithACL`, `SetObjectACL`, and rollback behavior.
- Create `internal/storage/operations_copy.go`: `CopyObjectRequest`, `CopyObjectResult`, streaming fallback, and optimized adapter use.
- Create `internal/storage/operations_versioning.go`: versioned get/head/list/delete and bucket versioning operations.
- Create `internal/storage/operations_policy.go`: bucket policy persistence plus compiled cache synchronization.
- Create `internal/storage/operations_test.go`, `operations_acl_test.go`, `operations_copy_test.go`, `operations_versioning_test.go`, `operations_policy_test.go`: contract tests around the facade.
- Modify `internal/server/server.go`: introduce `ServerStorage`, store `ops`, and wire constructor-only dependencies.
- Modify `internal/server/handlers.go`: route optional capability operations through `s.ops`.
- Modify `internal/server/versioning.go`: remove local find helpers and call `s.ops`.
- Modify `internal/server/policy_handlers.go`: call `s.ops` for policy CRUD and use `policy.Authorizer` for auth.
- Modify `internal/server/authz.go`: keep compiled authorizer hot path.
- Modify `internal/storage/recovery_gate.go`: use exported storage capability interfaces where needed.
- Modify `internal/storage/storage.go`: keep existing `Copier` unchanged and add exported capability interfaces if not created in `operations.go`.

---

## Task 1: Move Policy Domain To `internal/policy`

**Files:**
- Create: `internal/policy/policy.go`
- Create: `internal/policy/compiled.go`
- Create: `internal/policy/store.go`
- Modify: `internal/server/policy.go`
- Modify: `internal/server/policy_compiled.go`
- Modify: `internal/server/policy_store.go`
- Move tests: `internal/server/policy_test.go`, `internal/server/policy_compiled_test.go`, `internal/server/policy_bench_test.go`

- [ ] **Step 1: Create the target package with moved model code**

Create `internal/policy/policy.go` with the content from `internal/server/policy.go`, changing only the package line:

```go
package policy
```

Keep exported names unchanged: `BucketPolicy`, `PolicyStatement`, `PolicyPrincipal`, `ParsePolicy`, and `(*BucketPolicy).IsAllowed`.

- [ ] **Step 2: Create the compiled authorizer in the target package**

Create `internal/policy/compiled.go` from `internal/server/policy_compiled.go`, changing the package line and preserving the `s3auth` import:

```go
package policy

import (
	"context"
	"strings"
	"sync"

	"github.com/gritive/GrainFS/internal/s3auth"
)
```

Keep exported names unchanged: `CompiledPolicyStore` and `NewCompiledPolicyStore`.

- [ ] **Step 3: Create the parsed store in the target package**

Create `internal/policy/store.go` from `internal/server/policy_store.go`, changing the package line:

```go
package policy

import "sync"
```

Keep exported names unchanged: `PolicyStore` and `NewPolicyStore`.

- [ ] **Step 4: Move policy unit tests and benchmark**

Run:

```bash
mkdir -p internal/policy
git mv internal/server/policy_test.go internal/policy/policy_test.go
git mv internal/server/policy_compiled_test.go internal/policy/compiled_test.go
git mv internal/server/policy_bench_test.go internal/policy/bench_test.go
```

Edit the first line of all three moved files:

```go
package policy
```

- [ ] **Step 5: Replace server policy files with aliases during migration**

Replace `internal/server/policy.go` with:

```go
package server

import "github.com/gritive/GrainFS/internal/policy"

type BucketPolicy = policy.BucketPolicy
type PolicyStatement = policy.PolicyStatement
type PolicyPrincipal = policy.PolicyPrincipal

var ParsePolicy = policy.ParsePolicy
```

Replace `internal/server/policy_compiled.go` with:

```go
package server

import "github.com/gritive/GrainFS/internal/policy"

type CompiledPolicyStore = policy.CompiledPolicyStore

var NewCompiledPolicyStore = policy.NewCompiledPolicyStore
```

Replace `internal/server/policy_store.go` with:

```go
package server

import "github.com/gritive/GrainFS/internal/policy"

type PolicyStore = policy.PolicyStore

var NewPolicyStore = policy.NewPolicyStore
```

- [ ] **Step 6: Verify policy move**

Run:

```bash
go test ./internal/policy ./internal/server -run 'Policy|Authz' -count=1
```

Expected: PASS. If compile fails on unexported symbols in moved tests, keep those tests in package `policy` and move the referenced helper with the production code in the same commit.

- [ ] **Step 7: Commit**

```bash
git add internal/policy internal/server/policy.go internal/server/policy_compiled.go internal/server/policy_store.go
git commit -m "refactor: move bucket policy domain to internal policy"
```

---

## Task 2: Add Operations Core, Capability Plan, And Typed Errors

**Files:**
- Create: `internal/storage/operations_errors.go`
- Create: `internal/storage/operations.go`
- Test: `internal/storage/operations_test.go`

- [ ] **Step 1: Write failing tests for typed unsupported errors**

Create `internal/storage/operations_test.go`:

```go
package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnsupportedOperationErrorMatchesSentinel(t *testing.T) {
	err := UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	require.Equal(t, "DeleteObjectVersion", err.Op)
	require.Equal(t, UnsupportedReasonNoAdapter, err.Reason)
	require.Contains(t, err.Error(), "DeleteObjectVersion")
	require.Contains(t, err.Error(), string(UnsupportedReasonNoAdapter))
}

func TestUnsupportedOperationErrorsAsTyped(t *testing.T) {
	err := error(UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonUnsafeFallback})
	var typed UnsupportedOperationError

	require.True(t, errors.As(err, &typed))
	require.Equal(t, "CopyObject", typed.Op)
	require.Equal(t, UnsupportedReasonUnsafeFallback, typed.Reason)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/storage -run 'TestUnsupportedOperation' -count=1
```

Expected: FAIL with undefined `UnsupportedOperationError`.

- [ ] **Step 3: Implement typed unsupported errors**

Create `internal/storage/operations_errors.go`:

```go
package storage

import (
	"errors"
	"fmt"
)

var ErrUnsupportedOperation = errors.New("unsupported storage operation")

type UnsupportedReason string

const (
	UnsupportedReasonNoAdapter      UnsupportedReason = "no_adapter"
	UnsupportedReasonUnsafeFallback UnsupportedReason = "unsafe_fallback"
	UnsupportedReasonRollbackFailed UnsupportedReason = "rollback_failed"
)

type UnsupportedOperationError struct {
	Op     string
	Reason UnsupportedReason
}

func (e UnsupportedOperationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrUnsupportedOperation, e.Op, e.Reason)
}

func (e UnsupportedOperationError) Unwrap() error {
	return ErrUnsupportedOperation
}
```

- [ ] **Step 4: Add Operations skeleton and capability interfaces**

Create `internal/storage/operations.go`:

```go
package storage

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/policy"
)

type Unwrapper interface {
	Unwrap() Backend
}

type BucketPolicyBackend interface {
	GetBucketPolicy(bucket string) ([]byte, error)
	SetBucketPolicy(bucket string, policyJSON []byte) error
	DeleteBucketPolicy(bucket string) error
}

type BucketVersioner interface {
	SetBucketVersioning(bucket, state string) error
	GetBucketVersioning(bucket string) (string, error)
}

type VersionedGetter interface {
	GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error)
}

type VersionedHeader interface {
	HeadObjectVersion(bucket, key, versionID string) (*Object, error)
}

type ObjectVersionLister interface {
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error)
}

type ObjectVersionDeleter interface {
	DeleteObjectVersion(bucket, key, versionID string) error
}

type VersionedSoftDeleter interface {
	DeleteObjectReturningMarker(bucket, key string) (markerID string, err error)
}

type capabilityPlan struct {
	aclSetter              ACLSetter
	atomicACLPutter        AtomicACLPutter
	copier                 Copier
	policyBackend          BucketPolicyBackend
	bucketVersioner        BucketVersioner
	versionedGetter        VersionedGetter
	versionedHeader        VersionedHeader
	objectVersionLister    ObjectVersionLister
	objectVersionDeleter   ObjectVersionDeleter
	versionedSoftDeleter   VersionedSoftDeleter
}

type Operations struct {
	backend Backend
	policy  *policy.CompiledPolicyStore
	plan    capabilityPlan
}

func NewOperations(backend Backend, policyStore *policy.CompiledPolicyStore) *Operations {
	if policyStore == nil {
		policyStore = policy.NewCompiledPolicyStore()
	}
	ops := &Operations{backend: backend, policy: policyStore}
	ops.plan = ops.buildPlan(backend)
	return ops
}

func (o *Operations) Authorizer() *policy.CompiledPolicyStore {
	return o.policy
}

func (o *Operations) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return o.backend.PutObject(ctx, bucket, key, r, contentType)
}

func (o *Operations) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	return o.backend.GetObject(ctx, bucket, key)
}

func (o *Operations) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return o.backend.HeadObject(ctx, bucket, key)
}

func (o *Operations) DeleteObject(ctx context.Context, bucket, key string) error {
	return o.backend.DeleteObject(ctx, bucket, key)
}

func (o *Operations) buildPlan(root Backend) capabilityPlan {
	var plan capabilityPlan
	for b := root; b != nil; {
		if plan.aclSetter == nil {
			plan.aclSetter, _ = b.(ACLSetter)
		}
		if plan.atomicACLPutter == nil {
			plan.atomicACLPutter, _ = b.(AtomicACLPutter)
		}
		if plan.copier == nil {
			plan.copier, _ = b.(Copier)
		}
		if plan.policyBackend == nil {
			plan.policyBackend, _ = b.(BucketPolicyBackend)
		}
		if plan.bucketVersioner == nil {
			plan.bucketVersioner, _ = b.(BucketVersioner)
		}
		if plan.versionedGetter == nil {
			plan.versionedGetter, _ = b.(VersionedGetter)
		}
		if plan.versionedHeader == nil {
			plan.versionedHeader, _ = b.(VersionedHeader)
		}
		if plan.objectVersionLister == nil {
			plan.objectVersionLister, _ = b.(ObjectVersionLister)
		}
		if plan.objectVersionDeleter == nil {
			plan.objectVersionDeleter, _ = b.(ObjectVersionDeleter)
		}
		if plan.versionedSoftDeleter == nil {
			plan.versionedSoftDeleter, _ = b.(VersionedSoftDeleter)
		}
		u, ok := b.(Unwrapper)
		if !ok {
			break
		}
		b = u.Unwrap()
	}
	return plan
}

// Mutating capability decision tree:
//
// operation requested
//     |
//     +-- adapter exists on outer chain? ---- yes --> call adapter
//     |
//     no
//     |
//     +-- safe fallback exists? ------------ yes --> call fallback through Operations
//     |
//     no
//     |
//     +-- return UnsupportedOperationError{Op, Reason}
```

- [ ] **Step 5: Verify core tests pass**

Run:

```bash
go test ./internal/storage -run 'TestUnsupportedOperation' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/operations.go internal/storage/operations_errors.go internal/storage/operations_test.go
git commit -m "feat: add storage operations core"
```

---

## Task 3: Implement ACL Operations With Rollback

**Files:**
- Create: `internal/storage/operations_acl.go`
- Test: `internal/storage/operations_acl_test.go`

- [ ] **Step 1: Add failing ACL rollback tests**

Create `internal/storage/operations_acl_test.go` with a focused fake backend:

```go
package storage

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type aclOpsBackend struct {
	Backend
	putVersionID string
	setErr       error
	deleted      []string
}

func (b *aclOpsBackend) PutObject(context.Context, string, string, io.Reader, string) (*Object, error) {
	return &Object{Key: "k", VersionID: b.putVersionID}, nil
}

func (b *aclOpsBackend) SetObjectACL(string, string, uint8) error {
	return b.setErr
}

func (b *aclOpsBackend) DeleteObjectVersion(_, _, versionID string) error {
	b.deleted = append(b.deleted, versionID)
	return nil
}

func TestOperationsPutObjectWithACLRollsBackNewVersionOnSetFailure(t *testing.T) {
	backend := &aclOpsBackend{putVersionID: "v1", setErr: errTest("acl failed")}
	ops := NewOperations(backend, nil)

	obj, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 2)

	require.Nil(t, obj)
	require.Error(t, err)
	require.Equal(t, []string{"v1"}, backend.deleted)
}

func TestOperationsPutObjectWithACLReturnsRollbackFailureWhenVersionMissing(t *testing.T) {
	backend := &aclOpsBackend{setErr: errTest("acl failed")}
	ops := NewOperations(backend, nil)

	_, err := ops.PutObjectWithACL(context.Background(), "b", "k", strings.NewReader("data"), "text/plain", 2)

	require.ErrorIs(t, err, ErrUnsupportedOperation)
	var typed UnsupportedOperationError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, UnsupportedReasonRollbackFailed, typed.Reason)
	require.Empty(t, backend.deleted)
}

type errTest string

func (e errTest) Error() string { return string(e) }
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
go test ./internal/storage -run 'TestOperationsPutObjectWithACL' -count=1
```

Expected: FAIL with undefined `PutObjectWithACL`.

- [ ] **Step 3: Implement ACL operations**

Create `internal/storage/operations_acl.go`:

```go
package storage

import (
	"context"
	"fmt"
	"io"
)

func (o *Operations) SetObjectACL(bucket, key string, acl uint8) error {
	if o.plan.aclSetter == nil {
		return UnsupportedOperationError{Op: "SetObjectACL", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.aclSetter.SetObjectACL(bucket, key, acl)
}

func (o *Operations) PutObjectWithACL(ctx context.Context, bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	if o.plan.atomicACLPutter != nil {
		return o.plan.atomicACLPutter.PutObjectWithACL(bucket, key, r, contentType, acl)
	}
	obj, err := o.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := o.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" || o.plan.objectVersionDeleter == nil {
			return nil, UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}
		}
		if rollbackErr := o.plan.objectVersionDeleter.DeleteObjectVersion(bucket, key, obj.VersionID); rollbackErr != nil {
			return nil, fmt.Errorf("%w: rollback delete version %s: %v", UnsupportedOperationError{Op: "PutObjectWithACL", Reason: UnsupportedReasonRollbackFailed}, obj.VersionID, rollbackErr)
		}
		return nil, err
	}
	return obj, nil
}
```

- [ ] **Step 4: Verify ACL tests**

Run:

```bash
go test ./internal/storage -run 'TestOperationsPutObjectWithACL|TestUnsupportedOperation' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/operations_acl.go internal/storage/operations_acl_test.go
git commit -m "feat: add storage operations ACL fallback"
```

---

## Task 4: Implement Versioning And Policy Operations

**Files:**
- Create: `internal/storage/operations_versioning.go`
- Create: `internal/storage/operations_policy.go`
- Test: `internal/storage/operations_versioning_test.go`
- Test: `internal/storage/operations_policy_test.go`

- [ ] **Step 1: Write failing unsupported versioning tests**

Create `internal/storage/operations_versioning_test.go`:

```go
package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsDeleteObjectVersionUnsupported(t *testing.T) {
	ops := NewOperations(&LocalBackend{}, nil)

	err := ops.DeleteObjectVersion("b", "k", "v1")

	require.ErrorIs(t, err, ErrUnsupportedOperation)
}

func TestOperationsDeleteObjectReturningMarkerFallback(t *testing.T) {
	backend := &deleteOnlyBackend{}
	ops := NewOperations(backend, nil)

	markerID, err := ops.DeleteObjectReturningMarker("b", "k")

	require.NoError(t, err)
	require.Empty(t, markerID)
	require.Equal(t, []string{"b/k"}, backend.deleted)
}

type deleteOnlyBackend struct {
	Backend
	deleted []string
}

func (b *deleteOnlyBackend) DeleteObject(_ context.Context, bucket, key string) error {
	b.deleted = append(b.deleted, bucket+"/"+key)
	return nil
}
```

Add `context` to the imports.

- [ ] **Step 2: Implement versioning operations**

Create `internal/storage/operations_versioning.go`:

```go
package storage

import (
	"context"
	"io"
)

func (o *Operations) SetBucketVersioning(bucket, state string) error {
	if o.plan.bucketVersioner == nil {
		return UnsupportedOperationError{Op: "SetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.bucketVersioner.SetBucketVersioning(bucket, state)
}

func (o *Operations) GetBucketVersioning(bucket string) (string, error) {
	if o.plan.bucketVersioner == nil {
		return "", UnsupportedOperationError{Op: "GetBucketVersioning", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.bucketVersioner.GetBucketVersioning(bucket)
}

func (o *Operations) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *Object, error) {
	if o.plan.versionedGetter == nil {
		return nil, nil, UnsupportedOperationError{Op: "GetObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.versionedGetter.GetObjectVersion(bucket, key, versionID)
}

func (o *Operations) HeadObjectVersion(bucket, key, versionID string) (*Object, error) {
	if o.plan.versionedHeader == nil {
		return nil, UnsupportedOperationError{Op: "HeadObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.versionedHeader.HeadObjectVersion(bucket, key, versionID)
}

func (o *Operations) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*ObjectVersion, error) {
	if o.plan.objectVersionLister == nil {
		return nil, UnsupportedOperationError{Op: "ListObjectVersions", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.objectVersionLister.ListObjectVersions(bucket, prefix, maxKeys)
}

func (o *Operations) DeleteObjectVersion(bucket, key, versionID string) error {
	if o.plan.objectVersionDeleter == nil {
		return UnsupportedOperationError{Op: "DeleteObjectVersion", Reason: UnsupportedReasonNoAdapter}
	}
	return o.plan.objectVersionDeleter.DeleteObjectVersion(bucket, key, versionID)
}

func (o *Operations) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	if o.plan.versionedSoftDeleter != nil {
		return o.plan.versionedSoftDeleter.DeleteObjectReturningMarker(bucket, key)
	}
	return "", o.DeleteObject(context.Background(), bucket, key)
}
```

- [ ] **Step 3: Write failing policy operation tests**

Create `internal/storage/operations_policy_test.go`:

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type policyOpsBackend struct {
	Backend
	raw []byte
}

func (b *policyOpsBackend) SetBucketPolicy(_ string, raw []byte) error {
	b.raw = append([]byte(nil), raw...)
	return nil
}

func (b *policyOpsBackend) GetBucketPolicy(string) ([]byte, error) {
	return append([]byte(nil), b.raw...), nil
}

func (b *policyOpsBackend) DeleteBucketPolicy(string) error {
	b.raw = nil
	return nil
}

func TestOperationsPolicyCRUDSyncsCompiledCache(t *testing.T) {
	backend := &policyOpsBackend{}
	ops := NewOperations(backend, nil)
	raw := []byte(`{"Version":"2012-10-17","Statement":[]}`)

	require.NoError(t, ops.SetBucketPolicy("b", raw))
	got, err := ops.GetBucketPolicy("b")
	require.NoError(t, err)
	require.JSONEq(t, string(raw), string(got))

	require.NoError(t, ops.DeleteBucketPolicy("b"))
	require.Nil(t, ops.Authorizer().GetRaw("b"))
}
```

- [ ] **Step 4: Implement policy operations**

Create `internal/storage/operations_policy.go`:

```go
package storage

func (o *Operations) SetBucketPolicy(bucket string, policyJSON []byte) error {
	if o.plan.policyBackend == nil {
		if err := o.policy.Set(bucket, policyJSON); err != nil {
			return err
		}
		return nil
	}
	if err := o.plan.policyBackend.SetBucketPolicy(bucket, policyJSON); err != nil {
		return err
	}
	return o.policy.Set(bucket, policyJSON)
}

func (o *Operations) GetBucketPolicy(bucket string) ([]byte, error) {
	if raw := o.policy.GetRaw(bucket); raw != nil {
		return raw, nil
	}
	if o.plan.policyBackend == nil {
		return nil, UnsupportedOperationError{Op: "GetBucketPolicy", Reason: UnsupportedReasonNoAdapter}
	}
	raw, err := o.plan.policyBackend.GetBucketPolicy(bucket)
	if err != nil {
		return nil, err
	}
	if err := o.policy.Set(bucket, raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func (o *Operations) DeleteBucketPolicy(bucket string) error {
	if o.plan.policyBackend != nil {
		if err := o.plan.policyBackend.DeleteBucketPolicy(bucket); err != nil {
			return err
		}
	}
	o.policy.Delete(bucket)
	return nil
}
```

- [ ] **Step 5: Verify versioning and policy tests**

Run:

```bash
go test ./internal/storage -run 'TestOperations(DeleteObject|Policy|Unsupported)' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/storage/operations_versioning.go internal/storage/operations_policy.go internal/storage/operations_versioning_test.go internal/storage/operations_policy_test.go
git commit -m "feat: add storage operations versioning and policy methods"
```

---

## Task 5: Implement CopyObject Request Semantics With Streaming Fallback

**Files:**
- Create: `internal/storage/operations_copy.go`
- Test: `internal/storage/operations_copy_test.go`

- [ ] **Step 1: Write failing CopyObject fallback test**

Create `internal/storage/operations_copy_test.go`:

```go
package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type copyFallbackBackend struct {
	Backend
	putBody bytes.Buffer
}

func (b *copyFallbackBackend) GetObject(context.Context, string, string) (io.ReadCloser, *Object, error) {
	return io.NopCloser(bytes.NewBufferString("source-body")), &Object{ContentType: "text/plain", ACL: 2}, nil
}

func (b *copyFallbackBackend) PutObject(_ context.Context, _, key string, r io.Reader, contentType string) (*Object, error) {
	_, err := io.Copy(&b.putBody, r)
	if err != nil {
		return nil, err
	}
	return &Object{Key: key, ETag: "etag", LastModified: 1, ContentType: contentType}, nil
}

func TestOperationsCopyObjectFallbackStreamsSourceToPut(t *testing.T) {
	backend := &copyFallbackBackend{}
	ops := NewOperations(backend, nil)

	result, err := ops.CopyObject(context.Background(), CopyObjectRequest{
		SourceBucket: "src",
		SourceKey:    "k",
		DestBucket:   "dst",
		DestKey:      "copy",
	})

	require.NoError(t, err)
	require.Equal(t, "source-body", backend.putBody.String())
	require.Equal(t, "etag", result.Object.ETag)
}
```

- [ ] **Step 2: Implement CopyObject request and result types**

Create `internal/storage/operations_copy.go`:

```go
package storage

import (
	"context"
	"io"
)

type MetadataDirective string

const (
	MetadataDirectiveCopy    MetadataDirective = "COPY"
	MetadataDirectiveReplace MetadataDirective = "REPLACE"
)

type CopyObjectRequest struct {
	SourceBucket      string
	SourceKey         string
	SourceVersionID   string
	DestBucket        string
	DestKey           string
	ContentType       string
	ACL               uint8
	HasACL            bool
	MetadataDirective MetadataDirective
}

type CopyObjectResult struct {
	Object *Object
}

func (o *Operations) CopyObject(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
	if req.MetadataDirective == "" {
		req.MetadataDirective = MetadataDirectiveCopy
	}
	if o.plan.copier != nil && req.SourceVersionID == "" && req.MetadataDirective == MetadataDirectiveCopy && !req.HasACL {
		obj, err := o.plan.copier.CopyObject(req.SourceBucket, req.SourceKey, req.DestBucket, req.DestKey)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	var rc io.ReadCloser
	var src *Object
	var err error
	if req.SourceVersionID != "" {
		rc, src, err = o.GetObjectVersion(req.SourceBucket, req.SourceKey, req.SourceVersionID)
	} else {
		rc, src, err = o.GetObject(ctx, req.SourceBucket, req.SourceKey)
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	if src != nil && src.IsDeleteMarker {
		return nil, ErrMethodNotAllowed
	}

	contentType := req.ContentType
	if req.MetadataDirective != MetadataDirectiveReplace && src != nil {
		contentType = src.ContentType
	}

	var obj *Object
	if req.HasACL {
		obj, err = o.PutObjectWithACL(ctx, req.DestBucket, req.DestKey, rc, contentType, req.ACL)
	} else {
		obj, err = o.PutObject(ctx, req.DestBucket, req.DestKey, rc, contentType)
	}
	if err != nil {
		return nil, err
	}
	return &CopyObjectResult{Object: obj}, nil
}
```

- [ ] **Step 3: Verify CopyObject tests**

Run:

```bash
go test ./internal/storage -run 'TestOperationsCopyObject' -count=1
```

Expected: PASS.

- [ ] **Step 4: Add server integration tests for CopyObject headers**

Modify `internal/server/server_test.go` by adding tests near existing object tests:

```go
func TestCopyObject_MetadataDirectiveReplace(t *testing.T) {
	backend := storage.NewLocalBackend(t.TempDir())
	base, stop := startTestServer(t, backend)
	defer stop()

	putReq, _ := http.NewRequest(http.MethodPut, base+"/src/original.txt", strings.NewReader("body"))
	putReq.Header.Set("Content-Type", "text/plain")
	resp, err := http.DefaultClient.Do(putReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	copyReq, _ := http.NewRequest(http.MethodPut, base+"/dst/copy.txt", nil)
	copyReq.Header.Set("x-amz-copy-source", "/src/original.txt")
	copyReq.Header.Set("x-amz-metadata-directive", "REPLACE")
	copyReq.Header.Set("Content-Type", "application/octet-stream")
	resp, err = http.DefaultClient.Do(copyReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	headReq, _ := http.NewRequest(http.MethodHead, base+"/dst/copy.txt", nil)
	resp, err = http.DefaultClient.Do(headReq)
	require.NoError(t, err)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	resp.Body.Close()
}
```

- [ ] **Step 5: Commit**

```bash
git add internal/storage/operations_copy.go internal/storage/operations_copy_test.go internal/server/server_test.go
git commit -m "feat: add storage operations copy object"
```

---

## Task 6: Add ServerStorage Wiring

**Files:**
- Modify: `internal/server/server.go`
- Test: `internal/server/server_test.go`

- [ ] **Step 1: Add ServerStorage type and ops field**

Modify `internal/server/server.go` imports to include:

```go
	"github.com/gritive/GrainFS/internal/policy"
```

Add this type above `Server`:

```go
type ServerStorage struct {
	Ops           *storage.Operations
	VolumeBackend storage.Backend
	Snapshotable   storage.Snapshotable
	DBProvider     storage.DBProvider
	Authorizer     *policy.CompiledPolicyStore
}

func NewServerStorage(backend storage.Backend) ServerStorage {
	authorizer := policy.NewCompiledPolicyStore()
	return ServerStorage{
		Ops:           storage.NewOperations(backend, authorizer),
		VolumeBackend: backend,
		Authorizer:    authorizer,
	}
}
```

Add to `Server`:

```go
ops         *storage.Operations
authorizer  *policy.CompiledPolicyStore
```

- [ ] **Step 2: Convert constructor to use ServerStorage**

Keep the public signature stable:

```go
func New(addr string, backend storage.Backend, opts ...Option) *Server {
	return NewWithStorage(addr, NewServerStorage(backend), opts...)
}
```

Add:

```go
func NewWithStorage(addr string, ss ServerStorage, opts ...Option) *Server {
	if ss.Authorizer == nil {
		ss.Authorizer = policy.NewCompiledPolicyStore()
	}
	if ss.Ops == nil {
		ss.Ops = storage.NewOperations(ss.VolumeBackend, ss.Authorizer)
	}
	s := &Server{
		ops:         ss.Ops,
		authorizer:  ss.Authorizer,
		hub:         NewHub(),
		policyStore: ss.Authorizer,
		ipLimiter:   NewRateLimiter(100, 200, 100000),
		userLimiter: NewRateLimiter(50, 100, 100000),
	}
```

Move the existing constructor body into `NewWithStorage`, replacing constructor-only backend uses:

```go
if s.dataDir != "" && ss.Snapshotable != nil {
	dir := filepath.Join(s.dataDir, "snapshots")
	walDir := filepath.Join(s.dataDir, "wal")
	if mgr, err := snapshot.NewManager(dir, ss.Snapshotable, walDir); err == nil {
		s.snapMgr = mgr
	} else {
		log.Warn().Err(err).Msg("snapshot manager init failed, snapshot/PITR endpoints will be unavailable")
	}
}

if s.volMgr == nil {
	s.volMgr = volume.NewManager(ss.VolumeBackend)
}
if s.icebergCatalog == nil && ss.DBProvider != nil {
	s.icebergCatalog = icebergcatalog.NewStore(ss.DBProvider.DB(), "s3://grainfs-tables/warehouse")
}
```

- [ ] **Step 3: Preserve inferred constructor dependencies**

In `NewServerStorage`, infer optional constructor dependencies from the raw backend:

```go
if snap, ok := backend.(storage.Snapshotable); ok {
	ss.Snapshotable = snap
}
if dbp, ok := unwrapBackend(backend).(storage.DBProvider); ok {
	ss.DBProvider = dbp
}
```

Keep `unwrapBackend` in server for constructor inference only until all callers migrate.

- [ ] **Step 4: Verify constructor compile**

Run:

```bash
go test ./internal/server -run 'TestPutGetObject|TestHeadObject|Test.*Iceberg|Test.*Snapshot' -count=1
```

Expected: PASS or a compile failure pointing to remaining `s.backend` field uses. For compile failures, replace handler uses in Task 7.

- [ ] **Step 5: Commit**

```bash
git add internal/server/server.go internal/server/server_test.go
git commit -m "refactor: add explicit server storage wiring"
```

---

## Task 7: Migrate Server Handlers To Operations

**Files:**
- Modify: `internal/server/handlers.go`
- Modify: `internal/server/versioning.go`
- Modify: `internal/server/policy_handlers.go`
- Modify: `internal/server/authz.go`
- Test: `internal/server/versioning_test.go`
- Test: `internal/server/server_test.go`

- [ ] **Step 1: Update mapError for unsupported operations**

In `internal/server/handlers.go`, add this branch near the top of `mapError`:

```go
if errors.Is(err, storage.ErrUnsupportedOperation) {
	writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", err.Error())
	return
}
```

- [ ] **Step 2: Replace object put ACL probing**

In `putObject`, replace the `unwrapBackend(s.backend).(storage.AtomicACLPutter)` block with:

```go
if aclHeader != "" {
	acl := s3auth.ParseACLHeader(aclHeader)
	obj, putErr = s.ops.PutObjectWithACL(ctx, bucket, key, body, contentType, uint8(acl))
} else {
	obj, putErr = s.ops.PutObject(ctx, bucket, key, body, contentType)
}
```

- [ ] **Step 3: Replace versioned GET and HEAD probing**

In `getObject`, replace versioned getter lookup with:

```go
if versionID != "" {
	rc, obj, err = s.ops.GetObjectVersion(bucket, key, versionID)
} else {
	rc, obj, err = s.ops.GetObject(ctx, bucket, key)
}
```

In `headObject`, replace versioned header lookup with:

```go
if versionID != "" {
	obj, err = s.ops.HeadObjectVersion(bucket, key, versionID)
} else {
	obj, err = s.ops.HeadObject(ctx, bucket, key)
}
```

- [ ] **Step 4: Replace delete probing**

In `deleteObject`, replace `findVersionDeleter` and `findVersionedSoftDeleter` uses:

```go
if versionID := string(c.QueryArgs().Peek("versionId")); versionID != "" {
	if err := s.ops.DeleteObjectVersion(bucket, key, versionID); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
	return
}

markerID, err := s.ops.DeleteObjectReturningMarker(bucket, key)
if err != nil {
	mapError(c, err)
	return
}
if markerID != "" {
	c.Header("x-amz-delete-marker", "true")
	c.Header("x-amz-version-id", markerID)
}
```

- [ ] **Step 5: Replace versioning handlers**

In `internal/server/versioning.go`, remove `findBucketVersioner`, `findObjectVersionLister`, `findVersionedSoftDeleter`, and `findVersionDeleter`. Replace calls with:

```go
if err := s.ops.SetBucketVersioning(bucket, vc.Status); err != nil {
	mapError(c, err)
	return
}
```

```go
vs, err := s.ops.ListObjectVersions(bucket, prefix, maxKeys)
```

```go
state, err := s.ops.GetBucketVersioning(bucket)
```

- [ ] **Step 6: Replace policy handlers**

In `internal/server/policy_handlers.go`, replace backend probing with facade calls:

```go
if err := s.ops.SetBucketPolicy(bucket, body); err != nil {
	writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
	return
}
```

```go
data, err := s.ops.GetBucketPolicy(bucket)
if err != nil {
	writeXMLError(c, consts.StatusNotFound, "NoSuchBucketPolicy", "The bucket policy does not exist")
	return
}
```

```go
if err := s.ops.DeleteBucketPolicy(bucket); err != nil {
	writeXMLError(c, consts.StatusInternalServerError, "InternalError", err.Error())
	return
}
```

- [ ] **Step 7: Keep authz hot path on compiled authorizer**

In `internal/server/authz.go`, replace:

```go
if !s.policyStore.Allow(ctx, in) {
```

with:

```go
if !s.authorizer.Allow(ctx, in) {
```

- [ ] **Step 8: Remove dead server probing helpers**

Run:

```bash
rg -n 'unwrapBackend\\(s\\.backend\\)|findBucketVersioner|findObjectVersionLister|findVersionDeleter|findVersionedSoftDeleter|findVersionedHeader|VersionedGetter|VersionedHeader|PolicyBackend' internal/server
```

Expected: only constructor inference may reference `unwrapBackend`; no handler probing remains.

- [ ] **Step 9: Verify server regression tests**

Run:

```bash
go test ./internal/server -run 'TestPutGetObject|TestHeadObject|Test.*Version|TestPutGetDeleteBucketPolicy|TestCopyObject' -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add internal/server/handlers.go internal/server/versioning.go internal/server/policy_handlers.go internal/server/authz.go internal/server/server.go internal/server/server_test.go internal/server/versioning_test.go
git commit -m "refactor: route server storage capabilities through operations"
```

---

## Task 8: Decorator Stack, Recovery Gate, And WAL Contract Tests

**Files:**
- Modify: `internal/storage/recovery_gate.go`
- Test: `internal/storage/operations_test.go`
- Test: `internal/storage/recovery_gate_test.go`
- Test: `internal/cluster/wal_integration_test.go`

- [ ] **Step 1: Consolidate duplicate recovery gate interfaces**

In `internal/storage/recovery_gate.go`, remove local `policyBackend`, `bucketVersioner`, `versionedGetter`, `versionedHeader`, and `objectVersionLister` interface definitions. Use the exported interfaces from `operations.go`.

Replace:

```go
pb, ok := g.Backend.(policyBackend)
```

with:

```go
pb, ok := g.Backend.(BucketPolicyBackend)
```

Apply the same replacement for `BucketVersioner`, `VersionedGetter`, `VersionedHeader`, and `ObjectVersionLister`.

- [ ] **Step 2: Add outer-first operation plan test**

Append to `internal/storage/operations_test.go`:

```go
type outerACLBackend struct {
	Backend
	called bool
}

func (b *outerACLBackend) SetObjectACL(string, string, uint8) error {
	b.called = true
	return nil
}

func TestOperationsPlanUsesOuterFirstAdapter(t *testing.T) {
	inner := &aclOpsBackend{}
	outer := &outerACLBackend{Backend: inner}
	ops := NewOperations(outer, nil)

	require.NoError(t, ops.SetObjectACL("b", "k", 1))
	require.True(t, outer.called)
	require.Empty(t, inner.deleted)
}
```

- [ ] **Step 3: Verify recovery gate still blocks mutators**

Run:

```bash
go test ./internal/storage -run 'TestRecoveryWriteGate|TestOperationsPlanUsesOuterFirstAdapter' -count=1
```

Expected: PASS.

- [ ] **Step 4: Verify WAL integration still records version deletes**

Run:

```bash
go test ./internal/cluster -run 'TestWAL_WrapDistributedBackend_DeleteObjectVersion|TestWAL_WrapDistributedBackend_DeleteObjectReturningMarker' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/recovery_gate.go internal/storage/recovery_gate_test.go internal/storage/operations_test.go internal/cluster/wal_integration_test.go
git commit -m "test: verify operations decorator stack ordering"
```

---

## Task 9: Final Verification And Documentation

**Files:**
- Modify: `docs/architecture/storage-operations-facade.md`
- Modify: `CONTEXT.md`

- [ ] **Step 1: Run focused package tests**

Run:

```bash
go test ./internal/policy ./internal/storage ./internal/server ./internal/cluster -count=1
```

Expected: PASS.

- [ ] **Step 2: Run full test suite**

Run:

```bash
go test ./... -count=1
```

Expected: PASS. If a known slow integration test fails because of environment prerequisites, record the exact failing package, test name, and error in the final handoff.

- [ ] **Step 3: Check handler probing removal**

Run:

```bash
rg -n 'unwrapBackend\\(s\\.backend\\)|findBucketVersioner|findObjectVersionLister|findVersionDeleter|findVersionedSoftDeleter|findVersionedHeader|VersionedGetter|VersionedHeader|PolicyBackend' internal/server
```

Expected output:

```text
internal/server/server.go:<line>: if dbp, ok := unwrapBackend(backend).(storage.DBProvider); ok {
```

If more handler references appear, migrate them to `s.ops` before committing.

- [ ] **Step 4: Check CopyObject streaming contract**

Run:

```bash
go test ./internal/storage -run 'TestOperationsCopyObjectFallbackStreamsSourceToPut' -count=1
```

Expected: PASS.

- [ ] **Step 5: Update architecture status**

In `docs/architecture/storage-operations-facade.md`, change:

```markdown
**Status:** Proposed.
```

to:

```markdown
**Status:** Implemented.
```

Only make this change after all verification commands above pass.

- [ ] **Step 6: Commit**

```bash
git add docs/architecture/storage-operations-facade.md CONTEXT.md
git commit -m "docs: mark storage operations facade implemented"
```

---

## Failure Modes To Watch

- Recovery write gate bypass: a mutating fallback calls an inner backend directly. Covered by operations spy decorator tests.
- Cache stale read after version delete: facade calls hard-delete without outer cache invalidation. Covered by WAL/cache decorated stack tests.
- ACL rollback deletes the wrong version: rollback uses latest-version lookup. Prevented by using only `PutObject` returned `VersionID`.
- CopyObject memory regression: fallback buffers source body. Covered by streaming fallback test.
- Policy authz slowdown: middleware reads backend policy on every request. Prevented by `policy.Authorizer` hot path.
- Dynamic backend stale plan: `SwappableBackend` changes capabilities after `Operations` construction. Add generation refresh before enabling operations over swappable production chains.

## Parallelization Strategy

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Policy package move | `internal/policy`, `internal/server` | none |
| Operations core | `internal/storage` | none |
| ACL operations | `internal/storage` | Operations core |
| Versioning/policy operations | `internal/storage`, `internal/policy` | Policy package move, Operations core |
| CopyObject operations | `internal/storage`, `internal/server` | Operations core, ACL operations |
| Server wiring and migration | `internal/server` | Operations family tasks |
| Decorator contract tests | `internal/storage`, `internal/cluster` | Operations family tasks |
| Final verification | all touched modules | all implementation tasks |

Lane A: Policy package move.
Lane B: Operations core -> ACL -> versioning/policy -> CopyObject.
Lane C: Server wiring waits for Lane B, then server handler migration.
Lane D: Decorator/WAL tests wait for Lane B and can run partly in parallel with Lane C.

Execution order: launch Lane A and the first step of Lane B in parallel worktrees. Merge both. Continue Lane B. Then run Lane C and Lane D with careful coordination because both touch storage tests. Final verification runs after all lanes merge.

## Self-Review

Spec coverage: The plan maps every operation listed in `docs/architecture/storage-operations-facade.md` to either an `Operations` method or a server migration step. Policy package movement, typed unsupported errors, ACL rollback, CopyObject request semantics, ServerStorage wiring, capability planning, and test artifacts are all covered.

Placeholder scan: The plan contains no deferred implementation markers. Every code-changing step names exact files, exact symbols, exact test commands, and expected outcomes.

Type consistency: `storage.Operations`, `CopyObjectRequest`, `CopyObjectResult`, `UnsupportedOperationError`, `UnsupportedReason`, `ServerStorage`, and `policy.CompiledPolicyStore` are introduced before later tasks use them.
