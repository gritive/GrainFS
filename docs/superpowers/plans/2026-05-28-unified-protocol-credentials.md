# Unified Protocol Credentials Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the foundation for unified IAM-backed protocol credentials without changing live S3, Iceberg, NFS, 9P, or NBD behavior.

**Architecture:** Implement a focused `internal/protocred` package that creates, hashes, lists, rotates, revokes, and renders protocol-specific connection hints. Wire it to trusted admin UDS routes and `grainfs credential` CLI commands. Leave protocol servers as-is; enforcement is explicitly deferred.

**Tech Stack:** Go, Cobra, GrainFS admin HTTP transport, existing admin handler wrappers, existing IAM policy validation package.

---

## File Structure

- Create `internal/protocred/types.go`: protocol, resource, mode, credential, secret, connection hint, and validation types.
- Create `internal/protocred/store.go`: local in-memory store with copy-out reads.
- Create `internal/protocred/service.go`: create/list/get/rotate/revoke and connection hint generation.
- Create `internal/protocred/service_test.go`: domain tests.
- Modify `internal/adminapi/types.go`: credential wire schemas.
- Modify `internal/server/admin/dependencies.go`: `ProtocolCredentialService` interface.
- Modify `internal/server/admin/types.go`: add `ProtocolCredentials` dependency and type aliases.
- Create `internal/server/admin/handlers_credentials.go`: transport-agnostic handlers.
- Create `internal/server/admin/hertz_credentials_adapter.go`: route adapters for rotate/revoke param routes.
- Modify `internal/server/admin/route_paths.go` and `internal/server/admin/hertz_routes.go`: register `/v1/credentials`.
- Modify `internal/volumeadmin` only if shared helpers are useful; otherwise create `internal/credentialadmin` to keep the new CLI surface separate from volume admin.
- Create `internal/credentialadmin/{types.go,client.go,ops.go,format.go}`: CLI business logic and HTTP client.
- Create `cmd/grainfs/credential.go`: Cobra command group.
- Modify `cmd/grainfs/main.go` or init wiring only if the existing command pattern requires it.
- Modify `internal/iam/policy/cross_namespace.go` and tests: keep MountSA namespace strict and reject credential management actions for MountSA attach.
- Modify `docs/users/guide.md`, `docs/users/nfs-mount-quickstart.md`, `docs/reference/nbd-compatibility.md`, and `TODOS.md`: document foundation and follow-ups.

## Task 1: Protocol Credential Domain

**Files:**
- Create: `internal/protocred/types.go`
- Create: `internal/protocred/store.go`
- Create: `internal/protocred/service.go`
- Test: `internal/protocred/service_test.go`

- [ ] **Step 1: Write failing domain tests**

Create `internal/protocred/service_test.go` with these tests:

```go
package protocred

import (
	"strings"
	"testing"
	"time"
)

func TestServiceCreateGetListAndHints(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	secret, err := svc.Create(CreateRequest{
		SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if secret.ID == "" || secret.Secret == "" {
		t.Fatalf("secret missing fields: %+v", secret)
	}
	if got := secret.ConnectionHint["export_name"]; !strings.HasPrefix(got, "devdisk@") {
		t.Fatalf("export hint = %q, want devdisk@...", got)
	}

	item, err := svc.Get(secret.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if item.SecretHint == "" || strings.Contains(item.SecretHint, secret.Secret) {
		t.Fatalf("unsafe secret hint: item=%+v secret=%q", item, secret.Secret)
	}
	if item.SAID != "node-a" || item.Protocol != ProtocolNBD || item.Resource != "volume/devdisk" || item.Mode != ModeRW {
		t.Fatalf("item mismatch: %+v", item)
	}

	items := svc.List(ListFilter{SAID: "node-a", Protocol: ProtocolNBD})
	if len(items) != 1 || items[0].ID != secret.ID {
		t.Fatalf("List = %+v, want one item %s", items, secret.ID)
	}
}

func TestServiceRotateAndRevoke(t *testing.T) {
	svc := NewService(NewStore())
	first, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRO})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	second, err := svc.Rotate(first.ID)
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("Rotate ID = %q, want same credential ID %q", second.ID, first.ID)
	}
	if second.Secret == first.Secret {
		t.Fatal("Rotate reused secret")
	}
	if err := svc.Revoke(first.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	item, err := svc.Get(first.ID)
	if err != nil {
		t.Fatalf("Get revoked: %v", err)
	}
	if item.RevokedAt == nil {
		t.Fatalf("RevokedAt not set: %+v", item)
	}
	if _, err := svc.Rotate(first.ID); err == nil {
		t.Fatal("Rotate revoked credential succeeded")
	}
}

func TestServiceValidationAndExpiry(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	svc := NewService(NewStore(), WithNow(func() time.Time { return now }))

	if _, err := svc.Create(CreateRequest{SAID: "", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW}); err == nil {
		t.Fatal("Create without SAID succeeded")
	}
	if _, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: "ftp", Resource: "volume/devdisk", Mode: ModeRW}); err == nil {
		t.Fatal("Create with invalid protocol succeeded")
	}
	if _, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "bad", Mode: ModeRW}); err == nil {
		t.Fatal("Create with invalid resource succeeded")
	}
	past := now.Add(-time.Second)
	secret, err := svc.Create(CreateRequest{SAID: "node-a", Protocol: ProtocolNBD, Resource: "volume/devdisk", Mode: ModeRW, ExpiresAt: &past})
	if err != nil {
		t.Fatalf("Create expired: %v", err)
	}
	item, err := svc.Get(secret.ID)
	if err != nil {
		t.Fatalf("Get expired metadata should work: %v", err)
	}
	if item.ExpiresAt == nil {
		t.Fatalf("ExpiresAt not stored: %+v", item)
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
go test ./internal/protocred
```

Expected: build fails because the package does not exist.

- [ ] **Step 3: Implement the domain package**

Implement the types used in the tests:

```go
type Protocol string
const (
	ProtocolS3 Protocol = "s3"
	ProtocolIceberg Protocol = "iceberg"
	ProtocolNFS Protocol = "nfs"
	Protocol9P Protocol = "9p"
	ProtocolNBD Protocol = "nbd"
)

type Mode string
const (
	ModeRO Mode = "ro"
	ModeRW Mode = "rw"
)

type CreateRequest struct {
	SAID string
	Protocol Protocol
	Resource string
	Mode Mode
	ExpiresAt *time.Time
	CreatedBy string
}

type Secret struct {
	ID string
	Secret string
	ConnectionHint map[string]string
}

type Credential struct {
	ID string
	SAID string
	Protocol Protocol
	Resource string
	Mode Mode
	SecretHash [32]byte
	SecretHint string
	CreatedAt time.Time
	CreatedBy string
	ExpiresAt *time.Time
	RevokedAt *time.Time
	LastUsedAt *time.Time
}
```

Use `crypto/rand`, `base64.RawURLEncoding`, and `sha256.Sum256`. Generate IDs as `pc_...` and secrets as `pcsec_...`. Store only hashes. Generate hints:

```go
nbd:     export_name=<volume-name>@<secret>
nfs:     mount_path=<bucket-name>/<credential-id>
9p:      aname=<credential-id>@<bucket-name>
s3:      access_key_id=<credential-id>
iceberg: client_id=<credential-id>
```

- [ ] **Step 4: Verify domain package**

Run:

```bash
go test ./internal/protocred
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/protocred
git commit -m "feat: add protocol credential domain"
```

## Task 2: Admin API Surface

**Files:**
- Modify: `internal/adminapi/types.go`
- Modify: `internal/server/admin/dependencies.go`
- Modify: `internal/server/admin/types.go`
- Create: `internal/server/admin/handlers_credentials.go`
- Create: `internal/server/admin/hertz_credentials_adapter.go`
- Modify: `internal/server/admin/route_paths.go`
- Modify: `internal/server/admin/hertz_routes.go`
- Test: `internal/server/admin/handlers_credentials_test.go`

- [ ] **Step 1: Write failing admin handler tests**

Create tests that call handler functions directly:

```go
func TestCredentialHandlersCreateListGetRotateRevoke(t *testing.T)
func TestCredentialHandlersUnsupportedWhenServiceMissing(t *testing.T)
func TestCredentialHandlersRejectInvalidExpiresAt(t *testing.T)
```

Use a real `protocred.Service` and assert:
- Create returns `secret` and `connection_hint.export_name` for NBD.
- List/Get do not include `secret`.
- Rotate returns a new `secret` for the same credential ID.
- Revoke returns `{revoked:true}`.
- Missing service returns `unsupported`.

- [ ] **Step 2: Run failing admin tests**

Run:

```bash
go test ./internal/server/admin -run Credential
```

Expected: build fails because handlers/types are missing.

- [ ] **Step 3: Add wire types**

Add to `internal/adminapi/types.go`:

```go
type CredentialCreateReq struct {
	SAID string `json:"sa_id"`
	Protocol string `json:"protocol"`
	Resource string `json:"resource"`
	Mode string `json:"mode"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type CredentialResp struct {
	ID string `json:"id"`
	SAID string `json:"sa_id"`
	Protocol string `json:"protocol"`
	Resource string `json:"resource"`
	Mode string `json:"mode"`
	Secret string `json:"secret,omitempty"`
	SecretHint string `json:"secret_hint,omitempty"`
	ConnectionHint map[string]string `json:"connection_hint,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
	RevokedAt string `json:"revoked_at,omitempty"`
	LastUsedAt string `json:"last_used_at,omitempty"`
}

type CredentialListResp struct {
	Credentials []CredentialResp `json:"credentials"`
}

type CredentialRevokeResp struct {
	ID string `json:"id"`
	Revoked bool `json:"revoked"`
}
```

- [ ] **Step 4: Add admin dependency and handlers**

Add interface:

```go
type ProtocolCredentialService interface {
	Create(protocred.CreateRequest) (protocred.Secret, error)
	List(protocred.ListFilter) []protocred.Credential
	Get(id string) (protocred.Credential, error)
	Rotate(id string) (protocred.Secret, error)
	Revoke(id string) error
}
```

Handler functions:

```go
CreateCredential(ctx, deps, req) (CredentialResp, error)
ListCredentials(ctx, deps, filter) (CredentialListResp, error)
GetCredential(ctx, deps, id) (CredentialResp, error)
RotateCredential(ctx, deps, id) (CredentialResp, error)
RevokeCredential(ctx, deps, id) (CredentialRevokeResp, error)
```

Map `protocred.ErrNotFound` to `not_found`, validation errors to `invalid`, revoked rotate to `conflict`.

- [ ] **Step 5: Register routes**

Add:

```text
POST   /v1/credentials
GET    /v1/credentials
GET    /v1/credentials/:id
POST   /v1/credentials/:id/rotate
DELETE /v1/credentials/:id
```

- [ ] **Step 6: Verify admin tests**

Run:

```bash
go test ./internal/server/admin -run Credential
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/adminapi/types.go internal/server/admin
git commit -m "feat: expose protocol credential admin API"
```

## Task 3: Runtime Wiring

**Files:**
- Modify: `internal/serveruntime/boot_state.go`
- Modify: `internal/serveruntime/boot_phases_admin.go`
- Modify: `internal/serveruntime/boot_phases_node_services.go` only if the service is constructed near node services.
- Test: `internal/serveruntime/boot_phases_admin_test.go`

- [ ] **Step 1: Write failing wiring test**

Add a test asserting `admin.Deps.ProtocolCredentials` is non-nil after admin deps construction for a normal runtime config.

- [ ] **Step 2: Run failing test**

Run:

```bash
go test ./internal/serveruntime -run ProtocolCredential
```

Expected: FAIL because no dependency is wired.

- [ ] **Step 3: Wire local service**

Add a `protocolCredentials *protocred.Service` field to `bootState`, initialize it with `protocred.NewService(protocred.NewStore())`, and pass it into `admin.Deps`.

This PR intentionally uses node-local in-memory storage. Add a short code comment:

```go
// Protocol credentials are node-local in the foundation slice. Raft-backed
// persistence and cross-node propagation are follow-up work before enforcement.
```

- [ ] **Step 4: Verify wiring**

Run:

```bash
go test ./internal/serveruntime -run ProtocolCredential
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/serveruntime
git commit -m "feat: wire protocol credential service"
```

## Task 4: CLI Surface

**Files:**
- Create: `internal/credentialadmin/types.go`
- Create: `internal/credentialadmin/client.go`
- Create: `internal/credentialadmin/ops.go`
- Create: `internal/credentialadmin/format.go`
- Create: `internal/credentialadmin/ops_test.go`
- Create: `cmd/grainfs/credential.go`
- Create: `cmd/grainfs/credential_test.go`

- [ ] **Step 1: Write failing CLI business tests**

Test:
- create text output prints credential ID, SA, protocol, resource, mode, and NBD export name.
- create JSON output includes `secret`.
- list output excludes `secret`.
- rotate output includes new `secret`.
- revoke output prints revoked ID.

- [ ] **Step 2: Run failing tests**

Run:

```bash
go test ./internal/credentialadmin ./cmd/grainfs -run Credential
```

Expected: build fails because packages/commands are missing.

- [ ] **Step 3: Implement credentialadmin client**

Use `adminapi.Transport` like `internal/volumeadmin/client.go`:

```go
func (c *Client) CreateCredential(ctx context.Context, req CredentialCreateReq) (CredentialResp, error)
func (c *Client) ListCredentials(ctx context.Context, filter ListOptions) (CredentialListResp, error)
func (c *Client) GetCredential(ctx context.Context, id string) (CredentialResp, error)
func (c *Client) RotateCredential(ctx context.Context, id string) (CredentialResp, error)
func (c *Client) RevokeCredential(ctx context.Context, id string) (CredentialRevokeResp, error)
```

- [ ] **Step 4: Implement Cobra command**

Add:

```bash
grainfs credential create --sa <id> --protocol <protocol> --resource <resource> --mode ro|rw [--expires-at RFC3339]
grainfs credential list [--sa <id>] [--protocol <protocol>]
grainfs credential get <id>
grainfs credential rotate <id>
grainfs credential revoke <id>
```

Follow existing flags:
- `--endpoint`
- `--timeout`
- `--format text|json`

- [ ] **Step 5: Verify CLI tests**

Run:

```bash
go test ./internal/credentialadmin ./cmd/grainfs -run Credential
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/credentialadmin cmd/grainfs
git commit -m "feat: add protocol credential CLI"
```

## Task 5: IAM Namespace and Docs

**Files:**
- Modify: `internal/iam/policy/cross_namespace.go`
- Modify: `internal/iam/policy/cross_namespace_test.go`
- Modify: `docs/users/guide.md`
- Modify: `docs/users/nfs-mount-quickstart.md`
- Modify: `docs/reference/nbd-compatibility.md`
- Modify: `TODOS.md`

- [ ] **Step 1: Write failing IAM namespace tests**

Add tests:

```go
func TestValidateMountSAPolicyAttach_RejectsCredentialActions(t *testing.T)
func TestParse_AcceptsCredentialActions(t *testing.T)
```

Use `grainfs:CredentialCreate` and `grainfs:VolumeAttach`.

- [ ] **Step 2: Run failing tests**

Run:

```bash
go test ./internal/iam/policy -run 'Credential|MountSA'
```

Expected: fail until namespace rules are updated.

- [ ] **Step 3: Update namespace validation**

Keep MountSA attach strict: only `grainfs:NFSMount` and `grainfs:9PAttach` are valid for MountSA policies. Ensure credential and volume actions are rejected there while still parseable as `grainfs:` actions for normal IAM docs.

- [ ] **Step 4: Update docs**

Document:
- unified credential model is foundation-only.
- existing protocol auth remains live.
- `grainfs credential create` is inventory/control-plane groundwork.
- NBD enforcement and MountSA migration are follow-ups.

- [ ] **Step 5: Verify docs/tests**

Run:

```bash
go test ./internal/iam/policy
go test ./docs/reference
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/iam/policy docs TODOS.md
git commit -m "docs: describe unified credential migration"
```

## Task 6: Full Verification

**Files:**
- All touched files.

- [ ] **Step 1: Run focused suite**

Run:

```bash
go test ./internal/protocred ./internal/server/admin ./internal/serveruntime ./internal/credentialadmin ./cmd/grainfs ./internal/iam/policy ./docs/reference
```

Expected: PASS.

- [ ] **Step 2: Build binary**

Run:

```bash
make build
```

Expected: PASS.

- [ ] **Step 3: Run full Go tests**

Run:

```bash
go test ./...
```

Expected: PASS after `make build`, or failures only in environment-dependent suites with a clear note.

## Self-Review

- Spec coverage: tasks implement the foundation domain, admin API, CLI, IAM namespace reservation, docs, and explicitly avoid protocol enforcement.
- Placeholder scan: no open implementation placeholders remain.
- Type consistency: `protocred.Protocol`, `Mode`, `CreateRequest`, `Secret`, `Credential`, and `ListFilter` are used consistently across domain, admin, and CLI tasks.
