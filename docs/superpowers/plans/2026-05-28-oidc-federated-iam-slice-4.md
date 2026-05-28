# OIDC Federated IAM Slice 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let selected protocol credential admin operations authorize the actual typed IAM actor, including OIDC principals and their federated groups, while preserving legacy service-account behavior.

**Architecture:** Add `AuthorizePrincipal` beside the existing S3 `Authorize` method and delegate typed principals to `policy.Resolver.EffectivePrincipal`. Add a narrow admin context helper for actor principals; protocol credential handlers prefer that actor and fall back to the target service account for existing UDS/CLI flows. Keep this slice intentionally small: no broad admin bearer-token middleware is introduced here.

**Tech Stack:** Go 1.26, internal IAM policy resolver/evaluator, admin handler unit tests, operator markdown docs.

---

## File Structure

- Modify `internal/s3auth/authorizer.go`: add `AuthorizePrincipal(ctx, principal, bucket, request)` with the same fail-closed guards as `Authorize`.
- Modify `internal/s3auth/authorizer_test.go`: cover OIDC group allow, unsupported principal deny, and service-account compatibility.
- Create `internal/server/admin/actor_principal.go`: context helpers for carrying the already-authenticated actor principal into selected admin handlers.
- Modify `internal/server/admin/dependencies.go`: extend `CredentialAuthorizer` with `AuthorizePrincipal`.
- Modify `internal/server/admin/handlers_credentials.go`: use the actor principal when present and fall back to `principal.ServiceAccount(targetSAID)`.
- Modify `internal/server/admin/handlers_credentials_test.go`: update the stub and verify fallback plus OIDC actor behavior.
- Create `docs/operators/oidc-federated-iam.md`: operator flow and failure-mode notes for federated IAM authorization.
- Modify `TODOS.md`: mark Slice 4 shipped-in-branch and capture remaining middleware/broader-route follow-up.

## Task 1: Add AuthorizePrincipal To The Policy Authorizer

**Files:**
- Modify: `internal/s3auth/authorizer.go`
- Modify: `internal/s3auth/authorizer_test.go`

- [ ] **Step 1: Write failing OIDC and compatibility tests**

Add these tests to `internal/s3auth/authorizer_test.go`:

```go
func TestAuthorizePrincipal_OIDCGroupPolicyAllows(t *testing.T) {
	a, ps, pa, _ := newTestAuthorizer(t, false, false)
	require.NoError(t, ps.Put(context.Background(), "credential-admin", []byte(`{"Statement":[{"Effect":"Allow","Action":"grainfs:CredentialCreate","Resource":"protocol-credential/nbd/volume/devdisk"}]}`), true))
	require.NoError(t, pa.AttachToGroup(context.Background(), "oidc:example:storage-admins", "credential-admin"))

	actor := principal.OIDC("https://idp.example.com/", "alice", "oidc:example:alice", []string{"oidc:example:storage-admins"})
	got := a.AuthorizePrincipal(context.Background(), actor, "", policy.RequestContext{
		Action:   "grainfs:CredentialCreate",
		Resource: "protocol-credential/nbd/volume/devdisk",
	})

	require.Equal(t, policy.DecisionAllow, got.Decision, got.Reason)
	require.NotEmpty(t, got.ConditionContext)
}

func TestAuthorizePrincipal_ServiceAccountMatchesAuthorize(t *testing.T) {
	a, ps, pa, _ := newTestAuthorizer(t, false, false)
	require.NoError(t, ps.Put(context.Background(), "readonly", []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`), true))
	require.NoError(t, pa.AttachToSA(context.Background(), "sa-1", "readonly"))

	req := policy.RequestContext{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x"}
	legacy := a.Authorize(context.Background(), "sa-1", "b", req)
	typed := a.AuthorizePrincipal(context.Background(), principal.ServiceAccount("sa-1"), "b", req)

	require.Equal(t, legacy.Decision, typed.Decision)
	require.Equal(t, legacy.Reason, typed.Reason)
}

func TestAuthorizePrincipal_UnsupportedKindDeny(t *testing.T) {
	a, _, _, _ := newTestAuthorizer(t, false, false)
	got := a.AuthorizePrincipal(context.Background(), principal.Principal{Kind: "unknown", ID: "x"}, "", policy.RequestContext{
		Action:   "grainfs:CredentialCreate",
		Resource: "protocol-credential/nbd/volume/devdisk",
	})

	require.Equal(t, policy.DecisionDeny, got.Decision)
	require.Contains(t, got.Reason, "unsupported principal kind")
	require.NotEmpty(t, got.ConditionContext)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./internal/s3auth -run 'TestAuthorizePrincipal' -count=1
```

Expected: compile failure because `AuthorizePrincipal`, `principal`, and `require` imports are missing.

- [ ] **Step 3: Implement minimal authorizer support**

In `internal/s3auth/authorizer.go`, import `github.com/gritive/GrainFS/internal/iam/principal` and add:

```go
func (a *Authorizer) AuthorizePrincipal(ctx context.Context, p principal.Principal, bucket string, ctxReq policy.RequestContext) policy.EvalResult {
	if p.Kind == principal.KindServiceAccount {
		return a.Authorize(ctx, p.ID, bucket, ctxReq)
	}
	cc := policy.ConditionContextFromRequest(ctxReq)
	if adminUDSOnlyActions[ctxReq.Action] {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "admin-UDS-only action (D#8)", ConditionContext: cc}
	}
	if reservedname.IsInternalBucket(bucket) {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "internal bucket deny (data plane is admin-UDS-only)", ConditionContext: cc}
	}
	in, err := a.resolver.EffectivePrincipal(ctx, p, bucket)
	if err != nil {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "resolver: " + err.Error(), ConditionContext: cc}
	}
	in.Ctx = ctxReq
	allowAnon, _ := a.cfg.GetBool("iam.allow-anonymous-bucket-policy")
	in.AllowAnonBucket = allowAnon
	return policy.Evaluate(in)
}
```

Also add the missing test imports:

```go
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam/principal"
```

- [ ] **Step 4: Run authorizer tests**

Run:

```bash
go test ./internal/s3auth -count=1
```

Expected: PASS.

## Task 2: Thread Typed Actors Through Protocol Credential Admin Authorization

**Files:**
- Create: `internal/server/admin/actor_principal.go`
- Modify: `internal/server/admin/dependencies.go`
- Modify: `internal/server/admin/handlers_credentials.go`
- Modify: `internal/server/admin/handlers_credentials_test.go`

- [ ] **Step 1: Write failing admin actor tests**

Update the credential authorizer stub to record typed principals:

```go
type credentialAuthCall struct {
	principal principal.Principal
	action    string
	resource  string
}

func (s *credentialAuthorizerStub) AuthorizePrincipal(_ context.Context, p principal.Principal, _ string, ctxReq policy.RequestContext) policy.EvalResult {
	s.calls = append(s.calls, credentialAuthCall{principal: p, action: ctxReq.Action, resource: ctxReq.Resource})
	return policy.EvalResult{Decision: s.decision, Reason: s.reason}
}
```

Add this test to `internal/server/admin/handlers_credentials_test.go`:

```go
func TestCredentialHandlersAuthorizeOIDCActorInsteadOfTargetSA(t *testing.T) {
	d := newDeps(t)
	d.ProtocolCredentials = protocred.NewService(protocred.NewStore())
	authz := &credentialAuthorizerStub{decision: policy.DecisionAllow}
	d.ProtocolCredAuthz = authz
	ctx := admin.WithActorPrincipal(context.Background(), principal.OIDC(
		"https://idp.example.com/",
		"alice",
		"oidc:example:alice",
		[]string{"oidc:example:storage-admins"},
	))

	_, err := admin.CreateCredential(ctx, d, admin.CredentialCreateReq{
		SAID: "sa-app", Protocol: "nbd", Resource: "volume/devdisk", Mode: "rw",
	})

	require.NoError(t, err)
	require.Len(t, authz.calls, 1)
	require.Equal(t, principal.KindOIDC, authz.calls[0].principal.Kind)
	require.Equal(t, "oidc:example:alice", authz.calls[0].principal.ID)
	require.Equal(t, []string{"oidc:example:storage-admins"}, authz.calls[0].principal.Groups)
	require.Equal(t, "grainfs:CredentialCreate", authz.calls[0].action)
	require.Equal(t, "protocol-credential/nbd/volume/devdisk", authz.calls[0].resource)
}
```

Existing assertions should change from `saID: "sa-app"` to `principal: principal.ServiceAccount("sa-app")` for fallback behavior.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./internal/server/admin -run 'TestCredentialHandlersAuthorize' -count=1
```

Expected: compile failure because actor helpers and `AuthorizePrincipal` usage do not exist yet.

- [ ] **Step 3: Add actor context helpers**

Create `internal/server/admin/actor_principal.go`:

```go
package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam/principal"
)

type actorPrincipalContextKey struct{}

func WithActorPrincipal(ctx context.Context, p principal.Principal) context.Context {
	if p.Kind == "" || p.ID == "" {
		return ctx
	}
	return context.WithValue(ctx, actorPrincipalContextKey{}, p)
}

func ActorPrincipalFromContext(ctx context.Context) (principal.Principal, bool) {
	p, ok := ctx.Value(actorPrincipalContextKey{}).(principal.Principal)
	if !ok || p.Kind == "" || p.ID == "" {
		return principal.Principal{}, false
	}
	return p, true
}
```

- [ ] **Step 4: Extend interface and authorization helper**

In `internal/server/admin/dependencies.go`, import `internal/iam/principal` and change `CredentialAuthorizer` to:

```go
type CredentialAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
	AuthorizePrincipal(ctx context.Context, p principal.Principal, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}
```

In `internal/server/admin/handlers_credentials.go`, import `internal/iam/principal` and change `authorizeProtocolCredential` to choose the actor:

```go
	actor, ok := ActorPrincipalFromContext(ctx)
	if !ok {
		actor = principal.ServiceAccount(saID)
	}
	result := d.ProtocolCredAuthz.AuthorizePrincipal(ctx, actor, "", policy.RequestContext{
		Action:   action,
		Resource: protocolCredentialPolicyResource(protocol, resource),
	})
```

- [ ] **Step 5: Run admin tests**

Run:

```bash
go test ./internal/server/admin -count=1
```

Expected: PASS.

## Task 3: Document Operator Flow And Capture Remaining Follow-Up

**Files:**
- Create: `docs/operators/oidc-federated-iam.md`
- Modify: `TODOS.md`

- [ ] **Step 1: Add operator documentation**

Create `docs/operators/oidc-federated-iam.md` with sections:

```markdown
# OIDC Federated IAM

## What This Enables

OIDC principals can be evaluated by the same policy engine as service accounts. Protocol credential admin operations now use a typed actor when one is present in request context, so group policies attached to mapped OIDC groups can authorize credential create/read/list/rotate/revoke decisions.

## Operator Flow

1. Configure each issuer with a unique `group_prefix`.
2. Ensure tokens carry the configured subject, audience, and groups claim.
3. Attach policies to mapped group names such as `oidc:example:storage-admins`.
4. Simulate the expected actor and action before granting production access.
5. Audit denied protocol credential operations by checking the policy reason and mapped group names.

## Failure Modes

- Issuer, audience, or key mismatch: token authentication fails before authorization.
- Missing or malformed groups claim: the actor has no mapped group policies.
- Policy attached to the target service account only: federated actors are denied unless their direct principal ID or mapped groups also carry policy.
- Authorizer not configured: protocol credential admin operations fail closed.
- Unsupported actor principal kind: authorization denies with a resolver error.

## Current Boundary

This slice wires the typed actor boundary and protocol credential admin evaluation. A later slice should add HTTP bearer-token middleware for admin/protocol entry points and expand route adoption beyond protocol credential operations.
```

- [ ] **Step 2: Update TODO state**

In `TODOS.md`, change the Slice 4 entry under `## Next` to checked and add a new unchecked follow-up:

```markdown
- [x] **OIDC federated IAM Slice 4**: use `AuthorizePrincipal` in selected
  admin/protocol routes, add OIDC actor support, and document operator flows
  and failure modes. SHIPPED in current branch: `s3auth.Authorizer`
  exposes `AuthorizePrincipal`; protocol credential admin handlers evaluate
  typed actor principals from context and fall back to the target service
  account for existing UDS/CLI flows.

- [ ] **OIDC federated IAM Slice 5**: add admin/protocol HTTP bearer-token
  middleware for OIDC actors, expand `AuthorizePrincipal` adoption to additional
  admin/protocol routes, and add end-to-end denied/allowed audit examples.
```

- [ ] **Step 3: Run focused docs/code verification**

Run:

```bash
gofmt -w internal/s3auth/authorizer.go internal/s3auth/authorizer_test.go internal/server/admin/actor_principal.go internal/server/admin/dependencies.go internal/server/admin/handlers_credentials.go internal/server/admin/handlers_credentials_test.go
go test ./internal/s3auth ./internal/server/admin -count=1
```

Expected: PASS.

## Task 4: Final Verification Before Ship

**Files:**
- All modified files in this plan.

- [ ] **Step 1: Run unit suite**

Run:

```bash
make test-unit
```

Expected: PASS. If this is too slow or blocked by local tooling, run the narrow package tests above and report the limitation before `/ship`.

- [ ] **Step 2: Review diff for scope**

Run:

```bash
git diff --stat
git diff -- internal/s3auth internal/server/admin docs/operators/oidc-federated-iam.md TODOS.md
```

Expected: only the files listed in this plan changed; no generated files or unrelated formatting churn.

- [ ] **Step 3: Ship**

Invoke the ship workflow after the code review gate is clean. The ship phase will handle VERSION/CHANGELOG, commit, push, and PR creation. Stop when the PR is open; do not merge.

## Self-Review

- Spec coverage: the TODO asks for selected admin/protocol route adoption, OIDC actor support, and operator documentation. Tasks 1-3 cover those directly.
- Placeholder scan: no steps rely on TBD behavior; every code change has concrete signatures and commands.
- Type consistency: `principal.Principal` is the typed actor throughout; service-account fallback uses `principal.ServiceAccount(saID)` and production `*s3auth.Authorizer` satisfies the extended admin interface.
