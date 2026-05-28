# OIDC Federated IAM Slice 5 Plan

## Goal

Wire OIDC bearer-token actors into admin/protocol HTTP request handling so
protocol credential routes can use the typed IAM actor boundary without test-only
context injection. Keep the slice narrow: protocol credential routes only, with
operator documentation and tests that prove allowed and denied federated actors
flow through HTTP into `AuthorizePrincipal`.

Source of truth: `TODOS.md` "OIDC federated IAM Slice 5".

## Non-Goals

- No browser login/session model.
- No broad IAM policy/group admin route authorization conversion.
- No external PDP adapter.
- No NFS/9P/NBD per-operation IAM enforcement beyond existing protocol
  credential admin authorization.

## Tasks

### 1. Add a route-scoped admin OIDC bearer authenticator boundary

- Add a small `admin.ActorAuthenticator` interface that accepts a bearer token
  and returns `principal.Principal`.
- Add credential-route middleware that is mounted only on `/v1/credentials*`:
  - ignores requests without `Authorization: Bearer ...` so existing UDS/CLI
    peercred flows keep working;
  - rejects malformed/invalid bearer tokens with `401`;
  - stores the authenticated principal with `WithActorPrincipal`;
  - never logs or returns the raw bearer token.
- Test:
  - no bearer header preserves existing context;
  - valid bearer injects an OIDC actor;
  - invalid bearer returns `401` before handlers run;
  - lowercase `bearer` is accepted consistently with data-plane bearer parsing.
  - non-credential admin routes are unchanged when a malformed bearer header is
    present, proving the middleware is not global.

### 2. Wire runtime OIDC authenticators into admin deps

- Add an admin deps field for the actor authenticator.
- Add a runtime adapter that tries configured OIDC issuers with the existing
  `internal/iam/oidc.Authenticator` and shared JWKS cache.
- Keep disabled-by-default behavior: no configured issuers means no bearer
  authentication and existing UDS behavior remains unchanged.
- Add unit tests for first-match success, all-issuer failure, and nil/empty
  config behavior.

### 3. Prove all protocol credential HTTP actions use the bearer actor

- Extend protocol credential route tests so all `/v1/credentials*` methods with
  a valid bearer actor reach `AuthorizePrincipal` as `KindOIDC` instead of the
  target service account fallback:
  - `POST /v1/credentials`
  - `GET /v1/credentials`
  - `GET /v1/credentials/:id`
  - `POST /v1/credentials/:id/rotate`
  - `DELETE /v1/credentials/:id`
- Include HTTP denied examples for invalid bearer, authz deny on create, and
  authz deny on rotate/revoke before mutation.
- Keep handler-level fallback tests from Slice 4 intact.

### 4. Update operator docs and follow-up tracking

- Update `docs/operators/oidc-federated-iam.md` with:
  - bearer-token admin/protocol flow;
  - current configured-issuer requirement;
  - allowed/denied audit/debug examples without raw JWTs.
- Mark Slice 5 complete in `TODOS.md`.
- Add the next remaining IAM follow-up for broader admin route adoption or
  external PDP if still out of scope.

## Verification

- `go test ./internal/server/admin ./internal/serveruntime ./internal/iam/oidc ./internal/s3auth -count=1`
- `codex exec review --uncommitted --title "OIDC federated IAM Slice 5"`

## GSTACK REVIEW REPORT

Reviewer: codex plan gate
Passes: 2
Outcome: clean

Findings addressed:
- Scoped bearer middleware explicitly to `/v1/credentials*` so unrelated admin
  endpoints keep their existing behavior.
- Expanded HTTP coverage from create-only to create/list/get/rotate/revoke for
  protocol credentials.

Note: the feature-pipeline advisor hook was unavailable in this Codex session,
so the gate used codex plan review plus local verification instead.
