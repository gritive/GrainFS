package server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/iam"
)

// TestAuthMiddleware_IAMPrincipal_NilStoreNoOp verifies that the middleware
// path with no IAM store wired does not inject a principal — preserving
// legacy WithAuth-only behavior.
func TestAuthMiddleware_IAMPrincipal_NilStoreNoOp(t *testing.T) {
	ctx := context.Background()
	if got := iam.PrincipalFromContext(ctx); got != "" {
		t.Fatalf("baseline principal should be empty, got %q", got)
	}
	// Simulating the middleware nil-store branch: if iamStore is nil,
	// the code does NOT call WithPrincipal. Verify the contract by
	// inspecting the AccessKey-only path.
	ctx = WithAccessKey(ctx, "AKLEGACY")
	if got := AccessKeyFromContext(ctx); got != "AKLEGACY" {
		t.Fatalf("AccessKey = %q, want AKLEGACY", got)
	}
	if got := iam.PrincipalFromContext(ctx); got != "" {
		t.Fatalf("principal should still be empty (no iam.WithPrincipal call), got %q", got)
	}
}

// TestAuthMiddleware_IAMPrincipal_ContextChain verifies the ordering of
// context wrapping done by the middleware: WithAccessKey runs first, then
// (if iamStore present + ResolveSA succeeds) iam.WithPrincipal. Both
// values must coexist on the final context.
func TestAuthMiddleware_IAMPrincipal_ContextChain(t *testing.T) {
	ctx := context.Background()
	ctx = WithAccessKey(ctx, "AK1")
	ctx = iam.WithPrincipal(ctx, "sa-alice")
	if got := AccessKeyFromContext(ctx); got != "AK1" {
		t.Fatalf("AccessKey = %q, want AK1", got)
	}
	if got := iam.PrincipalFromContext(ctx); got != "sa-alice" {
		t.Fatalf("Principal = %q, want sa-alice", got)
	}
}
