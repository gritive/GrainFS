package server

import (
	"context"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// Review-forever Pass 1 I1: ensure auxiliary deny paths (scope-mismatch,
// internal-bucket-deny) stash a Decision on the request context so the audit
// envelope finalizer surfaces the deny reason instead of leaving the row
// silently missing.

func TestAuxDeny_ScopeMismatch_StashesDecision(t *testing.T) {
	// Wire just enough state for authorizeAccessKeyScope to run: iamStore
	// must be non-nil so accessKeyScopeEnforced() returns true, and iamAudit
	// is required by the RecordDeny call inside.
	s := &Server{
		iamStore: iam.NewStore(),
		iamAudit: iam.NewAuditLogger(&captureAuditEmitter{}),
	}
	c := app.NewContext(0)
	// Bucket "reports" is outside the scope ["logs"] → Layer 0 deny.
	ctx := iam.WithPrincipalScope(context.Background(), []string{"logs"})

	allowed := s.authorizeAccessKeyScope(ctx, c, "reports", "obj", s3auth.PutObject)

	require.False(t, allowed)
	v, ok := c.Get(auditAuthzDecisionKey)
	require.True(t, ok, "auditAuthzDecisionKey not set on scope-mismatch deny")
	dec, ok := v.(s3auth.Decision)
	require.True(t, ok, "stashed value is not s3auth.Decision")
	assert.False(t, dec.Allow)
	assert.Equal(t, "key_scope_mismatch", dec.Detail.Reason)
}

func TestAuxDeny_InternalBucket_StashesDecision(t *testing.T) {
	// auditInternalAccessKey="" + bucket=audit.BucketName + remote≠localhost
	// drives the deny branch of authorizeAuditInternalBucket.
	s := &Server{
		iamAudit: iam.NewAuditLogger(&captureAuditEmitter{}),
	}
	c := app.NewContext(0)
	// Bucket name == audit.BucketName ("_grainfs") triggers the deny branch.
	// signedAuditObjectRead requires iamConfigured + non-empty accessKey;
	// with iamStore=nil it returns false, so we land on the deny path.
	res := s.authorizeAuditInternalBucket(context.Background(), c, audit.BucketName, "obj", "GET", s3auth.GetObject)

	require.Equal(t, auditInternalBucketDenied, res)
	v, ok := c.Get(auditAuthzDecisionKey)
	require.True(t, ok, "auditAuthzDecisionKey not set on internal-bucket deny")
	dec, ok := v.(s3auth.Decision)
	require.True(t, ok, "stashed value is not s3auth.Decision")
	assert.False(t, dec.Allow)
	assert.Equal(t, "internal_bucket", dec.Detail.Reason)
}
