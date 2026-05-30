package iceberg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// recordingOAuthAuthorizer records whether its Authorize was consulted, so the
// exemption test can assert the audit-internal short-circuit happens BEFORE the
// (PDP-decorated, in production) base is reached.
type recordingOAuthAuthorizer struct {
	consulted bool
}

func (a *recordingOAuthAuthorizer) Authorize(_ context.Context, _, _ string, _ policy.RequestContext) policy.EvalResult {
	a.consulted = true
	return policy.EvalResult{Decision: policy.DecisionDeny}
}

// TestAuditInternalOAuthBypassesPDP pins the exemption: auditInternalOAuthAuthorizer
// short-circuits Allow for the audit-internal reader BEFORE delegating to base, so
// that issuance for the audit reader is intentionally NOT PDP-consulted (the base,
// once wrapped at boot, IS the PDP-decorated authorizer). A non-audit principal must
// still reach the base.
func TestAuditInternalOAuthBypassesPDP(t *testing.T) {
	base := &recordingOAuthAuthorizer{}
	adapter := auditInternalOAuthAuthorizer{base: base}

	res := adapter.Authorize(context.Background(), audit.SystemSA, audit.Warehouse,
		policy.RequestContext{Action: "iceberg:GetCatalogConfig"})
	require.Equal(t, policy.DecisionAllow, res.Decision)
	require.False(t, base.consulted, "audit-internal OAuth read must NOT reach the PDP-decorated base")

	base.consulted = false
	_ = adapter.Authorize(context.Background(), "other-sa", audit.Warehouse,
		policy.RequestContext{Action: "iceberg:GetCatalogConfig"})
	require.True(t, base.consulted, "non-audit principal must reach the base authorizer")
}
