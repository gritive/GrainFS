package s3auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPhaseConstants(t *testing.T) {
	assert.Equal(t, Phase(0), PhasePreLoad)
	assert.Equal(t, Phase(1), PhasePostLoad)
}

func TestDecision_ZeroValueDeny(t *testing.T) {
	var d Decision
	assert.False(t, d.Allow)
	assert.Empty(t, d.Layer)
	assert.Empty(t, d.Reason)
}

func TestDependencyInterfaces_Compile(t *testing.T) {
	// Compile-time check: nil values of each interface type must be assignable.
	var (
		_ IAMStore          = (IAMStore)(nil)
		_ IAMChecker        = IAMChecker(nil)
		_ PolicyChecker     = (PolicyChecker)(nil)
		_ AuditEmitter      = (AuditEmitter)(nil)
		_ PrincipalResolver = PrincipalResolver(nil)
	)
	// Reference ctx so the import is needed.
	_ = context.Background()
}
