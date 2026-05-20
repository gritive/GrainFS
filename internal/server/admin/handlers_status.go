package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// GetStatus returns a single-screen snapshot of cluster/phase/iam/encryption/
// tls/proxy/audit/jwt state. Registered as GET /v1/status.
func GetStatus(ctx context.Context, d *Deps) (adminapi.StatusReport, error) {
	if d.Status == nil {
		return adminapi.StatusReport{}, NewUnavailable("status service not configured")
	}
	return d.Status.Report(), nil
}
