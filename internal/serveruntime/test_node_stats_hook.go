//go:build !test_admin_endpoints

package serveruntime

import (
	hzserver "github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/cluster"
)

// registerTestEndpoints is a no-op in production builds.
func registerTestEndpoints(h *hzserver.Hertz, store *cluster.NodeStatsStore) {}
