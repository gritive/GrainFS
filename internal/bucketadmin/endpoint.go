package bucketadmin

import (
	"fmt"
	"os"
	"strings"
)

// ResolveEndpoint mirrors cmd/grainfs/admin_helpers.go:adminEndpointFromCmd:
// flag value → GRAINFS_ADMIN_SOCKET env → fail-fast. Rejects http(s):// URLs
// because admin protocol runs over UDS only. Strips an optional "unix:" prefix.
func ResolveEndpoint(raw string) (string, error) {
	ep := strings.TrimSpace(raw)
	if ep == "" {
		ep = strings.TrimSpace(os.Getenv("GRAINFS_ADMIN_SOCKET"))
	}
	if ep == "" {
		return "", fmt.Errorf("admin endpoint not configured.\n" +
			"  Hint: set GRAINFS_ADMIN_SOCKET=<data-dir>/admin.sock or use --endpoint")
	}
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		return "", fmt.Errorf("admin endpoint must be a UDS socket path; got %q.\n"+
			"  Use the admin socket: --endpoint <data-dir>/admin.sock", ep)
	}
	return strings.TrimPrefix(ep, "unix:"), nil
}
