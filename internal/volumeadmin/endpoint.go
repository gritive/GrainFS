package volumeadmin

import (
	"fmt"
	"os"
	"strings"
)

// ResolveEndpoint determines the admin endpoint string from the standard
// sources, in priority order:
//
//  1. flagVal (from --endpoint)
//  2. $GRAINFS_ENDPOINT
//
// Returns an actionable error if neither is configured.
//
// The returned value is a UDS path (production) or an http(s):// URL (test
// injection only). Callers pass the value to NewClient which dispatches the
// transport on scheme.
func ResolveEndpoint(flagVal string) (string, error) {
	if v := strings.TrimSpace(flagVal); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(os.Getenv("GRAINFS_ENDPOINT")); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("admin endpoint not configured.\n" +
		"  Hint:  grainfs <command> --endpoint <data-dir>/admin.sock\n" +
		"         또는 GRAINFS_ENDPOINT=<path> 환경변수 설정")
}
