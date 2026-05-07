package volumeadmin

import (
	"fmt"
	"strings"
)

// ResolveEndpoint trims and validates the --endpoint flag value. Empty input
// returns an actionable error; there is no environment-variable fallback.
//
// The returned value is a UDS path (production) or an http(s):// URL (test
// injection only). Callers pass the value to NewClient which dispatches the
// transport on scheme.
func ResolveEndpoint(flagVal string) (string, error) {
	if v := strings.TrimSpace(flagVal); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("admin endpoint not configured.\n" +
		"  Hint:  grainfs <command> --endpoint <data-dir>/admin.sock")
}
