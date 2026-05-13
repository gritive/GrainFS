package nfsadmin

import (
	"fmt"
	"strings"
)

func ResolveEndpoint(flagVal string) (string, error) {
	if v := strings.TrimSpace(flagVal); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("admin endpoint not configured.\n" +
		"  Hint:  grainfs nfs export <command> --endpoint <data-dir>/admin.sock")
}
