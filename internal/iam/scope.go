package iam

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Typed errors emitted by NormalizeScope. Admin handlers map these to
// HTTP 422 with a descriptive body.
var (
	ErrScopeEmptyEntry = errors.New("scope contains empty/whitespace entry")
	ErrScopeSentinel   = errors.New("scope contains reserved sentinel (* or __system__)")
)

// NormalizeScope returns a sorted, deduplicated, validated copy of the input.
// nil/empty → nil (unrestricted). Wildcard "*" or the internal sentinel "__system__"
// in the list → ErrScopeSentinel. Empty/whitespace-only entries →
// ErrScopeEmptyEntry. Otherwise sorted ascending, no duplicates.
func NormalizeScope(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, b := range in {
		trimmed := strings.TrimSpace(b)
		if trimmed == "" {
			return nil, fmt.Errorf("%w: %q", ErrScopeEmptyEntry, b)
		}
		if trimmed == "*" || trimmed == "__system__" {
			return nil, fmt.Errorf("%w: %q", ErrScopeSentinel, trimmed)
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	slices.Sort(out)
	return out, nil
}

// ScopeAllows reports whether the AccessKey scope permits the given bucket.
// Empty/nil scope = unrestricted (legacy backward-compat).
func ScopeAllows(scope []string, bucket string) bool {
	if len(scope) == 0 {
		return true
	}
	return slices.Contains(scope, bucket)
}
