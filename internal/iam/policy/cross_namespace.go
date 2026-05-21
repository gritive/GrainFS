package policy

import (
	"encoding/json"
	"fmt"
	"strings"
)

// mountActions lists the actions that belong exclusively to the Mount SA
// (NFS/9P) principal namespace.
var mountActions = map[string]bool{
	"grainfs:NFSMount": true,
	"grainfs:9PAttach": true,
}

// extractRawActions decodes only the action fields from a policy JSON document.
// It handles both single-string and array forms without re-running the full
// Parse to avoid duplicate JSON unmarshalling.
func extractRawActions(policyJSON string) ([]string, error) {
	type rawStatement struct {
		Action json.RawMessage `json:"Action"`
	}
	type rawDoc struct {
		Statement []rawStatement `json:"Statement"`
	}
	var doc rawDoc
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil, fmt.Errorf("policy parse: %w", err)
	}
	var out []string
	for _, st := range doc.Statement {
		if st.Action == nil {
			continue
		}
		// Try string form first.
		var s string
		if err := json.Unmarshal(st.Action, &s); err == nil {
			out = append(out, s)
			continue
		}
		// Try array form.
		var arr []string
		if err := json.Unmarshal(st.Action, &arr); err != nil {
			return nil, fmt.Errorf("policy parse: Action must be string or []string")
		}
		out = append(out, arr...)
	}
	return out, nil
}

// isMountAction reports whether a is a grainfs mount-SA action.
func isMountAction(a string) bool {
	return mountActions[a]
}

// isS3OrIcebergAction reports whether a belongs to the s3 or iceberg
// namespace (not a wildcard, not a grainfs action).
func isS3OrIcebergAction(a string) bool {
	return strings.HasPrefix(a, "s3:") || strings.HasPrefix(a, "iceberg:")
}

// ValidateForMountSAAttach returns an error when the policy JSON contains any
// action that is outside the Mount SA namespace (grainfs:NFSMount /
// grainfs:9PAttach). In particular:
//   - "*" is rejected: it is ambiguous and would implicitly grant S3 access
//     through a MountSA principal, breaking namespace isolation.
//   - s3:* / iceberg:* actions are rejected.
func ValidateForMountSAAttach(policyJSON string) error {
	actions, err := extractRawActions(policyJSON)
	if err != nil {
		return err
	}
	for _, a := range actions {
		if a == "*" {
			return fmt.Errorf("policy contains wildcard action %q; "+
				"Mount SA policies must use grainfs:NFSMount or grainfs:9PAttach only", a)
		}
		if isS3OrIcebergAction(a) {
			return fmt.Errorf("policy contains S3/Iceberg action %q; "+
				"Mount SA policies must use grainfs:NFSMount or grainfs:9PAttach only", a)
		}
		// grainfs:* wildcard is also disallowed — future grainfs actions may not
		// be mount-SA actions.
		if strings.HasPrefix(a, "grainfs:") && !isMountAction(a) {
			return fmt.Errorf("policy contains unknown grainfs action %q; "+
				"only grainfs:NFSMount and grainfs:9PAttach are valid for Mount SA policies", a)
		}
	}
	return nil
}

// ValidateForS3SAAttach returns an error when the policy JSON contains any
// Mount SA-only action (grainfs:NFSMount / grainfs:9PAttach). Wildcard "*"
// is allowed because for S3 SAs it is scoped to S3/Iceberg operations by
// the evaluator (MountSA principals are authenticated separately and never
// carry an S3 SA principal tag).
func ValidateForS3SAAttach(policyJSON string) error {
	actions, err := extractRawActions(policyJSON)
	if err != nil {
		return err
	}
	for _, a := range actions {
		if isMountAction(a) {
			return fmt.Errorf("policy contains mount action %q; "+
				"S3/Iceberg SA policies must not include grainfs:NFSMount or grainfs:9PAttach", a)
		}
	}
	return nil
}
