// internal/audit/schema_test.go
package audit

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestS3Event_HasPolicyDecisionFields(t *testing.T) {
	// Compile-time check: struct fields exist with the right types.
	var e S3Event
	e.MatchedPolicyID = "test"
	e.MatchedSID = "stmt-1"
	e.AuthzLatencyUS = 42
	e.ConditionContext = map[string]string{"k": "v"}
	_ = e
}

func TestIcebergSchemaJSON_Includes_T48_Columns(t *testing.T) {
	for _, want := range []string{
		`"name":"matched_policy_id"`,
		`"name":"matched_sid"`,
		`"name":"authz_latency_us"`,
		`"name":"condition_context_json"`,
	} {
		if !strings.Contains(auditIcebergSchemaJSON, want) {
			t.Errorf("auditIcebergSchemaJSON missing %s", want)
		}
	}
}

func TestS3InitialMetadata_LastColumnID_Updated(t *testing.T) {
	// S3InitialMetadata is a printf template; just grep the literal claim that
	// last-column-id is at least 27. Parsing the rendered JSON is brittle
	// (placeholders for table-uuid/location/last-updated-ms).
	re := regexp.MustCompile(`"last-column-id":(\d+)`)
	m := re.FindStringSubmatch(S3InitialMetadata)
	if m == nil {
		t.Fatalf("S3InitialMetadata does not contain last-column-id")
	}
	got, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatalf("last-column-id not an int: %v", err)
	}
	if got < 27 {
		t.Fatalf("last-column-id = %d, want >= 27", got)
	}
}
