package policy

import "testing"

func TestMatchAction_ExactAndWildcard(t *testing.T) {
	tests := []struct {
		pattern, req string
		want         bool
	}{
		{"s3:GetObject", "s3:GetObject", true},
		{"s3:GetObject", "s3:PutObject", false},
		{"s3:*", "s3:GetObject", true},
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get*", "s3:PutObject", false},
		{"*", "iceberg:LoadTable", true},
		{"iceberg:*Table", "iceberg:LoadTable", true},
		{"iceberg:*Table", "iceberg:ListTables", false},
	}
	for _, c := range tests {
		if got := matchAction(c.pattern, c.req); got != c.want {
			t.Errorf("matchAction(%q, %q) = %v, want %v", c.pattern, c.req, got, c.want)
		}
	}
}

func TestMatchResource_PrefixBoundary(t *testing.T) {
	// F#10: arn:aws:s3:::analytics/logs/* must NOT match analytics/logsx/secret
	if matchResource("arn:aws:s3:::analytics/logs/*", "arn:aws:s3:::analytics/logsx/secret") {
		t.Fatal("logs/* must not match logsx/secret")
	}
	if !matchResource("arn:aws:s3:::analytics/logs/*", "arn:aws:s3:::analytics/logs/2026-05.json") {
		t.Fatal("logs/* must match logs/2026-05.json")
	}
}

func TestMatchCondition_SourceIpAllow(t *testing.T) {
	cond := map[string]map[string]StringOrSlice{
		"IpAddress": {"aws:SourceIp": []string{"10.0.0.0/8"}},
	}
	if !matchCondition(cond, RequestContext{SourceIP: "10.1.2.3"}) {
		t.Fatal("10.1.2.3 should match 10.0.0.0/8")
	}
	if matchCondition(cond, RequestContext{SourceIP: "192.168.0.1"}) {
		t.Fatal("192.168.0.1 should not match 10.0.0.0/8")
	}
}

func TestMatchCondition_S3PrefixOnListOnly(t *testing.T) {
	cond := map[string]map[string]StringOrSlice{
		"StringLike": {"s3:prefix": []string{"logs/*"}},
	}
	if matchCondition(cond, RequestContext{Action: "s3:GetObject"}) {
		t.Fatal("s3:prefix on GetObject should NOT match (missing key)")
	}
	if !matchCondition(cond, RequestContext{Action: "s3:ListBucket", Prefix: "logs/2026"}) {
		t.Fatal("s3:prefix logs/* should match prefix logs/2026 on ListBucket")
	}
}
