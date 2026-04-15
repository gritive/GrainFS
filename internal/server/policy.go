package server

import (
	"encoding/json"
	"fmt"
	"strings"
)

// BucketPolicy represents an S3-compatible bucket policy.
type BucketPolicy struct {
	Version   string            `json:"Version"`
	Statement []PolicyStatement `json:"Statement"`
}

// PolicyStatement is a single statement in a bucket policy.
type PolicyStatement struct {
	Effect    string          `json:"Effect"`
	Principal PolicyPrincipal `json:"Principal"`
	Action    []string        `json:"Action"`
	Resource  []string        `json:"Resource"`
}

// PolicyPrincipal represents the principal in a policy statement.
// It can be "*" (all) or {"AWS": ["user1", "user2"]}.
type PolicyPrincipal struct {
	AWS []string
}

func (p *PolicyPrincipal) UnmarshalJSON(data []byte) error {
	// Try string form first: "*"
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		p.AWS = []string{s}
		return nil
	}

	// Try object form: {"AWS": ["user1"]} or {"AWS": "user1"}
	var obj struct {
		AWS json.RawMessage `json:"AWS"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("invalid principal: %w", err)
	}

	// AWS can be a string or array
	var arr []string
	if err := json.Unmarshal(obj.AWS, &arr); err == nil {
		p.AWS = arr
		return nil
	}
	var single string
	if err := json.Unmarshal(obj.AWS, &single); err == nil {
		p.AWS = []string{single}
		return nil
	}
	return fmt.Errorf("invalid principal AWS field")
}

// ParsePolicy parses a JSON bucket policy.
func ParsePolicy(data []byte) (*BucketPolicy, error) {
	var p BucketPolicy
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parse policy: %w", err)
	}
	return &p, nil
}

// IsAllowed evaluates whether the given access key is allowed to perform
// the action on the resource. Deny always overrides Allow.
// Returns false (default deny) if no statement matches.
func (p *BucketPolicy) IsAllowed(accessKey, action, bucket, key string) bool {
	if len(p.Statement) == 0 {
		return false
	}

	resource := "arn:aws:s3:::" + bucket
	if key != "" {
		resource = "arn:aws:s3:::" + bucket + "/" + key
	}

	hasAllow := false
	for _, stmt := range p.Statement {
		if !matchPrincipal(stmt.Principal, accessKey) {
			continue
		}
		if !matchAction(stmt.Action, action) {
			continue
		}
		if !matchResource(stmt.Resource, resource) {
			continue
		}

		if stmt.Effect == "Deny" {
			return false
		}
		if stmt.Effect == "Allow" {
			hasAllow = true
		}
	}
	return hasAllow
}

func matchPrincipal(p PolicyPrincipal, accessKey string) bool {
	for _, a := range p.AWS {
		if a == "*" || a == accessKey {
			return true
		}
	}
	return false
}

func matchAction(actions []string, action string) bool {
	for _, a := range actions {
		if a == action {
			return true
		}
		// Wildcard: s3:* matches any s3 action
		if strings.HasSuffix(a, ":*") {
			prefix := strings.TrimSuffix(a, "*")
			if strings.HasPrefix(action, prefix) {
				return true
			}
		}
	}
	return false
}

func matchResource(resources []string, resource string) bool {
	for _, r := range resources {
		if r == resource {
			return true
		}
		// Wildcard: arn:aws:s3:::bucket/* matches any key in bucket
		if strings.HasSuffix(r, "/*") {
			prefix := strings.TrimSuffix(r, "/*")
			if resource == prefix || strings.HasPrefix(resource, prefix+"/") {
				return true
			}
		}
	}
	return false
}
