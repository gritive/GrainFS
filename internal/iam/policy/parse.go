package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrUnsupportedField = errors.New("unsupported policy field")
	ErrInvalidEffect    = errors.New("invalid Effect (must be Allow or Deny)")
	ErrInvalidAction    = errors.New("invalid Action (unknown namespace or syntax)")
	ErrMalformedARN     = errors.New("malformed ARN")
	ErrInvalidCondition = errors.New("invalid Condition operator or key")
)

var allowedActionNamespaces = map[string]bool{"s3": true, "iceberg": true}

var allowedCondKeys = map[string]string{
	"aws:SourceIp": "IpAddress|NotIpAddress",
	"s3:prefix":    "StringEquals|StringLike",
}

var allowedCondOps = map[string]map[string]bool{
	"aws:SourceIp": {"IpAddress": true, "NotIpAddress": true},
	"s3:prefix":    {"StringEquals": true, "StringLike": true},
}

func (s *StringOrSlice) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		*s = []string{x}
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, e := range x {
			str, ok := e.(string)
			if !ok {
				return fmt.Errorf("expected string in array, got %T", e)
			}
			out = append(out, str)
		}
		*s = out
	default:
		return fmt.Errorf("expected string or []string, got %T", v)
	}
	return nil
}

func (m *StringOrMap) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		m.Star = s == "*"
		return nil
	}
	named := map[string][]string{}
	if err := json.Unmarshal(b, &named); err != nil {
		return err
	}
	m.Named = named
	return nil
}

// Parse accepts a JSON policy document and rejects unsupported features at parse time.
func Parse(raw []byte) (*Document, error) {
	for _, banned := range []string{`"NotAction"`, `"NotResource"`, `"NotPrincipal"`} {
		if strings.Contains(string(raw), banned) {
			return nil, fmt.Errorf("%w: %s", ErrUnsupportedField, banned)
		}
	}
	var doc Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse policy: %w", err)
	}
	if doc.Version != "" && doc.Version != "2012-10-17" {
		return nil, fmt.Errorf("unsupported Version %q (only 2012-10-17 or absent)", doc.Version)
	}
	for i, st := range doc.Statement {
		if st.Effect != EffectAllow && st.Effect != EffectDeny {
			return nil, fmt.Errorf("statement %d: %w (got %q)", i, ErrInvalidEffect, st.Effect)
		}
		for _, a := range st.Action {
			if !validAction(a) {
				return nil, fmt.Errorf("statement %d: %w: %s", i, ErrInvalidAction, a)
			}
		}
		for _, r := range st.Resource {
			if !validARN(r) {
				return nil, fmt.Errorf("statement %d: %w: %s", i, ErrMalformedARN, r)
			}
		}
		for op, kv := range st.Condition {
			for key := range kv {
				allowed, ok := allowedCondOps[key]
				if !ok {
					return nil, fmt.Errorf("statement %d: %w: condition key %q (supported: aws:SourceIp, s3:prefix)", i, ErrInvalidCondition, key)
				}
				if !allowed[op] {
					return nil, fmt.Errorf("statement %d: %w: operator %q for key %q (supported: %s)", i, ErrInvalidCondition, op, key, allowedCondKeys[key])
				}
			}
		}
	}
	return &doc, nil
}

func validAction(a string) bool {
	if a == "*" {
		return true
	}
	parts := strings.SplitN(a, ":", 2)
	if len(parts) != 2 {
		return false
	}
	return allowedActionNamespaces[parts[0]]
}

func validARN(r string) bool {
	if r == "*" {
		return true
	}
	if !strings.HasPrefix(r, "arn:aws:s3:::") {
		return false
	}
	rest := strings.TrimPrefix(r, "arn:aws:s3:::")
	return rest != ""
}
