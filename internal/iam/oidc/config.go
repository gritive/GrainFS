package oidc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"
)

type FailurePolicy string

const (
	FailurePolicyStrict FailurePolicy = "strict"
	FailurePolicyGrace  FailurePolicy = "grace"
)

type IssuerConfig struct {
	Name              string        `json:"name"`
	IssuerURL         string        `json:"issuer_url"`
	Audience          string        `json:"audience"`
	JWKSURL           string        `json:"jwks_url"`
	GroupsClaim       string        `json:"groups_claim"`
	GroupPrefix       string        `json:"group_prefix"`
	JWKSFailurePolicy FailurePolicy `json:"jwks_failure_policy"`
	GraceTTL          time.Duration `json:"-"`
	GraceTTLSeconds   int64         `json:"grace_ttl_seconds,omitempty"`
}

func ParseIssuerConfigs(raw []byte) ([]IssuerConfig, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, nil
	}
	var cfgs []IssuerConfig
	if err := json.Unmarshal(raw, &cfgs); err != nil {
		return nil, fmt.Errorf("parse oidc issuer config: %w", err)
	}
	seenNames := make(map[string]struct{}, len(cfgs))
	seenGroupPrefixes := make(map[string]struct{}, len(cfgs))
	for i := range cfgs {
		if cfgs[i].JWKSFailurePolicy == "" {
			cfgs[i].JWKSFailurePolicy = FailurePolicyStrict
		}
		if cfgs[i].GraceTTLSeconds > 0 {
			cfgs[i].GraceTTL = time.Duration(cfgs[i].GraceTTLSeconds) * time.Second
		}
		if err := cfgs[i].Validate(); err != nil {
			return nil, fmt.Errorf("issuer %d: %w", i, err)
		}
		if _, exists := seenNames[cfgs[i].Name]; exists {
			return nil, fmt.Errorf("issuer %d: duplicate name %q", i, cfgs[i].Name)
		}
		seenNames[cfgs[i].Name] = struct{}{}
		if _, exists := seenGroupPrefixes[cfgs[i].GroupPrefix]; exists {
			return nil, fmt.Errorf("issuer %d: duplicate group_prefix %q", i, cfgs[i].GroupPrefix)
		}
		seenGroupPrefixes[cfgs[i].GroupPrefix] = struct{}{}
	}
	return cfgs, nil
}

func (c IssuerConfig) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	if c.IssuerURL == "" {
		return errors.New("issuer_url required")
	}
	if c.Audience == "" {
		return errors.New("audience required")
	}
	if c.JWKSURL == "" {
		return errors.New("jwks_url required")
	}
	if c.GroupsClaim == "" {
		return errors.New("groups_claim required")
	}
	if c.GroupPrefix == "" {
		return errors.New("group_prefix required")
	}
	if err := validateHTTPSURL("issuer_url", c.IssuerURL); err != nil {
		return err
	}
	if err := validateHTTPSURL("jwks_url", c.JWKSURL); err != nil {
		return err
	}
	if !strings.HasPrefix(c.GroupPrefix, "oidc:") {
		return errors.New("group_prefix must start with \"oidc:\"")
	}
	if strings.ContainsFunc(c.GroupPrefix, unicode.IsSpace) {
		return errors.New("group_prefix must not contain whitespace")
	}
	switch c.JWKSFailurePolicy {
	case FailurePolicyStrict:
		return nil
	case FailurePolicyGrace:
		if c.GraceTTL <= 0 {
			return errors.New("grace_ttl_seconds must be positive when jwks_failure_policy is grace")
		}
		return nil
	default:
		return fmt.Errorf("jwks_failure_policy must be strict or grace, got %q", c.JWKSFailurePolicy)
	}
}

func validateHTTPSURL(field, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("%s must be an absolute https URL", field)
	}
	return nil
}

func MapGroups(cfg IssuerConfig, values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if err := validateGroupValue(value); err != nil {
			return nil, err
		}
		out = append(out, cfg.GroupPrefix+value)
	}
	return out, nil
}

func validateGroupValue(value string) error {
	if value == "" {
		return errors.New("group value must not be empty")
	}
	if len(value) > 128 {
		return errors.New("group value exceeds 128 bytes")
	}
	if strings.Contains(value, "..") {
		return fmt.Errorf("invalid group value %q", value)
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '.', '_', ':', '@', '+', '-':
			continue
		default:
			return fmt.Errorf("invalid group value %q", value)
		}
	}
	return nil
}
