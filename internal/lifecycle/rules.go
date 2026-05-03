package lifecycle

import (
	"encoding/xml"
	"errors"
	"fmt"
)

const (
	StatusEnabled  = "Enabled"
	StatusDisabled = "Disabled"
)

// LifecycleConfiguration is the S3-compatible lifecycle config for a bucket.
type LifecycleConfiguration struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// Rule defines a single lifecycle rule.
type Rule struct {
	ID                          string                       `xml:"ID"`
	Status                      string                       `xml:"Status"` // "Enabled" | "Disabled"
	Filter                      *Filter                      `xml:"Filter,omitempty"`
	Expiration                  *Expiration                  `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration *NoncurrentVersionExpiration `xml:"NoncurrentVersionExpiration,omitempty"`
}

// Filter restricts a rule to objects matching a prefix.
// Phase 13 scope: Prefix-only. Tag-based filters are deferred.
type Filter struct {
	Prefix string `xml:"Prefix,omitempty"`
}

// Expiration causes objects older than Days to be deleted.
type Expiration struct {
	Days int `xml:"Days"`
}

// NoncurrentVersionExpiration prunes noncurrent versions.
type NoncurrentVersionExpiration struct {
	NoncurrentDays          int `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions int `xml:"NewerNoncurrentVersions,omitempty"`
}

// Validate returns an error if cfg contains invalid rules.
func Validate(cfg *LifecycleConfiguration) error {
	ids := make(map[string]bool, len(cfg.Rules))
	for _, r := range cfg.Rules {
		if r.ID == "" {
			return errors.New("lifecycle rule ID must not be empty")
		}
		if ids[r.ID] {
			return fmt.Errorf("duplicate lifecycle rule ID: %s", r.ID)
		}
		ids[r.ID] = true
		if r.Status != StatusEnabled && r.Status != StatusDisabled {
			return fmt.Errorf("invalid rule status %q: must be Enabled or Disabled", r.Status)
		}
		if r.Expiration != nil && r.Expiration.Days <= 0 {
			return fmt.Errorf("rule %q: expiration days must be > 0", r.ID)
		}
	}
	return nil
}
