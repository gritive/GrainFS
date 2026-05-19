package lifecycle

import (
	"encoding/xml"
	"errors"
	"fmt"
	"time"
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
	ID                             string                          `xml:"ID"`
	Status                         string                          `xml:"Status"` // "Enabled" | "Disabled"
	Filter                         *Filter                         `xml:"Filter,omitempty"`
	Expiration                     *Expiration                     `xml:"Expiration,omitempty"`
	NoncurrentVersionExpiration    *NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
}

// Filter restricts a rule to objects matching constraints (prefix, tag, size, or
// composite And of these). Per S3 wire spec these fields are mutually exclusive
// except through the And container; Validate (Task 2) will enforce that.
type Filter struct {
	Prefix                string     `xml:"Prefix,omitempty"`
	Tag                   *Tag       `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan *int64     `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64     `xml:"ObjectSizeLessThan,omitempty"`
	And                   *AndFilter `xml:"And,omitempty"`
}

// AndFilter is the S3 composite filter: all listed predicates must match.
type AndFilter struct {
	Prefix                string `xml:"Prefix,omitempty"`
	Tags                  []Tag  `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan *int64 `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64 `xml:"ObjectSizeLessThan,omitempty"`
}

// Tag mirrors storage.Tag with XML element names matching the S3 wire format.
type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// Expiration causes objects older than Days (or past Date) to be deleted.
// ExpiredObjectDeleteMarker, when true, cleans up dangling delete markers.
type Expiration struct {
	Days                      int        `xml:"Days,omitempty"`
	Date                      *time.Time `xml:"Date,omitempty"`
	ExpiredObjectDeleteMarker *bool      `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

// NoncurrentVersionExpiration prunes noncurrent versions.
type NoncurrentVersionExpiration struct {
	NoncurrentDays          int `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions int `xml:"NewerNoncurrentVersions,omitempty"`
}

// AbortIncompleteMultipartUpload aborts MPU uploads older than
// DaysAfterInitiation days.
type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation int `xml:"DaysAfterInitiation"`
}

// Validate returns an error if cfg contains invalid rules.
// Phase 1 / Task 1 scope: existing ID/Status/Days checks preserved. Full
// hardening (filter exclusivity, tag/prefix validation, date+days mutex,
// AbortIncompleteMultipartUpload bounds, etc.) lands in Task 2.
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
