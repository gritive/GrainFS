package lifecycle

import (
	"encoding/xml"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/tagging"
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
// except through the And container; Validate enforces that.
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

func (t Tag) toStorage() storage.Tag {
	return storage.Tag{Key: t.Key, Value: t.Value}
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

// Validate returns an error if cfg contains invalid rules. Hardened to
// MinIO-parity: Expiration field mutex, Filter flat-vs-And exclusivity,
// And criterion count, tag charset + aws: prefix rejection, ObjectSize
// ordering, and AbortIncompleteMultipartUpload bounds.
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
		if err := validateExpiration(r); err != nil {
			return fmt.Errorf("rule %q: %w", r.ID, err)
		}
		if err := validateFilter(r); err != nil {
			return fmt.Errorf("rule %q: %w", r.ID, err)
		}
		if r.AbortIncompleteMultipartUpload != nil &&
			r.AbortIncompleteMultipartUpload.DaysAfterInitiation <= 0 {
			return fmt.Errorf("rule %q: AbortIncompleteMultipartUpload.DaysAfterInitiation must be > 0", r.ID)
		}
	}
	return nil
}

func validateExpiration(r Rule) error {
	exp := r.Expiration
	if exp == nil {
		return nil
	}
	if exp.Days < 0 {
		return errors.New("Expiration: Days must be >= 0")
	}
	hasDays := exp.Days > 0
	hasDate := exp.Date != nil
	hasEODM := exp.ExpiredObjectDeleteMarker != nil && *exp.ExpiredObjectDeleteMarker
	if hasDays && hasDate {
		return errors.New("Expiration: Days and Date cannot be co-set")
	}
	if hasEODM && (hasDays || hasDate) {
		return errors.New("Expiration: ExpiredObjectDeleteMarker cannot be co-set with Days or Date")
	}
	if hasDate {
		d := exp.Date.UTC()
		if !d.Equal(d.Truncate(24 * time.Hour)) {
			return errors.New("Expiration: Date must equal UTC midnight")
		}
	}
	return nil
}

func validateFilter(r Rule) error {
	f := r.Filter
	if f == nil {
		return nil
	}
	hasFlat := f.Prefix != "" || f.Tag != nil ||
		f.ObjectSizeGreaterThan != nil || f.ObjectSizeLessThan != nil
	if hasFlat && f.And != nil {
		return errors.New("Filter: flat fields and And cannot be co-set")
	}
	if f.Tag != nil {
		if err := validateTag(f.Tag); err != nil {
			return err
		}
	}
	if f.ObjectSizeGreaterThan != nil && f.ObjectSizeLessThan != nil {
		if *f.ObjectSizeGreaterThan >= *f.ObjectSizeLessThan {
			return errors.New("Filter: ObjectSizeGreaterThan must be < ObjectSizeLessThan")
		}
	}
	if f.And != nil {
		if err := validateAnd(f.And); err != nil {
			return err
		}
	}
	return nil
}

func validateAnd(a *AndFilter) error {
	criteria := 0
	if a.Prefix != "" {
		criteria++
	}
	if len(a.Tags) > 0 {
		criteria++
	}
	if a.ObjectSizeGreaterThan != nil {
		criteria++
	}
	if a.ObjectSizeLessThan != nil {
		criteria++
	}
	if criteria < 2 {
		return errors.New("Filter.And requires at least two criteria")
	}
	for i := range a.Tags {
		if err := validateTag(&a.Tags[i]); err != nil {
			return err
		}
	}
	if a.ObjectSizeGreaterThan != nil && a.ObjectSizeLessThan != nil &&
		*a.ObjectSizeGreaterThan >= *a.ObjectSizeLessThan {
		return errors.New("Filter.And: ObjectSizeGreaterThan must be < ObjectSizeLessThan")
	}
	return nil
}

func validateTag(t *Tag) error {
	if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
		return fmt.Errorf("tag key with reserved 'aws:' prefix: %q", t.Key)
	}
	return tagging.Validate([]storage.Tag{t.toStorage()})
}
