package lifecycle

import (
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// MatchFilter reports whether the given object version satisfies the
// filter. A nil filter matches everything (S3 default). A delete marker
// presents Size=0 and Tags=nil; rule authors targeting delete markers
// typically leave Filter nil and rely on Expiration.ExpiredObjectDeleteMarker.
//
// ObjectSizeGreaterThan / ObjectSizeLessThan are strict: size must be
// strictly greater / less than the bound (AWS S3 spec).
func MatchFilter(v *storage.ObjectVersionRecord, key string, f *Filter) bool {
	if f == nil {
		return true
	}
	if f.And != nil {
		return matchAnd(v, key, f.And)
	}
	if f.Prefix != "" && !strings.HasPrefix(key, f.Prefix) {
		return false
	}
	if f.Tag != nil && !hasTag(v.Tags, *f.Tag) {
		return false
	}
	if f.ObjectSizeGreaterThan != nil && v.Size <= *f.ObjectSizeGreaterThan {
		return false
	}
	if f.ObjectSizeLessThan != nil && v.Size >= *f.ObjectSizeLessThan {
		return false
	}
	return true
}

func matchAnd(v *storage.ObjectVersionRecord, key string, a *AndFilter) bool {
	if a.Prefix != "" && !strings.HasPrefix(key, a.Prefix) {
		return false
	}
	for _, want := range a.Tags {
		if !hasTag(v.Tags, want) {
			return false
		}
	}
	if a.ObjectSizeGreaterThan != nil && v.Size <= *a.ObjectSizeGreaterThan {
		return false
	}
	if a.ObjectSizeLessThan != nil && v.Size >= *a.ObjectSizeLessThan {
		return false
	}
	return true
}

// hasTag returns true if haystack contains a tag equal to needle (both
// Key and Value match).
func hasTag(haystack []storage.Tag, needle Tag) bool {
	for _, t := range haystack {
		if t.Key == needle.Key && t.Value == needle.Value {
			return true
		}
	}
	return false
}
