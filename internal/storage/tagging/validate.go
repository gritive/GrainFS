// Package tagging validates S3 object tag sets to AWS spec strictness.
// Single source of truth used by both the body parser (PutObjectTagging
// XML) and the header parser (x-amz-tagging).
package tagging

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	MaxTags        = 10
	MaxKeyLen      = 128
	MaxValueLen    = 256
	ReservedPrefix = "aws:"
)

var (
	ErrTagCount     = errors.New("tag count exceeds 10")
	ErrTagKeyLen    = errors.New("tag key length out of range")
	ErrTagValueLen  = errors.New("tag value length out of range")
	ErrTagCharset   = errors.New("tag key or value contains disallowed characters")
	ErrReservedTag  = errors.New("tag key with reserved 'aws:' prefix")
	ErrDuplicateTag = errors.New("duplicate tag key")
)

// Validate enforces AWS S3 object-tag rules. Nil/empty slice is valid.
func Validate(tags []storage.Tag) error {
	if len(tags) > MaxTags {
		return fmt.Errorf("%w: got %d", ErrTagCount, len(tags))
	}
	seen := make(map[string]struct{}, len(tags))
	for _, t := range tags {
		if l := len(t.Key); l < 1 || l > MaxKeyLen {
			return fmt.Errorf("%w: %d (key=%q)", ErrTagKeyLen, l, t.Key)
		}
		if l := len(t.Value); l > MaxValueLen {
			return fmt.Errorf("%w: %d (key=%q)", ErrTagValueLen, l, t.Key)
		}
		if strings.HasPrefix(strings.ToLower(t.Key), ReservedPrefix) {
			return fmt.Errorf("%w: %q", ErrReservedTag, t.Key)
		}
		if !allowedRunes(t.Key) {
			return fmt.Errorf("%w: key=%q", ErrTagCharset, t.Key)
		}
		if !allowedRunes(t.Value) {
			return fmt.Errorf("%w: value of key=%q", ErrTagCharset, t.Key)
		}
		if _, dup := seen[t.Key]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateTag, t.Key)
		}
		seen[t.Key] = struct{}{}
	}
	return nil
}

func allowedRunes(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			continue
		}
		switch r {
		case '_', '.', ':', '/', '=', '+', '-', '@':
			continue
		}
		return false
	}
	return true
}
