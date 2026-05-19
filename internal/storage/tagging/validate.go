// Package tagging validates S3 object tag sets to AWS spec strictness.
// Single source of truth used by both the body parser (PutObjectTagging
// XML) and the header parser (x-amz-tagging).
package tagging

import (
	"errors"
	"fmt"

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
		if _, dup := seen[t.Key]; dup {
			return fmt.Errorf("%w: %q", ErrDuplicateTag, t.Key)
		}
		seen[t.Key] = struct{}{}
	}
	return nil
}
