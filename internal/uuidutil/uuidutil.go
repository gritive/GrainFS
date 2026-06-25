// Package uuidutil centralizes UUIDv7 generation for time-ordered IDs.
package uuidutil

import "github.com/google/uuid"

// MustNewV7 returns a fresh UUIDv7 string. UUIDv7 is k-sortable by millisecond
// timestamp. It panics only if the crypto/rand entropy source fails, which is
// not expected in practice; callers that need a graceful fallback must not use
// this helper.
func MustNewV7() string {
	return uuid.Must(uuid.NewV7()).String()
}
