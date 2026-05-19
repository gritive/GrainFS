package oauth

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/iam"
)

// errUnknownOrRevoked is returned by StoreResolver for missing/revoked/expired keys.
var errUnknownOrRevoked = errors.New("unknown or revoked access key")

// StoreResolver adapts an iam.Store to the SAResolver interface.
type StoreResolver struct {
	store *iam.Store
}

// NewStoreResolver wraps an *iam.Store as an SAResolver.
func NewStoreResolver(s *iam.Store) *StoreResolver {
	return &StoreResolver{store: s}
}

// LookupByAccessKey resolves an access_key to (saID, secretKey, nil).
// Returns errUnknownOrRevoked when the key is missing, revoked, or expired.
func (r *StoreResolver) LookupByAccessKey(_ context.Context, accessKey string) (string, []byte, error) {
	k, saID, ok := iam.ResolveSA(r.store, accessKey)
	if !ok {
		return "", nil, errUnknownOrRevoked
	}
	return saID, []byte(k.SecretKey), nil
}
