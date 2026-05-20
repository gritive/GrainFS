package serveruntime

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// icebergConfigAdapter implements admin.IcebergConfigService by reading from
// an *iam.Store. The store already holds decrypted SecretKey in-memory.
type icebergConfigAdapter struct {
	store *iam.Store
}

var _ admin.IcebergConfigService = (*icebergConfigAdapter)(nil)

// newIcebergConfigAdapter returns a wired adapter, or nil when store is nil.
func newIcebergConfigAdapter(store *iam.Store) admin.IcebergConfigService {
	if store == nil {
		return nil
	}
	return &icebergConfigAdapter{store: store}
}

// RevealSAKeyPair returns the access_key + plaintext secret_key of the
// oldest active AccessKey belonging to saID.
func (a *icebergConfigAdapter) RevealSAKeyPair(_ context.Context, saID string) (string, string, error) {
	keys := a.store.ListKeysForSA(saID)
	for _, k := range keys {
		if k.Status == iam.KeyStatusActive {
			if k.SecretKey == "" {
				return "", "", fmt.Errorf("sa %q has no in-memory plaintext key (DEK not loaded?)", saID)
			}
			return k.AccessKey, k.SecretKey, nil
		}
	}
	if len(keys) == 0 {
		return "", "", fmt.Errorf("service account %q not found or has no keys", saID)
	}
	return "", "", fmt.Errorf("service account %q has no active keys", saID)
}
