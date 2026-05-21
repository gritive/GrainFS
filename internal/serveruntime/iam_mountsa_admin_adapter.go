package serveruntime

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// iamMountSAAdminAdapter bridges mountsastore.Store + MetaRaft.Propose into
// admin.IAMMountSAService. It satisfies the read methods by delegating to the
// in-memory snapshot (no DB read on the hot path).
type iamMountSAAdminAdapter struct {
	store   *mountsastore.Store
	propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

var _ admin.IAMMountSAService = (*iamMountSAAdminAdapter)(nil)

func (a *iamMountSAAdminAdapter) Propose(
	ctx context.Context,
	cmdType clusterpb.MetaCmdType,
	payload []byte,
) error {
	return a.propose(ctx, cmdType, payload)
}

func (a *iamMountSAAdminAdapter) List() []admin.MountSAItem {
	sas := a.store.ListAll()
	out := make([]admin.MountSAItem, len(sas))
	for i, sa := range sas {
		out[i] = mountSAToItem(sa)
	}
	return out
}

func (a *iamMountSAAdminAdapter) Get(name string) (admin.MountSAItem, bool) {
	sa, ok := a.store.Get(name)
	if !ok {
		return admin.MountSAItem{}, false
	}
	return mountSAToItem(sa), true
}

func mountSAToItem(sa mountsastore.MountSA) admin.MountSAItem {
	return admin.MountSAItem{
		Name:       sa.Name,
		NumericUID: sa.NumericUID,
		CreatedAt:  sa.CreatedAt,
		CreatedBy:  sa.CreatedBy,
	}
}
