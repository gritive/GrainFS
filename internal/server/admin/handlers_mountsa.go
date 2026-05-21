package admin

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/mountsastore"
)

// CreateMountSAReq is the wire body for POST /v1/iam/mount-sa.
type CreateMountSAReq struct {
	Name      string `json:"name"`
	UID       uint32 `json:"uid"`
	CreatedBy string `json:"created_by,omitempty"`
}

// CreateMountSA proposes a MountSACreate command via Raft.
// CreatedAt is stamped by the proposer before Propose; the FSM apply must not
// call time.Now().
func CreateMountSA(ctx context.Context, d *Deps, req CreateMountSAReq) (MountSAItem, error) {
	if d.IAMMountSA == nil {
		return MountSAItem{}, NewInternal("iam mount-sa admin disabled")
	}
	if req.Name == "" {
		return MountSAItem{}, NewInvalid("name is required")
	}
	sa := mountsastore.MountSA{
		Name:       req.Name,
		NumericUID: req.UID,
		CreatedAt:  time.Now().UnixMilli(),
		CreatedBy:  req.CreatedBy,
	}
	payload := mountsastore.EncodeCreatePayload(sa)
	if err := d.IAMMountSA.Propose(ctx, clusterpb.MetaCmdTypeMountSACreate, payload); err != nil {
		return MountSAItem{}, err
	}
	return MountSAItem{
		Name:       sa.Name,
		NumericUID: sa.NumericUID,
		CreatedAt:  sa.CreatedAt,
		CreatedBy:  sa.CreatedBy,
	}, nil
}

// DeleteMountSA proposes a MountSADelete command via Raft.
func DeleteMountSA(ctx context.Context, d *Deps, name string) error {
	if d.IAMMountSA == nil {
		return NewInternal("iam mount-sa admin disabled")
	}
	if name == "" {
		return NewInvalid("name is required")
	}
	payload := mountsastore.EncodeDeletePayload(name)
	return d.IAMMountSA.Propose(ctx, clusterpb.MetaCmdTypeMountSADelete, payload)
}

// ListMountSAs returns all MountSAs from the in-memory snapshot.
func ListMountSAs(_ context.Context, d *Deps) ([]MountSAItem, error) {
	if d.IAMMountSA == nil {
		return nil, NewInternal("iam mount-sa admin disabled")
	}
	items := d.IAMMountSA.List()
	if items == nil {
		items = []MountSAItem{}
	}
	return items, nil
}

// GetMountSA returns the MountSA for name.
func GetMountSA(_ context.Context, d *Deps, name string) (MountSAItem, error) {
	if d.IAMMountSA == nil {
		return MountSAItem{}, NewInternal("iam mount-sa admin disabled")
	}
	if name == "" {
		return MountSAItem{}, NewInvalid("name is required")
	}
	item, ok := d.IAMMountSA.Get(name)
	if !ok {
		return MountSAItem{}, NewNotFound("mount-sa " + name + " not found")
	}
	return item, nil
}
