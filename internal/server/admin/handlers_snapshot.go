package admin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/volume"
)

type SnapshotInfo = adminapi.SnapshotInfo
type CreateSnapshotResp = adminapi.CreateSnapshotResp

func CreateSnapshot(ctx context.Context, d *Deps, name string) (CreateSnapshotResp, error) {
	id, err := d.Manager.CreateSnapshot(name)
	if errors.Is(err, volume.ErrNotFound) {
		return CreateSnapshotResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return CreateSnapshotResp{}, NewInternal(err.Error())
	}
	snaps, _ := d.Manager.ListSnapshots(name)
	var blocks int64
	for _, s := range snaps {
		if s.ID == id {
			blocks = s.BlockCount
			break
		}
	}
	return CreateSnapshotResp{ID: id, BlockCount: blocks}, nil
}

func ListSnapshots(ctx context.Context, d *Deps, name string) ([]SnapshotInfo, error) {
	snaps, err := d.Manager.ListSnapshots(name)
	if errors.Is(err, volume.ErrNotFound) {
		return nil, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return nil, NewInternal(err.Error())
	}
	out := make([]SnapshotInfo, len(snaps))
	for i, snap := range snaps {
		out[i] = snapshotToInfo(snap)
	}
	return out, nil
}

func snapshotToInfo(s volume.SnapshotInfo) SnapshotInfo {
	return SnapshotInfo{
		ID:         s.ID,
		CreatedAt:  s.CreatedAt.UTC().Format(time.RFC3339Nano),
		BlockCount: s.BlockCount,
	}
}

func DeleteSnapshot(ctx context.Context, d *Deps, name, snapID string) error {
	if err := d.Manager.DeleteSnapshot(name, snapID); err != nil {
		if errors.Is(err, volume.ErrNotFound) {
			return NewNotFound(fmt.Sprintf("volume %q not found", name))
		}
		// Manager returns plain error for snapshot-not-found.
		return NewNotFound(err.Error())
	}
	return nil
}

func RollbackVolume(ctx context.Context, d *Deps, name, snapID string) error {
	if err := d.Manager.Rollback(name, snapID); err != nil {
		if errors.Is(err, volume.ErrNotFound) {
			return NewNotFound(fmt.Sprintf("volume %q not found", name))
		}
		return NewInternal(err.Error())
	}
	return nil
}

type RecalculateResp = adminapi.RecalculateResp

func RecalculateVolume(ctx context.Context, d *Deps, name string) (RecalculateResp, error) {
	before, after, err := d.Manager.Recalculate(name)
	if errors.Is(err, volume.ErrNotFound) {
		return RecalculateResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return RecalculateResp{}, NewInternal(err.Error())
	}
	return RecalculateResp{Volume: name, Before: before, After: after, Fixed: before != after}, nil
}

type CloneReq = adminapi.CloneReq

func CloneVolume(ctx context.Context, d *Deps, req CloneReq) error {
	if req.Src == "" || req.Dst == "" {
		return NewInvalid("both src and dst required")
	}
	if err := d.Manager.Clone(req.Src, req.Dst); err != nil {
		if errors.Is(err, volume.ErrNotFound) {
			return NewNotFound(err.Error())
		}
		if isAlreadyExists(err) {
			return NewConflict(err.Error(), nil)
		}
		return NewInternal(err.Error())
	}
	return nil
}
