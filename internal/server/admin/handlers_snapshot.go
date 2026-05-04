package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/volume"
)

// SnapshotInfo mirrors volume.SnapshotInfo for JSON output.
type SnapshotInfo = volume.SnapshotInfo

// CreateSnapshotResp is returned by CreateSnapshot.
type CreateSnapshotResp struct {
	ID         string `json:"id"`
	BlockCount int64  `json:"block_count"`
}

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
	return snaps, nil
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

// RecalculateResp is returned by RecalculateVolume.
type RecalculateResp struct {
	Volume string `json:"volume"`
	Before int64  `json:"before"`
	After  int64  `json:"after"`
	Fixed  bool   `json:"fixed"`
}

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

// CloneReq is the JSON body for CloneVolume.
type CloneReq struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

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
