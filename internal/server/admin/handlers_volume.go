package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/volume"
)

type ListVolumesResp = adminapi.ListVolumesResp

// ListVolumes returns all volumes registered with the manager.
func ListVolumes(ctx context.Context, d *Deps) (ListVolumesResp, error) {
	vs, err := d.Manager.List()
	if err != nil {
		return ListVolumesResp{}, NewInternal("list volumes: " + err.Error())
	}
	out := make([]VolumeInfo, len(vs))
	for i, v := range vs {
		out[i] = toVolumeInfo(v)
	}
	fetchAndAnnotateHealth(ctx, d, out)
	return ListVolumesResp{Volumes: out}, nil
}

type CreateVolumeReq = adminapi.CreateVolumeReq

// CreateVolume creates a new volume. Conflict if name exists, invalid if size <= 0.
func CreateVolume(ctx context.Context, d *Deps, req CreateVolumeReq) (VolumeInfo, error) {
	if req.Name == "" {
		return VolumeInfo{}, NewInvalid("name required")
	}
	if req.Size <= 0 {
		return VolumeInfo{}, NewInvalid(fmt.Sprintf("size must be > 0, got %d", req.Size))
	}
	v, err := d.Manager.Create(req.Name, req.Size)
	if err != nil {
		// Manager returns a plain error with "already exists" text — match by string.
		if errors.Is(err, volume.ErrPoolQuotaExceeded) {
			return VolumeInfo{}, NewConflict(err.Error(), nil)
		}
		// Best-effort detection of duplicate name (Manager.Create returns fmt.Errorf without sentinel).
		if isAlreadyExists(err) {
			return VolumeInfo{}, NewConflict(fmt.Sprintf("volume %q already exists", req.Name), nil)
		}
		return VolumeInfo{}, NewInternal("create: " + err.Error())
	}
	return toVolumeInfo(v), nil
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

// GetVolume returns the metadata for a single volume.
func GetVolume(ctx context.Context, d *Deps, name string) (VolumeInfo, error) {
	v, err := d.Manager.Get(name)
	if errors.Is(err, volume.ErrNotFound) {
		return VolumeInfo{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return VolumeInfo{}, NewInternal(err.Error())
	}
	out := []VolumeInfo{toVolumeInfo(v)}
	fetchAndAnnotateHealth(ctx, d, out)
	return out[0], nil
}

type DeleteResp = adminapi.DeleteResp

// DeleteVolume removes a volume.
func DeleteVolume(ctx context.Context, d *Deps, name string) (DeleteResp, error) {
	if err := d.Manager.Delete(name); err != nil {
		if errors.Is(err, volume.ErrNotFound) {
			return DeleteResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
		}
		return DeleteResp{}, NewInternal(err.Error())
	}
	return DeleteResp{Deleted: true}, nil
}

type ResizeReq = adminapi.ResizeReq
type ResizeResp = adminapi.ResizeResp

// ResizeVolume grows a volume. Shrink returns "unsupported" with a hint to
// create a smaller new volume instead. Equal size is a no-op (changed=false).
func ResizeVolume(ctx context.Context, d *Deps, name string, req ResizeReq) (ResizeResp, error) {
	v, err := d.Manager.Get(name)
	if errors.Is(err, volume.ErrNotFound) {
		return ResizeResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return ResizeResp{}, NewInternal(err.Error())
	}
	old := v.Size
	if err := d.Manager.Resize(name, req.Size); err != nil {
		if errors.Is(err, volume.ErrShrinkNotSupported) {
			return ResizeResp{}, NewUnsupported(
				fmt.Sprintf("volume %q shrink not supported (current=%d, requested=%d)", name, old, req.Size),
				map[string]any{
					"current_size": old,
					"requested":    req.Size,
					"hint":         "create a smaller new volume and copy the data instead",
				},
			)
		}
		if errors.Is(err, volume.ErrInvalidSize) {
			return ResizeResp{}, NewInvalid(err.Error())
		}
		return ResizeResp{}, NewInternal(err.Error())
	}
	return ResizeResp{Name: name, OldSize: old, NewSize: req.Size, Changed: old != req.Size}, nil
}
