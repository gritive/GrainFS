package admin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/volume"
)

// ListVolumesResp is returned by ListVolumes.
type ListVolumesResp struct {
	Volumes []VolumeInfo `json:"volumes"`
}

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
	return ListVolumesResp{Volumes: out}, nil
}

// CreateVolumeReq is the JSON body for CreateVolume.
type CreateVolumeReq struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

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
	return toVolumeInfo(v), nil
}

// DeleteResp is returned by DeleteVolume.
type DeleteResp struct {
	Deleted bool `json:"deleted"`
}

// DeleteVolume removes a volume. With force=false, refuses if snapshots exist
// and returns a conflict containing up to 3 recent snapshots and the cascade
// command (DX4). With force=true, cascades through DeleteWithSnapshots.
func DeleteVolume(ctx context.Context, d *Deps, name string, force bool) (DeleteResp, error) {
	snaps, err := d.Manager.ListSnapshots(name)
	if errors.Is(err, volume.ErrNotFound) {
		return DeleteResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return DeleteResp{}, NewInternal(err.Error())
	}
	if len(snaps) > 0 && !force {
		recent := snaps
		if len(recent) > 3 {
			recent = recent[:3]
		}
		return DeleteResp{}, NewConflict(
			fmt.Sprintf("volume %q has %d snapshots; refused without --force", name, len(snaps)),
			map[string]any{
				"snapshot_count":  len(snaps),
				"recent":          recent,
				"cascade_command": fmt.Sprintf("grainfs volume delete %s --force", name),
				"list_command":    fmt.Sprintf("grainfs volume snapshot list %s", name),
			},
		)
	}
	if force {
		if err := d.Manager.DeleteWithSnapshots(name); err != nil {
			return DeleteResp{}, NewInternal(err.Error())
		}
	} else {
		if err := d.Manager.Delete(name); err != nil {
			return DeleteResp{}, NewInternal(err.Error())
		}
	}
	return DeleteResp{Deleted: true}, nil
}

// ResizeReq is the JSON body for ResizeVolume.
type ResizeReq struct {
	Size int64 `json:"size"`
}

// ResizeResp is returned by ResizeVolume.
type ResizeResp struct {
	Name    string `json:"name"`
	OldSize int64  `json:"old_size"`
	NewSize int64  `json:"new_size"`
	Changed bool   `json:"changed"`
}

// ResizeVolume grows a volume. Shrink returns "unsupported" with a hint to clone
// to a smaller new volume. Equal size is a no-op (changed=false).
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
					"current_size":  old,
					"requested":     req.Size,
					"hint":          "clone to a smaller new volume instead",
					"clone_command": fmt.Sprintf("grainfs volume clone %s <new>", name),
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

// StatResp is returned by StatVolume.
type StatResp struct {
	Volume          VolumeInfo               `json:"volume"`
	RecentIncidents []incident.IncidentState `json:"recent_incidents,omitempty"`
}

// StatVolume returns volume metadata plus recent incidents scoped to this volume.
// Scrub status is intentionally omitted in Phase B (deferred to follow-up since
// volume blocks bypass the EC scrub path; see TODOS.md).
func StatVolume(ctx context.Context, d *Deps, name string) (StatResp, error) {
	v, err := d.Manager.Get(name)
	if errors.Is(err, volume.ErrNotFound) {
		return StatResp{}, NewNotFound(fmt.Sprintf("volume %q not found", name))
	}
	if err != nil {
		return StatResp{}, NewInternal(err.Error())
	}
	resp := StatResp{Volume: toVolumeInfo(v)}
	if d.Incident != nil {
		all, lerr := d.Incident.List(ctx, 500)
		if lerr == nil {
			blockPrefix := volume.BlockKeyPrefix(name)
			for _, st := range all {
				if st.Scope.Bucket != volume.VolumeBucketName {
					continue
				}
				if st.Scope.Key == name || strings.HasPrefix(st.Scope.Key, blockPrefix) {
					resp.RecentIncidents = append(resp.RecentIncidents, st)
					if len(resp.RecentIncidents) >= 50 {
						break
					}
				}
			}
		}
	}
	return resp, nil
}
