package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/incident"
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
		recentInfos := make([]SnapshotInfo, len(recent))
		for i, snap := range recent {
			recentInfos[i] = snapshotToInfo(snap)
		}
		return DeleteResp{}, NewConflict(
			fmt.Sprintf("volume %q has %d snapshots; refused without --force", name, len(snaps)),
			map[string]any{
				"snapshot_count":  len(snaps),
				"recent":          recentInfos,
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

type ResizeReq = adminapi.ResizeReq
type ResizeResp = adminapi.ResizeResp

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

type StatResp = adminapi.StatResp

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
				if !incidentMatchesVolume(st, name, blockPrefix) {
					continue
				}
				resp.RecentIncidents = append(resp.RecentIncidents, incidentToWireMap(st))
				if len(resp.RecentIncidents) >= 50 {
					break
				}
			}
			vols := []VolumeInfo{resp.Volume}
			replicas := fetchReplicaSummaries(ctx, d, vols)
			annotateVolumeHealth(vols, all, replicas)
			resp.Volume = vols[0]
		} else {
			resp.Volume.Health = "unknown"
			resp.Volume.HealthReasons = []string{"incident_lookup_failed"}
		}
	}
	return resp, nil
}

func incidentToWireMap(st incident.IncidentState) map[string]any {
	var out map[string]any
	buf, err := json.Marshal(st)
	if err != nil {
		return map[string]any{"id": st.ID}
	}
	if err := json.Unmarshal(buf, &out); err != nil {
		return map[string]any{"id": st.ID}
	}
	return out
}

// fetchAndAnnotateHealth fetches active incident state plus per-volume
// replica/EC layout signals and delegates composition to the pure
// annotateVolumeHealth composer in health.go. On incident-fetch error it
// stamps "unknown"/"incident_lookup_failed" without invoking the composer.
// VolumePlacement errors are silent — the composer falls back to incident-only
// health rather than stamping a noisy "replica_lookup_failed" while clusters
// are reconfiguring.
func fetchAndAnnotateHealth(ctx context.Context, d *Deps, vols []VolumeInfo) {
	if d.Incident == nil {
		return
	}
	all, err := d.Incident.List(ctx, 500)
	if err != nil {
		for i := range vols {
			vols[i].Health = "unknown"
			vols[i].HealthReasons = []string{"incident_lookup_failed"}
		}
		return
	}
	replicas := fetchReplicaSummaries(ctx, d, vols)
	annotateVolumeHealth(vols, all, replicas)
}

// fetchReplicaSummaries calls the optional VolumePlacement source and returns
// nil on absence or error. Pulled into its own helper so handlers stay simple
// and the silent-on-error contract is named.
func fetchReplicaSummaries(ctx context.Context, d *Deps, vols []VolumeInfo) map[string]ReplicaLayoutFact {
	if d.VolumePlacement == nil {
		return nil
	}
	names := make([]string, len(vols))
	for i, v := range vols {
		names[i] = v.Name
	}
	replicas, err := d.VolumePlacement.VolumeReplicaSummaries(ctx, names)
	if err != nil {
		return nil
	}
	return replicas
}

// WriteAtVolume writes bytes to a volume at the given offset. Used by debugging
// and end-to-end tests; not exposed via the data-plane S3 surface.
func WriteAtVolume(ctx context.Context, d *Deps, req WriteAtVolumeReq) (WriteAtVolumeResp, error) {
	if req.Name == "" {
		return WriteAtVolumeResp{}, NewInvalid("name required")
	}
	n, err := d.Manager.WriteAt(req.Name, req.Data, req.Offset)
	if err != nil {
		return WriteAtVolumeResp{}, NewInternal(fmt.Sprintf("write-at: %v", err))
	}
	return WriteAtVolumeResp{Bytes: int64(n)}, nil
}

// ReadAtVolume reads bytes from a volume starting at the given offset.
func ReadAtVolume(ctx context.Context, d *Deps, req ReadAtVolumeReq) (ReadAtVolumeResp, error) {
	if req.Name == "" {
		return ReadAtVolumeResp{}, NewInvalid("name required")
	}
	if req.Length <= 0 || req.Length > 64<<20 {
		return ReadAtVolumeResp{}, NewInvalid("length out of range (1..64MiB)")
	}
	buf := make([]byte, req.Length)
	n, err := d.Manager.ReadAt(req.Name, buf, req.Offset)
	if err != nil {
		return ReadAtVolumeResp{}, NewInternal(fmt.Sprintf("read-at: %v", err))
	}
	return ReadAtVolumeResp{Data: buf[:n]}, nil
}
