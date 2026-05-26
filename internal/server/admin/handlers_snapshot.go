package admin

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/volume"
)

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
