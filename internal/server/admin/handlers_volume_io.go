package admin

import (
	"context"
	"fmt"
)

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
