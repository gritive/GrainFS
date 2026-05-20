package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// ScanObjects implements lifecycle.Scrubbable's legacy scrubber-compatible
// method. Lifecycle no longer calls this path, but the full interface still
// requires it.
func (c *ClusterCoordinator) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	if c.router == nil || c.groups == nil {
		scan, ok := c.base.(interface {
			ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error)
		})
		if !ok {
			return nil, ErrCoordinatorNoRouter
		}
		return scan.ScanObjects(bucket)
	}
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return nil, err
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(context.Background(), target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ScanObjects(bucket)
	}
	out := make(chan scrubber.ObjectRecord)
	close(out)
	return out, nil
}
