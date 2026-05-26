package serveruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// dekReadyBootTimeout is the maximum time WaitDEKReady will block at boot.
// A joiner that cannot install gen-0 within this window fails fast rather
// than deadlocking boot.
const dekReadyBootTimeout = 60 * time.Second

// WaitDEKReady blocks until the keeper's active generation has its material
// installed — i.e. gen-0 (or later) has been installed via raft replay,
// genesis Apply, or snapshot restore. Genesis nodes are ready immediately
// (they generated gen-0). Bounded by ctx: a joiner that never installs
// surfaces ctx.Err() rather than deadlocking boot.
//
// nil keeper → return nil (encryption disabled).
func WaitDEKReady(ctx context.Context, keeper *encrypt.DEKKeeper) error {
	if keeper == nil {
		return nil // encryption disabled — nothing to gate
	}
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if keeper.HasActiveGen() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("WaitDEKReady: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
