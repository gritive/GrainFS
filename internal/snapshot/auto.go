package snapshot

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// SnapshotPolicy provides hot-reloadable interval/retain values to AutoSnapshotter.
// Implemented by *cluster.ClusterConfig (via SnapshotInterval / SnapshotRetain).
// Tests pass a fake that returns mutable values.
type SnapshotPolicy interface {
	SnapshotInterval() time.Duration
	SnapshotRetain() int32
}

// AutoSnapshotter creates snapshots at a policy-driven interval and enforces a
// policy-driven retention limit. Both values are re-read every loop iteration,
// so PATCH→effect latency is bounded by the current wait, not the previous one.
type AutoSnapshotter struct {
	mgr    *Manager
	policy SnapshotPolicy
	idle   time.Duration // poll interval when policy.SnapshotInterval()<=0 (disabled); 5s default
	wg     sync.WaitGroup
}

// NewAutoSnapshotter constructs an AutoSnapshotter driven by a hot-reloadable policy.
// idleWhenDisabled is how long the loop sleeps when SnapshotInterval()<=0 before
// re-checking; pass 0 to use the default (5 seconds).
//
// Rationale (eng-review D2): balancer hot-reload uses time.NewTicker + ticker.Reset
// per tick (balancer.go:344-360). That works for balancer because gossip default is
// 30s. AutoSnapshotter default is 1h and disable is a first-class operator action,
// so we use time.After + short idle so PATCH→effect latency is bounded to idle (5s),
// not interval (could be 1h). idle=5s is a polling pattern but the CPU cost is
// negligible (one cluster-config atomic.Pointer load per 5s).
func NewAutoSnapshotter(mgr *Manager, policy SnapshotPolicy, idleWhenDisabled time.Duration) *AutoSnapshotter {
	if idleWhenDisabled <= 0 {
		idleWhenDisabled = 5 * time.Second
	}
	return &AutoSnapshotter{mgr: mgr, policy: policy, idle: idleWhenDisabled}
}

// Start launches the background snapshot loop. It stops when ctx is cancelled.
func (a *AutoSnapshotter) Start(ctx context.Context) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			interval := a.policy.SnapshotInterval()
			wait := interval
			if wait <= 0 {
				wait = a.idle
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			// Re-check after wake: policy may have changed.
			if a.policy.SnapshotInterval() <= 0 {
				continue
			}
			a.takeAndPrune()
		}
	}()
}

// Wait blocks until the background goroutine exits.
func (a *AutoSnapshotter) Wait() {
	a.wg.Wait()
}

func (a *AutoSnapshotter) takeAndPrune() {
	if _, err := a.mgr.Create("auto"); err != nil {
		log.Warn().Err(err).Msg("auto-snapshot failed")
		return
	}
	a.pruneOld(int(a.policy.SnapshotRetain()))
}

// pruneOld deletes auto-created snapshots beyond maxRetain. Manual snapshots
// (Reason != "auto" && Reason != "") are never deleted — the user must remove
// them explicitly via the snapshot API. Legacy snapshots written before the
// Reason field was populated (Reason == "") are treated as auto.
func (a *AutoSnapshotter) pruneOld(maxRetain int) {
	snaps, err := a.mgr.List()
	if err != nil {
		return
	}
	// Filter to auto-created snapshots only
	var autoSnaps []*Snapshot
	for _, s := range snaps {
		if s.Reason == "auto" || s.Reason == "" {
			autoSnaps = append(autoSnaps, s)
		}
	}
	if len(autoSnaps) <= maxRetain {
		return
	}
	// Sort ascending by seq so oldest are first
	sort.Slice(autoSnaps, func(i, j int) bool { return autoSnaps[i].Seq < autoSnaps[j].Seq })
	toDelete := autoSnaps[:len(autoSnaps)-maxRetain]
	for _, s := range toDelete {
		if err := a.mgr.Delete(s.Seq); err != nil {
			log.Warn().Uint64("seq", s.Seq).Err(err).Msg("auto-snapshot prune failed")
		}
	}
}
