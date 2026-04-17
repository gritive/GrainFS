package snapshot

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// AutoSnapshotter creates snapshots at a fixed interval and enforces a retention limit.
type AutoSnapshotter struct {
	mgr       *Manager
	interval  time.Duration
	maxRetain int
	wg        sync.WaitGroup
}

// NewAutoSnapshotter creates a new AutoSnapshotter.
// interval: how often to take a snapshot.
// maxRetain: maximum number of snapshots to keep (oldest are deleted).
func NewAutoSnapshotter(mgr *Manager, interval time.Duration, maxRetain int) *AutoSnapshotter {
	return &AutoSnapshotter{mgr: mgr, interval: interval, maxRetain: maxRetain}
}

// Start launches the background snapshot loop. It stops when ctx is cancelled.
func (a *AutoSnapshotter) Start(ctx context.Context) {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		ticker := time.NewTicker(a.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				a.takeAndPrune()
			}
		}
	}()
}

// Wait blocks until the background goroutine exits.
func (a *AutoSnapshotter) Wait() {
	a.wg.Wait()
}

func (a *AutoSnapshotter) takeAndPrune() {
	if _, err := a.mgr.Create("auto"); err != nil {
		slog.Warn("auto-snapshot failed", "err", err)
		return
	}
	a.pruneOld()
}

func (a *AutoSnapshotter) pruneOld() {
	snaps, err := a.mgr.List()
	if err != nil || len(snaps) <= a.maxRetain {
		return
	}
	// Sort ascending by seq so oldest are first
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Seq < snaps[j].Seq })
	toDelete := snaps[:len(snaps)-a.maxRetain]
	for _, s := range toDelete {
		if err := a.mgr.Delete(s.Seq); err != nil {
			slog.Warn("auto-snapshot prune failed", "seq", s.Seq, "err", err)
		}
	}
}
