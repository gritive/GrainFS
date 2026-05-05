package resourcewatch

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// VlogProviderOptions configures VlogProvider. Registry nil → Default.
type VlogProviderOptions struct {
	DataDir  string
	Registry *Registry
}

// VlogProvider implements Provider over registered BadgerDB handles. It sums
// db.Size().vlog across categories and reports disk capacity (Bavail + vlog)
// as Limit so Detector's ratio is workload-invariant (D4=B).
type VlogProvider struct {
	dataDir  string
	registry *Registry
}

// NewVlogProvider constructs a VlogProvider. Registry nil → Default.
func NewVlogProvider(opts VlogProviderOptions) *VlogProvider {
	r := opts.Registry
	if r == nil {
		r = Default
	}
	return &VlogProvider{dataDir: opts.DataDir, registry: r}
}

// Snapshot returns the current vlog sample. Iterates Registry.Snapshot()
// without holding any registry lock so GC duration cannot block Register or
// Deregister (Arch #1 plan-eng-review).
func (p *VlogProvider) Snapshot(ctx context.Context) (Sample, error) {
	_ = ctx
	var totalVlog int64
	cats := make(map[Category]int64)
	for _, e := range p.registry.Snapshot() {
		_, vlog := e.DB.Size()
		cats[e.Category] += vlog
		totalVlog += vlog
	}

	var stat unix.Statfs_t
	if err := unix.Statfs(p.dataDir, &stat); err != nil {
		return Sample{}, fmt.Errorf("vlog provider statfs %s: %w", p.dataDir, err)
	}
	bavail := int64(stat.Bavail) * int64(stat.Bsize)
	limit := bavail + totalVlog

	catsInt := make(map[Category]int, len(cats))
	for k, v := range cats {
		catsInt[k] = int(v)
	}
	return Sample{
		Open:        int(totalVlog),
		Limit:       int(limit),
		Categories:  catsInt,
		CollectedAt: time.Now().UTC(),
	}, nil
}
