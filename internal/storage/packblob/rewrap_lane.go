package packblob

import (
	"context"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// RewrapPackblobEntriesTotal counts packblob index entries re-encrypted onto the
// active DEK generation by a rewrap sweep, labelled by the active generation.
var RewrapPackblobEntriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_rewrap_packblob_entries_total",
	Help: "Packblob entries re-encrypted onto the active DEK generation.",
}, []string{"active_gen"})

// PackblobRewrapLane is the encrypt.RewrapLane arm for packblob. On a DEK
// rotation it sweeps the live index and migrates every entry not already on the
// active gen. It is migration-only and does not reclaim old blob bytes.
type PackblobRewrapLane struct {
	pb *PackedBackend
}

// NewPackblobRewrapLane returns a lane bound to pb. pb may be nil on
// single-node-only stores where the wiring task nil-gates the lane; RewrapByGen
// is then a no-op.
func NewPackblobRewrapLane(pb *PackedBackend) *PackblobRewrapLane {
	return &PackblobRewrapLane{pb: pb}
}

var _ encrypt.RewrapLane = (*PackblobRewrapLane)(nil)

// Name identifies the lane.
func (l *PackblobRewrapLane) Name() string { return "packblob" }

// RewrapByGen sweeps the live index onto activeGen. oldGen is ignored (SWEEP
// semantics, idempotent). A nil-pb lane is a no-op.
func (l *PackblobRewrapLane) RewrapByGen(ctx context.Context, _ uint32, activeGen uint32) error {
	if l.pb == nil {
		return nil
	}
	n, err := l.pb.RewrapStaleEntries(ctx, activeGen)
	if err != nil {
		return err
	}
	RewrapPackblobEntriesTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)).Add(float64(n))
	return nil
}
