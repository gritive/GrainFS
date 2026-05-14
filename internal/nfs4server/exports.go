package nfs4server

import (
	"context"
	"hash/fnv"
	"sort"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/nfsexport"
)

type exportConfig struct {
	readOnly   bool
	fsidMajor  uint64
	fsidMinor  uint64
	generation uint64
}

type exportSnap struct {
	byBucket    map[string]exportConfig
	sortedNames []string
	verifier    uint64
}

var emptySnap = &exportSnap{byBucket: map[string]exportConfig{}}

type exportSource interface {
	Get(bucket string) (nfsexport.Config, bool)
	List() []string
}

func buildSnap(rows map[string]exportConfig) *exportSnap {
	names := make([]string, 0, len(rows))
	for name := range rows {
		names = append(names, name)
	}
	sort.Strings(names)
	h := fnv.New64a()
	for _, name := range names {
		_, _ = h.Write([]byte(name))
		var gen [8]byte
		g := rows[name].generation
		for i := range gen {
			gen[i] = byte(g >> (8 * i))
		}
		_, _ = h.Write(gen[:])
	}
	copied := make(map[string]exportConfig, len(rows))
	for name, cfg := range rows {
		copied[name] = cfg
	}
	return &exportSnap{byBucket: copied, sortedNames: names, verifier: h.Sum64()}
}

func (s *Server) SetExportSource(src exportSource) {
	s.exportSource = src
	_ = s.RefreshExports(context.Background())
}

func (s *Server) RefreshExports(_ context.Context) error {
	if s.exportSource == nil {
		metrics.NFSExportsTotal.WithLabelValues("ro").Set(0)
		metrics.NFSExportsTotal.WithLabelValues("rw").Set(0)
		return nil
	}
	prev := s.loadExports()
	rows := make(map[string]exportConfig)
	ro, rw := 0, 0
	for _, bucket := range s.exportSource.List() {
		cfg, ok := s.exportSource.Get(bucket)
		if !ok {
			continue
		}
		rows[bucket] = exportConfig{
			readOnly:   cfg.ReadOnly,
			fsidMajor:  cfg.FsidMajor,
			fsidMinor:  cfg.FsidMinor,
			generation: cfg.Generation,
		}
		if cfg.ReadOnly {
			ro++
		} else {
			rw++
		}
	}
	if s.state != nil {
		for bucket, old := range prev.byBucket {
			next, ok := rows[bucket]
			switch {
			case !ok:
				n := s.state.InvalidateForBucket(bucket)
				metrics.NFSRevokedStateIDs.WithLabelValues("export_remove").Add(float64(n))
			case !old.readOnly && next.readOnly:
				n := s.state.InvalidateForBucket(bucket)
				metrics.NFSRevokedStateIDs.WithLabelValues("export_ro_update").Add(float64(n))
			}
		}
	}
	s.exports.Store(buildSnap(rows))
	metrics.NFSExportsTotal.WithLabelValues("ro").Set(float64(ro))
	metrics.NFSExportsTotal.WithLabelValues("rw").Set(float64(rw))
	return nil
}

func (s *Server) loadExports() *exportSnap {
	snap := s.exports.Load()
	if snap == nil {
		return emptySnap
	}
	return snap
}

func (s *Server) SetExportsForTest(snap *exportSnap) {
	s.exports.Store(snap)
}

var _ = atomic.Pointer[exportSnap]{}
