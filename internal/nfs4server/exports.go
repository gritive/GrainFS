package nfs4server

import (
	"context"
	"hash/fnv"
	"sort"
	"sync/atomic"

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
		return nil
	}
	rows := make(map[string]exportConfig)
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
	}
	s.exports.Store(buildSnap(rows))
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
