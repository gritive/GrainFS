package nfsexport

import (
	"context"
	"errors"
	"fmt"
)

var ErrPropagationBarrierRequired = errors.New("nfsexport: propagation barrier required")

type Proposer interface {
	ProposeUpsert(ctx context.Context, bucket string, cfg Config) (uint64, error)
	ProposeDelete(ctx context.Context, bucket string) (uint64, error)
}

type PropagationBarrier interface {
	WaitApplied(ctx context.Context, index uint64) error
}

type ServiceConfig struct {
	Store            *Store
	Proposer         Proposer
	Barrier          PropagationBarrier
	ClusterNodeCount func() int
}

type UpsertParams struct {
	ReadOnly bool
}

type ExportService struct {
	store            *Store
	proposer         Proposer
	barrier          PropagationBarrier
	clusterNodeCount func() int
}

func NewExportService(cfg ServiceConfig) *ExportService {
	s := &ExportService{
		store:            cfg.Store,
		proposer:         cfg.Proposer,
		barrier:          cfg.Barrier,
		clusterNodeCount: cfg.ClusterNodeCount,
	}
	return s
}

func (s *ExportService) Upsert(ctx context.Context, bucket string, p UpsertParams) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if s.store == nil {
		return fmt.Errorf("nfsexport: store not configured")
	}
	if s.proposer == nil {
		return fmt.Errorf("nfsexport: proposer not configured")
	}
	if err := s.ensurePropagationSupported(); err != nil {
		return err
	}
	cfg := Config{ReadOnly: p.ReadOnly}
	idx, err := s.proposer.ProposeUpsert(ctx, bucket, cfg)
	if err != nil {
		return err
	}
	return s.waitApplied(ctx, idx)
}

func (s *ExportService) Delete(ctx context.Context, bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if s.proposer == nil {
		return fmt.Errorf("nfsexport: proposer not configured")
	}
	if err := s.ensurePropagationSupported(); err != nil {
		return err
	}
	idx, err := s.proposer.ProposeDelete(ctx, bucket)
	if err != nil {
		return err
	}
	return s.waitApplied(ctx, idx)
}

func (s *ExportService) Get(bucket string) (Config, bool) {
	if s.store == nil {
		return Config{}, false
	}
	return s.store.Get(bucket)
}

func (s *ExportService) List() []string {
	if s.store == nil {
		return nil
	}
	return s.store.Snapshot().SortedNames()
}

func (s *ExportService) waitApplied(ctx context.Context, index uint64) error {
	if s.barrier == nil {
		return nil
	}
	return s.barrier.WaitApplied(ctx, index)
}

func (s *ExportService) ensurePropagationSupported() error {
	if s.barrier != nil || s.clusterNodeCount == nil {
		return nil
	}
	if s.clusterNodeCount() > 1 {
		return ErrPropagationBarrierRequired
	}
	return nil
}
