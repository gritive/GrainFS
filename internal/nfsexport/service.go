package nfsexport

import (
	"context"
	"fmt"
)

type Proposer interface {
	ProposeUpsert(ctx context.Context, bucket string, cfg Config) error
	ProposeDelete(ctx context.Context, bucket string) error
}

type PropagationBarrier interface {
	WaitApplied(ctx context.Context) error
}

type ServiceConfig struct {
	Store    *Store
	Proposer Proposer
	Barrier  PropagationBarrier
}

type UpsertParams struct {
	ReadOnly bool
}

type ExportService struct {
	store    *Store
	proposer Proposer
	barrier  PropagationBarrier
}

func NewExportService(cfg ServiceConfig) *ExportService {
	s := &ExportService{
		store:    cfg.Store,
		proposer: cfg.Proposer,
		barrier:  cfg.Barrier,
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
	cfg := Config{ReadOnly: p.ReadOnly}
	if err := s.proposer.ProposeUpsert(ctx, bucket, cfg); err != nil {
		return err
	}
	return s.waitApplied(ctx)
}

func (s *ExportService) Delete(ctx context.Context, bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if s.proposer == nil {
		return fmt.Errorf("nfsexport: proposer not configured")
	}
	if err := s.proposer.ProposeDelete(ctx, bucket); err != nil {
		return err
	}
	return s.waitApplied(ctx)
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

func (s *ExportService) waitApplied(ctx context.Context) error {
	if s.barrier == nil {
		return nil
	}
	return s.barrier.WaitApplied(ctx)
}
