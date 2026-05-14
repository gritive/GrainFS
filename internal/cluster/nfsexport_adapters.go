package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/nfsexport"
)

type NfsExportProposer struct {
	Propose         func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error)
	ProposeWithGate func(ctx context.Context, plan compat.GatePlan, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error)
	GatePlan        func(operation compat.Operation) (compat.GatePlan, error)
}

func (p *NfsExportProposer) ProposeCreate(ctx context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	if p.GatePlan == nil || p.ProposeWithGate == nil {
		return 0, fmt.Errorf("nfs export proposer: capability gate not wired")
	}
	payload, err := nfsexport.EncodeUpsertPayload(bucket, cfg)
	if err != nil {
		return 0, err
	}
	plan, err := p.GatePlan(compat.OperationNfsExportCreate)
	if err != nil {
		return 0, err
	}
	return p.ProposeWithGate(ctx, plan, clusterpb.MetaCmdTypeNfsExportCreate, payload)
}

func (p *NfsExportProposer) ProposeUpsert(ctx context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	payload, err := nfsexport.EncodeUpsertPayload(bucket, cfg)
	if err != nil {
		return 0, err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportUpsert, payload)
}

func (p *NfsExportProposer) ProposeDelete(ctx context.Context, bucket string) (uint64, error) {
	payload, err := nfsexport.EncodeDeletePayload(bucket)
	if err != nil {
		return 0, err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportDelete, payload)
}

func (p *NfsExportProposer) ProposeBucketDeleteCascade(ctx context.Context, bucket string, force bool) (uint64, error) {
	payload, err := nfsexport.EncodeBucketDeleteCascadePayload(bucket, force)
	if err != nil {
		return 0, err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade, payload)
}
