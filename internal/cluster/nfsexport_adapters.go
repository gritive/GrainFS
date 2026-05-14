package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/nfsexport"
)

type NfsExportProposer struct {
	Propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

func (p *NfsExportProposer) ProposeUpsert(ctx context.Context, bucket string, cfg nfsexport.Config) error {
	payload, err := nfsexport.EncodeUpsertPayload(bucket, cfg)
	if err != nil {
		return err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportUpsert, payload)
}

func (p *NfsExportProposer) ProposeDelete(ctx context.Context, bucket string) error {
	payload, err := nfsexport.EncodeDeletePayload(bucket)
	if err != nil {
		return err
	}
	return p.Propose(ctx, clusterpb.MetaCmdTypeNfsExportDelete, payload)
}
