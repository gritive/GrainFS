package cluster

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/migration"
)

// MigrationProposer satisfies migration.Proposer over the cluster meta-FSM.
// Mirrors LifecycleProposer.
type MigrationProposer struct {
	Propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

func (p *MigrationProposer) ProposeJobStart(ctx context.Context, bucket string) error {
	return p.Propose(ctx, clusterpb.MetaCmdTypeMigrationJobStart,
		migration.EncodeJobStartPayload(bucket, time.Now().UnixNano()))
}

func (p *MigrationProposer) ProposeJobDone(ctx context.Context, bucket string, copied, errors int64) error {
	return p.Propose(ctx, clusterpb.MetaCmdTypeMigrationJobDone,
		migration.EncodeJobDonePayload(bucket, copied, errors, time.Now().UnixNano()))
}

func (p *MigrationProposer) ProposeJobFailed(ctx context.Context, bucket, reason string, errors int64) error {
	return p.Propose(ctx, clusterpb.MetaCmdTypeMigrationJobFailed,
		migration.EncodeJobFailedPayload(bucket, reason, errors, time.Now().UnixNano()))
}
