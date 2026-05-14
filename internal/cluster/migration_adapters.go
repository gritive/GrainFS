package cluster

import (
	"context"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/migration"
)

// MigrationProposer satisfies migration.Proposer over the cluster meta-FSM.
// Mirrors LifecycleProposer.
type MigrationProposer struct {
	Propose         func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
	ProposeWithGate func(ctx context.Context, plan compat.GatePlan, cmdType clusterpb.MetaCmdType, payload []byte) error
	GatePlan        func(operation compat.Operation) (compat.GatePlan, error)
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

func (p *MigrationProposer) ProposeCutover(ctx context.Context, plan compat.GatePlan, bucket string) error {
	if p.ProposeWithGate == nil {
		return fmt.Errorf("migration proposer: gated propose not configured")
	}
	return p.ProposeWithGate(ctx, plan, clusterpb.MetaCmdTypeMigrationCutover,
		encodeMigrationCutoverPayload(bucket, time.Now().UnixNano()))
}

func (p *MigrationProposer) ProposeBucketUpstreamCutover(ctx context.Context, bucket string) error {
	if p.GatePlan == nil || p.ProposeWithGate == nil {
		return fmt.Errorf("migration proposer: capability gate not wired")
	}
	plan, err := p.GatePlan(compat.OperationMigrationCutover)
	if err != nil {
		return err
	}
	return p.ProposeCutover(ctx, plan, bucket)
}

func encodeMigrationCutoverPayload(bucket string, updatedAt int64) []byte {
	b := flatbuffers.NewBuilder(128)
	bucketOff := b.CreateString(bucket)
	clusterpb.MetaMigrationCutoverCmdStart(b)
	clusterpb.MetaMigrationCutoverCmdAddBucket(b, bucketOff)
	clusterpb.MetaMigrationCutoverCmdAddUpdatedAtUnixNs(b, updatedAt)
	root := clusterpb.MetaMigrationCutoverCmdEnd(b)
	return fbFinish(b, root)
}
