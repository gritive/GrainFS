package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/migration"
)

func TestMigrationProposer_ProposeJobStart(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &MigrationProposer{
		Propose: func(_ context.Context, ct clusterpb.MetaCmdType, payload []byte) error {
			gotType = ct
			gotPayload = payload
			return nil
		},
	}
	require.NoError(t, p.ProposeJobStart(context.Background(), "my-bucket"))
	assert.Equal(t, clusterpb.MetaCmdTypeMigrationJobStart, gotType)
	bucket, _, err := migration.DecodeJobStartPayload(gotPayload)
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", bucket)
}

func TestMigrationProposer_ProposeJobDone(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &MigrationProposer{
		Propose: func(_ context.Context, ct clusterpb.MetaCmdType, payload []byte) error {
			gotType = ct
			gotPayload = payload
			return nil
		},
	}
	require.NoError(t, p.ProposeJobDone(context.Background(), "b", 10, 2))
	assert.Equal(t, clusterpb.MetaCmdTypeMigrationJobDone, gotType)
	bucket, copied, errs, _, err := migration.DecodeJobDonePayload(gotPayload)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, int64(10), copied)
	assert.Equal(t, int64(2), errs)
}

func TestMigrationProposer_ProposeJobFailed(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &MigrationProposer{
		Propose: func(_ context.Context, ct clusterpb.MetaCmdType, payload []byte) error {
			gotType = ct
			gotPayload = payload
			return nil
		},
	}
	require.NoError(t, p.ProposeJobFailed(context.Background(), "b", "list error", 5))
	assert.Equal(t, clusterpb.MetaCmdTypeMigrationJobFailed, gotType)
	bucket, reason, errs, _, err := migration.DecodeJobFailedPayload(gotPayload)
	require.NoError(t, err)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, "list error", reason)
	assert.Equal(t, int64(5), errs)
}

func TestMigrationProposer_ProposeError_Propagates(t *testing.T) {
	propErr := errors.New("raft unavailable")
	p := &MigrationProposer{
		Propose: func(_ context.Context, _ clusterpb.MetaCmdType, _ []byte) error {
			return propErr
		},
	}
	assert.ErrorIs(t, p.ProposeJobStart(context.Background(), "b"), propErr)
	assert.ErrorIs(t, p.ProposeJobDone(context.Background(), "b", 0, 0), propErr)
	assert.ErrorIs(t, p.ProposeJobFailed(context.Background(), "b", "err", 0), propErr)
}

func TestMigrationProposerCutoverUsesGate(t *testing.T) {
	var gotPlan compat.GatePlan
	var gotType clusterpb.MetaCmdType
	p := &MigrationProposer{
		ProposeWithGate: func(_ context.Context, plan compat.GatePlan, cmdType clusterpb.MetaCmdType, payload []byte) error {
			gotPlan = plan
			gotType = cmdType
			require.NotEmpty(t, payload)
			return nil
		},
	}
	plan := compat.GatePlan{
		Capability: compat.CapabilityMigrationCutoverV1,
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationMigrationCutover,
		ConfigID:   7,
	}
	require.NoError(t, p.ProposeCutover(context.Background(), plan, "bucket-a"))
	require.Equal(t, plan, gotPlan)
	require.Equal(t, clusterpb.MetaCmdTypeMigrationCutover, gotType)
}
