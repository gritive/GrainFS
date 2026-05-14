package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/nfsexport"
)

func TestNfsExportProposerProposeUpsert(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &NfsExportProposer{Propose: func(_ context.Context, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error) {
		gotType = cmdType
		gotPayload = payload
		return 99, nil
	}}

	cfg := nfsexport.Config{ReadOnly: true}
	idx, err := p.ProposeUpsert(context.Background(), "b1", cfg)
	require.NoError(t, err)
	require.Equal(t, uint64(99), idx)
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportUpsert, gotType)
	bucket, gotCfg, err := nfsexport.DecodeUpsertPayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
	require.Equal(t, cfg, gotCfg)
}

func TestNfsExportProposerProposeDelete(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &NfsExportProposer{Propose: func(_ context.Context, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error) {
		gotType = cmdType
		gotPayload = payload
		return 100, nil
	}}

	idx, err := p.ProposeDelete(context.Background(), "b1")
	require.NoError(t, err)
	require.Equal(t, uint64(100), idx)
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportDelete, gotType)
	bucket, err := nfsexport.DecodeDeletePayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
}

func TestNfsExportProposerProposeBucketDeleteCascade(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &NfsExportProposer{Propose: func(_ context.Context, cmdType clusterpb.MetaCmdType, payload []byte) (uint64, error) {
		gotType = cmdType
		gotPayload = payload
		return 101, nil
	}}

	idx, err := p.ProposeBucketDeleteCascade(context.Background(), "b1", true)
	require.NoError(t, err)
	require.Equal(t, uint64(101), idx)
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportBucketDeleteCascade, gotType)
	bucket, force, err := nfsexport.DecodeBucketDeleteCascadePayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
	require.True(t, force)
}
