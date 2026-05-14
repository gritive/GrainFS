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
	p := &NfsExportProposer{Propose: func(_ context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error {
		gotType = cmdType
		gotPayload = payload
		return nil
	}}

	cfg := nfsexport.Config{ReadOnly: true}
	require.NoError(t, p.ProposeUpsert(context.Background(), "b1", cfg))
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportUpsert, gotType)
	bucket, gotCfg, err := nfsexport.DecodeUpsertPayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
	require.Equal(t, cfg, gotCfg)
}

func TestNfsExportProposerProposeDelete(t *testing.T) {
	var gotType clusterpb.MetaCmdType
	var gotPayload []byte
	p := &NfsExportProposer{Propose: func(_ context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error {
		gotType = cmdType
		gotPayload = payload
		return nil
	}}

	require.NoError(t, p.ProposeDelete(context.Background(), "b1"))
	require.Equal(t, clusterpb.MetaCmdTypeNfsExportDelete, gotType)
	bucket, err := nfsexport.DecodeDeletePayload(gotPayload)
	require.NoError(t, err)
	require.Equal(t, "b1", bucket)
}
