package cluster

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

func TestMetaForwardRequest_RoundTrip_NilPlan(t *testing.T) {
	cmd := []byte("raft-command-bytes")
	payload := encodeMetaForwardRequest(cmd, nil)
	require.True(t, bytes.HasPrefix(payload, []byte("GFSMFWD2")))

	gotCmd, gotPlan, framed, err := decodeMetaForwardRequest(payload)
	require.NoError(t, err)
	require.True(t, framed)
	require.Equal(t, cmd, gotCmd)
	require.Nil(t, gotPlan)
}

func TestMetaForwardRequest_RoundTrip_FullPlan(t *testing.T) {
	cmd := []byte("gated-raft-command")
	plan := &compat.GatePlan{
		Capability: "test_cap",
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationCreateMultipartUpload,
		ConfigID:   42,
		Required:   []compat.NodeID{"node-a", "node-b"},
		Missing:    []compat.NodeID{"node-c"},
		Unknown:    []compat.NodeID{"node-d"},
		Stale: []compat.StaleNode{
			{NodeID: "node-e", LastSeen: time.UnixMilli(1700000000000).UTC()},
		},
	}

	payload := encodeMetaForwardRequest(cmd, plan)
	gotCmd, gotPlan, framed, err := decodeMetaForwardRequest(payload)
	require.NoError(t, err)
	require.True(t, framed)
	require.Equal(t, cmd, gotCmd)
	require.NotNil(t, gotPlan)
	require.Equal(t, plan.Capability, gotPlan.Capability)
	require.Equal(t, plan.Scope, gotPlan.Scope)
	require.Equal(t, plan.Severity, gotPlan.Severity)
	require.Equal(t, plan.Operation, gotPlan.Operation)
	require.Equal(t, plan.ConfigID, gotPlan.ConfigID)
	require.Equal(t, plan.Required, gotPlan.Required)
	require.Equal(t, plan.Missing, gotPlan.Missing)
	require.Equal(t, plan.Unknown, gotPlan.Unknown)
	require.Len(t, gotPlan.Stale, 1)
	require.Equal(t, plan.Stale[0].NodeID, gotPlan.Stale[0].NodeID)
	require.True(t, plan.Stale[0].LastSeen.Equal(gotPlan.Stale[0].LastSeen))
}

func TestMetaForwardRequest_PassthroughUnframed(t *testing.T) {
	raw := []byte("not-framed-just-raw-bytes")
	gotCmd, gotPlan, framed, err := decodeMetaForwardRequest(raw)
	require.NoError(t, err)
	require.False(t, framed)
	require.Equal(t, raw, gotCmd)
	require.Nil(t, gotPlan)
}

func TestMetaForwardRequest_RejectsLegacyV1Magic(t *testing.T) {
	payload := append([]byte("GFSMFWD1"), []byte(`{"command":"AAAA"}`)...)
	_, _, framed, err := decodeMetaForwardRequest(payload)
	require.True(t, framed, "v1 magic must be detected (framed=true)")
	require.Error(t, err)
	require.ErrorIs(t, err, icebergcatalog.ErrServiceUnavailable)
	require.Contains(t, err.Error(), "GFSMFWD1")
}

func TestMetaForwardRequest_MalformedFB(t *testing.T) {
	payload := append([]byte("GFSMFWD2"), 0x00, 0x01, 0x02, 0x03)
	_, _, framed, err := decodeMetaForwardRequest(payload)
	require.True(t, framed)
	require.Error(t, err)
	require.ErrorIs(t, err, icebergcatalog.ErrServiceUnavailable)
}

func TestMetaForwardReply_Success(t *testing.T) {
	payload := encodeMetaForwardReplyWithIndex(12345, nil)
	idx, err := decodeMetaForwardReplyWithIndex(payload)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), idx)
}

func TestMetaForwardReply_AllErrorTypes(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"not-leader", raft.ErrNotLeader, raft.ErrNotLeader},
		{"capability-rejected", compat.ErrCapabilityRejected, compat.ErrCapabilityRejected},
		{"namespace-not-found", icebergcatalog.ErrNamespaceNotFound, icebergcatalog.ErrNamespaceNotFound},
		{"namespace-exists", icebergcatalog.ErrNamespaceExists, icebergcatalog.ErrNamespaceExists},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload := encodeMetaForwardReplyWithIndex(0, tc.in)
			_, err := decodeMetaForwardReplyWithIndex(payload)
			require.Error(t, err)
			require.True(t, errors.Is(err, tc.want), "got %v, want errors.Is(%v)", err, tc.want)
		})
	}
}

func TestMetaForwardReply_UnknownErrorType(t *testing.T) {
	b := newMetaForwardBuilder()
	errType := b.CreateString("future-error-type")
	errMsg := b.CreateString("some explanation")
	clusterpb.MetaForwardReplyStart(b)
	clusterpb.MetaForwardReplyAddErrorType(b, errType)
	clusterpb.MetaForwardReplyAddErrorMessage(b, errMsg)
	b.Finish(clusterpb.MetaForwardReplyEnd(b))
	payload := append([]byte(nil), b.FinishedBytes()...)
	releaseMetaForwardBuilder(b)

	_, err := decodeMetaForwardReplyWithIndex(payload)
	require.Error(t, err)
	require.NotErrorIs(t, err, raft.ErrNotLeader, "must not be misclassified")
}

func TestCompatEnumConverters_RoundTrip(t *testing.T) {
	scopes := []compat.Scope{
		compat.ScopeMetaRaft, compat.ScopeDataGroup,
		compat.ScopePeerTransport, compat.ScopeLocal,
	}
	for _, s := range scopes {
		require.Equal(t, s, scopeFromFB(scopeToFB(s)), "scope %s", s)
	}
	require.Equal(t, compat.Scope(""), scopeFromFB(clusterpb.CompatScopeUnknown))

	sevs := []compat.Severity{compat.SeverityHard, compat.SeveritySoft}
	for _, s := range sevs {
		require.Equal(t, s, severityFromFB(severityToFB(s)), "severity %s", s)
	}
	require.Equal(t, compat.Severity(""), severityFromFB(clusterpb.CompatSeverityUnknown))

	ops := []compat.Operation{
		compat.OperationMigrationCutover, compat.OperationNfsExportCreate,
		compat.OperationCreateMultipartUpload, compat.OperationListMultipartUploads,
		compat.OperationListParts,
	}
	for _, o := range ops {
		require.Equal(t, o, operationFromFB(operationToFB(o)), "operation %s", o)
	}
	require.Equal(t, compat.Operation(""), operationFromFB(clusterpb.CompatOperationUnknown))
}

func TestCompatEnumDriftGuard(t *testing.T) {
	scopes := []compat.Scope{
		compat.ScopeMetaRaft, compat.ScopeDataGroup,
		compat.ScopePeerTransport, compat.ScopeLocal,
	}
	for _, s := range scopes {
		require.NotEqual(t, clusterpb.CompatScopeUnknown, scopeToFB(s),
			"compat.Scope %q has no FB enum mapping; update cluster.fbs", s)
	}

	sevs := []compat.Severity{compat.SeverityHard, compat.SeveritySoft}
	for _, s := range sevs {
		require.NotEqual(t, clusterpb.CompatSeverityUnknown, severityToFB(s),
			"compat.Severity %q has no FB enum mapping; update cluster.fbs", s)
	}

	ops := []compat.Operation{
		compat.OperationMigrationCutover, compat.OperationNfsExportCreate,
		compat.OperationCreateMultipartUpload, compat.OperationListMultipartUploads,
		compat.OperationListParts,
	}
	for _, o := range ops {
		require.NotEqual(t, clusterpb.CompatOperationUnknown, operationToFB(o),
			"compat.Operation %q has no FB enum mapping; update cluster.fbs", o)
	}
}
