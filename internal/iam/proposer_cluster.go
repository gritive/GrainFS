package iam

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// ProposeFunc is the cluster-side propose function signature: takes a
// MetaCmd type and an already-encoded typed payload, returns nil on
// successful raft commit + local apply.
type ProposeFunc func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error

// MetaProposer implements Proposer by wrapping a cluster meta-FSM Propose
// closure. Each method builds the iampb-encoded payload and dispatches it
// through the closure. Errors propagate raw from the closure (no wrapping
// — caller context is enough).
type MetaProposer struct {
	Propose ProposeFunc
}

func (m *MetaProposer) ProposeSACreate(ctx context.Context, sa ServiceAccount) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMSACreate, buildSACreatePayload(sa))
}

func (m *MetaProposer) ProposeSADelete(ctx context.Context, saID string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMSADelete, buildSADeletePayload(saID))
}

func (m *MetaProposer) ProposeKeyCreate(ctx context.Context, k AccessKey) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMKeyCreate, buildKeyCreatePayload(k))
}

func (m *MetaProposer) ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMKeyCreateScoped, buildKeyCreatePayload(k))
}

func (m *MetaProposer) ProposeKeyRevoke(ctx context.Context, accessKey string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMKeyRevoke, buildKeyRevokePayload(accessKey))
}

func (m *MetaProposer) ProposeGrantPut(ctx context.Context, g Grant) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMGrantPut, buildGrantPutPayload(g))
}

func (m *MetaProposer) ProposeGrantDelete(ctx context.Context, saID, bucket string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMGrantDelete, buildGrantDeletePayload(saID, bucket))
}

func (m *MetaProposer) ProposeGrantWildcardPut(ctx context.Context, g Grant) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMGrantWildcardPut, buildGrantWildcardPutPayload(g))
}

func (m *MetaProposer) ProposeGrantWildcardDelete(ctx context.Context, saID string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMGrantWildcardDelete, buildGrantWildcardDeletePayload(saID))
}

func (m *MetaProposer) ProposeInitFirstSA(ctx context.Context, sa ServiceAccount, k AccessKey, g Grant) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMInitFirstSA, buildInitFirstSAPayload(sa, k, g))
}

func (m *MetaProposer) ProposeBucketUpstreamPut(ctx context.Context, u BucketUpstream) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMBucketUpstreamPut, buildBucketUpstreamPutPayload(u))
}

func (m *MetaProposer) ProposeBucketUpstreamDelete(ctx context.Context, bucket string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMBucketUpstreamDelete, buildBucketUpstreamDeletePayload(bucket))
}
