package iam

import (
	"context"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
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
	Cutover func(ctx context.Context, bucket string) error
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

func (m *MetaProposer) ProposeBucketUpstreamPut(ctx context.Context, u BucketUpstream) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMBucketUpstreamPut, buildBucketUpstreamPutPayload(u))
}

func (m *MetaProposer) ProposeBucketUpstreamDelete(ctx context.Context, bucket string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeIAMBucketUpstreamDelete, buildBucketUpstreamDeletePayload(bucket))
}

func (m *MetaProposer) ProposeBucketUpstreamCutover(ctx context.Context, bucket string) error {
	if m.Cutover == nil {
		return fmt.Errorf("iam: bucket upstream cutover proposer not configured")
	}
	return m.Cutover(ctx, bucket)
}

func (m *MetaProposer) ProposePolicyAttachToSAPut(ctx context.Context, saID, policy string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToSAPut, buildPolicyAttachToSAPutPayload(saID, policy))
}

func (m *MetaProposer) ProposePolicyAttachToSADelete(ctx context.Context, saID, policy string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypePolicyAttachToSADelete, buildPolicyAttachToSADeletePayload(saID, policy))
}

func (m *MetaProposer) ProposeCreateBucketWithPolicyAttach(ctx context.Context, bucket, sa, policy string) error {
	return m.Propose(ctx, clusterpb.MetaCmdTypeCreateBucketWithPolicyAttach, buildCreateBucketWithPolicyAttachPayload(bucket, sa, policy))
}

func buildPolicyAttachToSAPutPayload(saID, policy string) []byte {
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	polOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToSAPutCmdStart(b)
	clusterpb.MetaPolicyAttachToSAPutCmdAddSaId(b, saOff)
	clusterpb.MetaPolicyAttachToSAPutCmdAddPolicy(b, polOff)
	b.Finish(clusterpb.MetaPolicyAttachToSAPutCmdEnd(b))
	return b.FinishedBytes()
}

func buildPolicyAttachToSADeletePayload(saID, policy string) []byte {
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	polOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToSADeleteCmdStart(b)
	clusterpb.MetaPolicyAttachToSADeleteCmdAddSaId(b, saOff)
	clusterpb.MetaPolicyAttachToSADeleteCmdAddPolicy(b, polOff)
	b.Finish(clusterpb.MetaPolicyAttachToSADeleteCmdEnd(b))
	return b.FinishedBytes()
}

func buildCreateBucketWithPolicyAttachPayload(bucket, sa, policy string) []byte {
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(bucket)
	saOff := b.CreateString(sa)
	polOff := b.CreateString(policy)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdStart(b)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddBucket(b, bucketOff)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddAttachSa(b, saOff)
	clusterpb.MetaCreateBucketWithPolicyAttachCmdAddAttachPolicy(b, polOff)
	b.Finish(clusterpb.MetaCreateBucketWithPolicyAttachCmdEnd(b))
	return b.FinishedBytes()
}
