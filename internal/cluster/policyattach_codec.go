package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodePolicyAttachToSAPutPayload serializes a PolicyAttachToSAPut inner
// payload (the data bytes of a MetaCmd envelope).
func EncodePolicyAttachToSAPutPayload(saID, policy string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	saIDOff := b.CreateString(saID)
	policyOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToSAPutCmdStart(b)
	clusterpb.MetaPolicyAttachToSAPutCmdAddSaId(b, saIDOff)
	clusterpb.MetaPolicyAttachToSAPutCmdAddPolicy(b, policyOff)
	return fbFinish(b, clusterpb.MetaPolicyAttachToSAPutCmdEnd(b)), nil
}

// DecodePolicyAttachToSAPutPayload parses a PolicyAttachToSAPut inner payload.
func DecodePolicyAttachToSAPutPayload(data []byte) (saID, policy string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyAttachToSAPutCmd {
		return clusterpb.GetRootAsMetaPolicyAttachToSAPutCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("policyattach_codec: PolicyAttachToSAPut: %w", err)
	}
	return string(t.SaId()), string(t.Policy()), nil
}

// EncodePolicyAttachToSADeletePayload serializes a PolicyAttachToSADelete inner
// payload.
func EncodePolicyAttachToSADeletePayload(saID, policy string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	saIDOff := b.CreateString(saID)
	policyOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToSADeleteCmdStart(b)
	clusterpb.MetaPolicyAttachToSADeleteCmdAddSaId(b, saIDOff)
	clusterpb.MetaPolicyAttachToSADeleteCmdAddPolicy(b, policyOff)
	return fbFinish(b, clusterpb.MetaPolicyAttachToSADeleteCmdEnd(b)), nil
}

// DecodePolicyAttachToSADeletePayload parses a PolicyAttachToSADelete inner
// payload.
func DecodePolicyAttachToSADeletePayload(data []byte) (saID, policy string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyAttachToSADeleteCmd {
		return clusterpb.GetRootAsMetaPolicyAttachToSADeleteCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("policyattach_codec: PolicyAttachToSADelete: %w", err)
	}
	return string(t.SaId()), string(t.Policy()), nil
}

// EncodePolicyAttachToGroupPutPayload serializes a PolicyAttachToGroupPut inner
// payload.
func EncodePolicyAttachToGroupPutPayload(group, policy string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	groupOff := b.CreateString(group)
	policyOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToGroupPutCmdStart(b)
	clusterpb.MetaPolicyAttachToGroupPutCmdAddGroup(b, groupOff)
	clusterpb.MetaPolicyAttachToGroupPutCmdAddPolicy(b, policyOff)
	return fbFinish(b, clusterpb.MetaPolicyAttachToGroupPutCmdEnd(b)), nil
}

// DecodePolicyAttachToGroupPutPayload parses a PolicyAttachToGroupPut inner
// payload.
func DecodePolicyAttachToGroupPutPayload(data []byte) (group, policy string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyAttachToGroupPutCmd {
		return clusterpb.GetRootAsMetaPolicyAttachToGroupPutCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("policyattach_codec: PolicyAttachToGroupPut: %w", err)
	}
	return string(t.Group()), string(t.Policy()), nil
}

// EncodePolicyAttachToGroupDeletePayload serializes a PolicyAttachToGroupDelete
// inner payload.
func EncodePolicyAttachToGroupDeletePayload(group, policy string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	groupOff := b.CreateString(group)
	policyOff := b.CreateString(policy)
	clusterpb.MetaPolicyAttachToGroupDeleteCmdStart(b)
	clusterpb.MetaPolicyAttachToGroupDeleteCmdAddGroup(b, groupOff)
	clusterpb.MetaPolicyAttachToGroupDeleteCmdAddPolicy(b, policyOff)
	return fbFinish(b, clusterpb.MetaPolicyAttachToGroupDeleteCmdEnd(b)), nil
}

// DecodePolicyAttachToGroupDeletePayload parses a PolicyAttachToGroupDelete
// inner payload.
func DecodePolicyAttachToGroupDeletePayload(data []byte) (group, policy string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyAttachToGroupDeleteCmd {
		return clusterpb.GetRootAsMetaPolicyAttachToGroupDeleteCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("policyattach_codec: PolicyAttachToGroupDelete: %w", err)
	}
	return string(t.Group()), string(t.Policy()), nil
}
