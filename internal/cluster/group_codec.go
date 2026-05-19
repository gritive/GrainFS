package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeGroupPutPayload serializes a GroupPut inner payload (the data bytes of a
// MetaCmd envelope). Exposed for dispatchers and tests that construct the payload
// outside the cluster package.
func EncodeGroupPutPayload(name string, policies []string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	nameOff := b.CreateString(name)
	// Build policies string vector. Strings must be created before StartObject.
	policyOffs := make([]flatbuffers.UOffsetT, len(policies))
	for i := range policies {
		policyOffs[i] = b.CreateString(policies[i])
	}
	clusterpb.MetaGroupPutCmdStartPoliciesVector(b, len(policyOffs))
	for i := len(policyOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(policyOffs[i])
	}
	policiesVec := b.EndVector(len(policyOffs))
	clusterpb.MetaGroupPutCmdStart(b)
	clusterpb.MetaGroupPutCmdAddName(b, nameOff)
	clusterpb.MetaGroupPutCmdAddPolicies(b, policiesVec)
	return fbFinish(b, clusterpb.MetaGroupPutCmdEnd(b)), nil
}

// DecodeGroupPutPayload parses a GroupPut inner payload and returns the group name
// and attached policy names.
func DecodeGroupPutPayload(data []byte) (name string, policies []string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaGroupPutCmd {
		return clusterpb.GetRootAsMetaGroupPutCmd(d, 0)
	})
	if err != nil {
		return "", nil, fmt.Errorf("group_codec: GroupPut: %w", err)
	}
	out := make([]string, t.PoliciesLength())
	for i := range out {
		out[i] = string(t.Policies(i))
	}
	return string(t.Name()), out, nil
}

// EncodeGroupDeletePayload serializes a GroupDelete inner payload.
func EncodeGroupDeletePayload(name string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	nameOff := b.CreateString(name)
	clusterpb.MetaGroupDeleteCmdStart(b)
	clusterpb.MetaGroupDeleteCmdAddName(b, nameOff)
	return fbFinish(b, clusterpb.MetaGroupDeleteCmdEnd(b)), nil
}

// DecodeGroupDeletePayload parses a GroupDelete inner payload and returns the
// group name.
func DecodeGroupDeletePayload(data []byte) (name string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaGroupDeleteCmd {
		return clusterpb.GetRootAsMetaGroupDeleteCmd(d, 0)
	})
	if err != nil {
		return "", fmt.Errorf("group_codec: GroupDelete: %w", err)
	}
	return string(t.Name()), nil
}

// EncodeGroupMemberPutPayload serializes a GroupMemberPut inner payload.
func EncodeGroupMemberPutPayload(group, saID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	groupOff := b.CreateString(group)
	saIDOff := b.CreateString(saID)
	clusterpb.MetaGroupMemberPutCmdStart(b)
	clusterpb.MetaGroupMemberPutCmdAddGroup(b, groupOff)
	clusterpb.MetaGroupMemberPutCmdAddSaId(b, saIDOff)
	return fbFinish(b, clusterpb.MetaGroupMemberPutCmdEnd(b)), nil
}

// DecodeGroupMemberPutPayload parses a GroupMemberPut inner payload and returns
// the group name and sa_id.
func DecodeGroupMemberPutPayload(data []byte) (group, saID string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaGroupMemberPutCmd {
		return clusterpb.GetRootAsMetaGroupMemberPutCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("group_codec: GroupMemberPut: %w", err)
	}
	return string(t.Group()), string(t.SaId()), nil
}

// EncodeGroupMemberDeletePayload serializes a GroupMemberDelete inner payload.
func EncodeGroupMemberDeletePayload(group, saID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	groupOff := b.CreateString(group)
	saIDOff := b.CreateString(saID)
	clusterpb.MetaGroupMemberDeleteCmdStart(b)
	clusterpb.MetaGroupMemberDeleteCmdAddGroup(b, groupOff)
	clusterpb.MetaGroupMemberDeleteCmdAddSaId(b, saIDOff)
	return fbFinish(b, clusterpb.MetaGroupMemberDeleteCmdEnd(b)), nil
}

// DecodeGroupMemberDeletePayload parses a GroupMemberDelete inner payload and
// returns the group name and sa_id.
func DecodeGroupMemberDeletePayload(data []byte) (group, saID string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaGroupMemberDeleteCmd {
		return clusterpb.GetRootAsMetaGroupMemberDeleteCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("group_codec: GroupMemberDelete: %w", err)
	}
	return string(t.Group()), string(t.SaId()), nil
}
