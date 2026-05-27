package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeRegisterPendingLearnerCmd serializes a RegisterPendingLearner payload
// (raft cmd data — wrap with encodeMetaCmd to get the MetaCmd envelope).
func encodeRegisterPendingLearnerCmd(nodeID string, spki [32]byte, addr string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	spkiOff := b.CreateByteVector(spki[:])
	addrOff := b.CreateString(addr)
	clusterpb.MetaRegisterPendingLearnerCmdStart(b)
	clusterpb.MetaRegisterPendingLearnerCmdAddNodeId(b, idOff)
	clusterpb.MetaRegisterPendingLearnerCmdAddSpki(b, spkiOff)
	clusterpb.MetaRegisterPendingLearnerCmdAddAddress(b, addrOff)
	return fbFinish(b, clusterpb.MetaRegisterPendingLearnerCmdEnd(b)), nil
}

func decodeRegisterPendingLearnerCmd(data []byte) (nodeID string, spki [32]byte, addr string, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaRegisterPendingLearnerCmd {
		return clusterpb.GetRootAsMetaRegisterPendingLearnerCmd(d, 0)
	})
	if e != nil {
		return "", [32]byte{}, "", e
	}
	raw := t.SpkiBytes()
	if len(raw) != 32 {
		return "", [32]byte{}, "", fmt.Errorf("peer_registry_codec: spki must be 32 bytes, got %d", len(raw))
	}
	copy(spki[:], raw)
	return string(t.NodeId()), spki, string(t.Address()), nil
}

// encodeRegisterMemberCmd serializes a RegisterMember payload (non-demoting
// boot-time self-registration). presentsPerNode is recording-only (Task 7).
func encodeRegisterMemberCmd(nodeID string, spki [32]byte, addr string, presentsPerNode bool) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	spkiOff := b.CreateByteVector(spki[:])
	addrOff := b.CreateString(addr)
	clusterpb.MetaRegisterMemberCmdStart(b)
	clusterpb.MetaRegisterMemberCmdAddNodeId(b, idOff)
	clusterpb.MetaRegisterMemberCmdAddSpki(b, spkiOff)
	clusterpb.MetaRegisterMemberCmdAddAddress(b, addrOff)
	clusterpb.MetaRegisterMemberCmdAddPresentsPerNode(b, presentsPerNode)
	return fbFinish(b, clusterpb.MetaRegisterMemberCmdEnd(b)), nil
}

func decodeRegisterMemberCmd(data []byte) (nodeID string, spki [32]byte, addr string, presentsPerNode bool, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaRegisterMemberCmd {
		return clusterpb.GetRootAsMetaRegisterMemberCmd(d, 0)
	})
	if e != nil {
		return "", [32]byte{}, "", false, e
	}
	raw := t.SpkiBytes()
	if len(raw) != 32 {
		return "", [32]byte{}, "", false, fmt.Errorf("peer_registry_codec: spki must be 32 bytes, got %d", len(raw))
	}
	copy(spki[:], raw)
	return string(t.NodeId()), spki, string(t.Address()), t.PresentsPerNode(), nil
}

// encodePromoteMemberCmd serializes a PromoteMember payload.
func encodePromoteMemberCmd(nodeID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	clusterpb.MetaPromoteMemberCmdStart(b)
	clusterpb.MetaPromoteMemberCmdAddNodeId(b, idOff)
	return fbFinish(b, clusterpb.MetaPromoteMemberCmdEnd(b)), nil
}

func decodePromoteMemberCmd(data []byte) (nodeID string, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaPromoteMemberCmd {
		return clusterpb.GetRootAsMetaPromoteMemberCmd(d, 0)
	})
	if e != nil {
		return "", e
	}
	return string(t.NodeId()), nil
}

// encodeRevokePeerCmd serializes a RevokePeer payload.
func encodeRevokePeerCmd(nodeID string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString(nodeID)
	clusterpb.MetaRevokePeerCmdStart(b)
	clusterpb.MetaRevokePeerCmdAddNodeId(b, idOff)
	return fbFinish(b, clusterpb.MetaRevokePeerCmdEnd(b)), nil
}

func decodeRevokePeerCmd(data []byte) (nodeID string, err error) {
	t, e := fbSafe(data, func(d []byte) *clusterpb.MetaRevokePeerCmd {
		return clusterpb.GetRootAsMetaRevokePeerCmd(d, 0)
	})
	if e != nil {
		return "", e
	}
	return string(t.NodeId()), nil
}
