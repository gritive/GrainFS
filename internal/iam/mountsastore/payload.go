package mountsastore

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodeCreatePayload encodes a MountSA create operation as a FlatBuffers buffer.
func EncodeCreatePayload(sa MountSA) []byte {
	b := flatbuffers.NewBuilder(128)
	nameOff := b.CreateString(sa.Name)
	createdByOff := b.CreateString(sa.CreatedBy)
	clusterpb.MountSACreatePayloadStart(b)
	clusterpb.MountSACreatePayloadAddName(b, nameOff)
	clusterpb.MountSACreatePayloadAddNumericUid(b, sa.NumericUID)
	clusterpb.MountSACreatePayloadAddCreatedAt(b, sa.CreatedAt)
	clusterpb.MountSACreatePayloadAddCreatedBy(b, createdByOff)
	b.Finish(clusterpb.MountSACreatePayloadEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

// DecodeCreatePayload decodes a FlatBuffers buffer into a MountSA.
func DecodeCreatePayload(buf []byte) (sa MountSA, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mountsa: invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return MountSA{}, fmt.Errorf("mountsa: empty payload")
	}
	p := clusterpb.GetRootAsMountSACreatePayload(buf, 0)
	sa = MountSA{
		Name:       string(p.Name()),
		NumericUID: p.NumericUid(),
		CreatedAt:  p.CreatedAt(),
		CreatedBy:  string(p.CreatedBy()),
	}
	if sa.Name == "" {
		return MountSA{}, fmt.Errorf("mountsa: name is required")
	}
	return sa, nil
}

// EncodeDeletePayload encodes a MountSA delete operation as a FlatBuffers buffer.
func EncodeDeletePayload(name string) []byte {
	b := flatbuffers.NewBuilder(64)
	nameOff := b.CreateString(name)
	clusterpb.MountSADeletePayloadStart(b)
	clusterpb.MountSADeletePayloadAddName(b, nameOff)
	b.Finish(clusterpb.MountSADeletePayloadEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

// DecodeDeletePayload decodes a FlatBuffers buffer and returns the MountSA name.
func DecodeDeletePayload(buf []byte) (name string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mountsa: invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", fmt.Errorf("mountsa: empty payload")
	}
	p := clusterpb.GetRootAsMountSADeletePayload(buf, 0)
	name = string(p.Name())
	if name == "" {
		return "", fmt.Errorf("mountsa: name is required")
	}
	return name, nil
}

// EncodeAttachPolicyPayload encodes a MountSA attach-policy operation as a FlatBuffers buffer.
func EncodeAttachPolicyPayload(mountSA, policy string) []byte {
	b := flatbuffers.NewBuilder(128)
	mountSAOff := b.CreateString(mountSA)
	policyOff := b.CreateString(policy)
	clusterpb.MountSAAttachPolicyPayloadStart(b)
	clusterpb.MountSAAttachPolicyPayloadAddMountSa(b, mountSAOff)
	clusterpb.MountSAAttachPolicyPayloadAddPolicy(b, policyOff)
	b.Finish(clusterpb.MountSAAttachPolicyPayloadEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

// DecodeAttachPolicyPayload decodes a FlatBuffers buffer and returns mountSA and policy names.
func DecodeAttachPolicyPayload(buf []byte) (mountSA, policy string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mountsa: invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", "", fmt.Errorf("mountsa: empty payload")
	}
	p := clusterpb.GetRootAsMountSAAttachPolicyPayload(buf, 0)
	return string(p.MountSa()), string(p.Policy()), nil
}

// EncodeDetachPolicyPayload encodes a MountSA detach-policy operation as a FlatBuffers buffer.
func EncodeDetachPolicyPayload(mountSA, policy string) []byte {
	b := flatbuffers.NewBuilder(128)
	mountSAOff := b.CreateString(mountSA)
	policyOff := b.CreateString(policy)
	clusterpb.MountSADetachPolicyPayloadStart(b)
	clusterpb.MountSADetachPolicyPayloadAddMountSa(b, mountSAOff)
	clusterpb.MountSADetachPolicyPayloadAddPolicy(b, policyOff)
	b.Finish(clusterpb.MountSADetachPolicyPayloadEnd(b))
	return append([]byte(nil), b.FinishedBytes()...)
}

// DecodeDetachPolicyPayload decodes a FlatBuffers buffer and returns mountSA and policy names.
func DecodeDetachPolicyPayload(buf []byte) (mountSA, policy string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mountsa: invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", "", fmt.Errorf("mountsa: empty payload")
	}
	p := clusterpb.GetRootAsMountSADetachPolicyPayload(buf, 0)
	return string(p.MountSa()), string(p.Policy()), nil
}
