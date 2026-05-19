package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// EncodePolicyPutPayload serializes a PolicyPut inner payload (the data bytes of a
// MetaCmd envelope). Exposed for dispatchers and tests that construct the payload
// outside the cluster package.
func EncodePolicyPutPayload(name string, docJSON []byte, builtin bool) ([]byte, error) {
	b := clusterBuilderPool.Get()
	nameOff := b.CreateString(name)
	docOff := b.CreateByteVector(docJSON)
	clusterpb.MetaPolicyPutCmdStart(b)
	clusterpb.MetaPolicyPutCmdAddName(b, nameOff)
	clusterpb.MetaPolicyPutCmdAddDocJson(b, docOff)
	clusterpb.MetaPolicyPutCmdAddBuiltin(b, builtin)
	return fbFinish(b, clusterpb.MetaPolicyPutCmdEnd(b)), nil
}

// DecodePolicyPutPayload parses a PolicyPut inner payload and returns the name,
// doc JSON bytes, and builtin flag. The returned doc bytes are a copy independent
// of the input buffer.
func DecodePolicyPutPayload(data []byte) (name string, docJSON []byte, builtin bool, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyPutCmd {
		return clusterpb.GetRootAsMetaPolicyPutCmd(d, 0)
	})
	if err != nil {
		return "", nil, false, fmt.Errorf("policy_codec: PolicyPut: %w", err)
	}
	// DocJsonBytes() aliases the underlying FlatBuffers buffer; copy to ensure the
	// caller's slice is independent of any pooled/reused Raft log entry buffer.
	rawDoc := t.DocJsonBytes()
	docCopy := append([]byte(nil), rawDoc...)
	return string(t.Name()), docCopy, t.Builtin(), nil
}

// EncodePolicyDeletePayload serializes a PolicyDelete inner payload.
func EncodePolicyDeletePayload(name string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	nameOff := b.CreateString(name)
	clusterpb.MetaPolicyDeleteCmdStart(b)
	clusterpb.MetaPolicyDeleteCmdAddName(b, nameOff)
	return fbFinish(b, clusterpb.MetaPolicyDeleteCmdEnd(b)), nil
}

// DecodePolicyDeletePayload parses a PolicyDelete inner payload and returns the
// policy name.
func DecodePolicyDeletePayload(data []byte) (name string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaPolicyDeleteCmd {
		return clusterpb.GetRootAsMetaPolicyDeleteCmd(d, 0)
	})
	if err != nil {
		return "", fmt.Errorf("policy_codec: PolicyDelete: %w", err)
	}
	return string(t.Name()), nil
}
