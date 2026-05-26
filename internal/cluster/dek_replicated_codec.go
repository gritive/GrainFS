package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// DEKReplicatedRotateCmd is the decoded in-memory form of a MetaDEKReplicatedRotateCmd payload.
// Phase D replicates leader-generated KEK-wrapped DEK bytes through the Raft log so all
// nodes hold byte-identical DEKs.
//
// ExpectedActiveGen is the active DEK gen the leader observed at propose time.
// math.MaxUint32 is the gen-0 bootstrap sentinel ("no existing DEK").
//
// NOTE: The codec is exported because the serveruntime post-commit hook (Task 8, package
// serveruntime) must decode this payload cross-package, mirroring how handleConfigPut
// calls the exported cluster.DecodeConfigPutPayload.
type DEKReplicatedRotateCmd struct {
	Gen               uint32
	WrappedDEK        []byte
	ExpectedActiveGen uint32 // math.MaxUint32 = gen-0 bootstrap sentinel
	ActiveKEKVer      uint32
}

// EncodeDEKReplicatedRotateCmd serializes a DEKReplicatedRotateCmd to a FlatBuffers byte slice.
// Rejects empty WrappedDEK.
func EncodeDEKReplicatedRotateCmd(cmd DEKReplicatedRotateCmd) ([]byte, error) {
	if len(cmd.WrappedDEK) == 0 {
		return nil, fmt.Errorf("dek_replicated_codec: wrapped_dek must be non-empty")
	}

	b := clusterBuilderPool.Get()

	// Byte vectors must be created before opening the table.
	wrappedOff := b.CreateByteVector(cmd.WrappedDEK)

	clusterpb.MetaDEKReplicatedRotateCmdStart(b)
	clusterpb.MetaDEKReplicatedRotateCmdAddGen(b, cmd.Gen)
	clusterpb.MetaDEKReplicatedRotateCmdAddWrappedDek(b, wrappedOff)
	clusterpb.MetaDEKReplicatedRotateCmdAddExpectedActiveGen(b, cmd.ExpectedActiveGen)
	clusterpb.MetaDEKReplicatedRotateCmdAddActiveKekVer(b, cmd.ActiveKEKVer)
	return fbFinish(b, clusterpb.MetaDEKReplicatedRotateCmdEnd(b)), nil
}

// DecodeDEKReplicatedRotateCmd parses a FlatBuffers-encoded MetaDEKReplicatedRotateCmd payload.
// Returns errors for empty data, FlatBuffers panics, and empty wrapped_dek.
func DecodeDEKReplicatedRotateCmd(data []byte) (DEKReplicatedRotateCmd, error) {
	if len(data) == 0 {
		return DEKReplicatedRotateCmd{}, fmt.Errorf("dek_replicated_codec: empty payload")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaDEKReplicatedRotateCmd {
		return clusterpb.GetRootAsMetaDEKReplicatedRotateCmd(d, 0)
	})
	if err != nil {
		return DEKReplicatedRotateCmd{}, fmt.Errorf("dek_replicated_codec: %w", err)
	}

	w := t.WrappedDekBytes()
	if len(w) == 0 {
		return DEKReplicatedRotateCmd{}, fmt.Errorf("dek_replicated_codec: wrapped_dek empty")
	}
	return DEKReplicatedRotateCmd{
		Gen:               t.Gen(),
		WrappedDEK:        append([]byte(nil), w...),
		ExpectedActiveGen: t.ExpectedActiveGen(),
		ActiveKEKVer:      t.ActiveKekVer(),
	}, nil
}
