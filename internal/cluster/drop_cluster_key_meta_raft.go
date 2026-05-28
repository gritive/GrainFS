package cluster

import (
	"context"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// DropClusterKeyStamp carries the voter set + raft config index observed by
// the leader immediately before proposing DropClusterKeyAccept.
type DropClusterKeyStamp struct {
	Voters      []string
	ConfigIndex uint64
}

// ProposeDropClusterKeyAccept proposes the cluster-key-drop command.
func (m *MetaRaft) ProposeDropClusterKeyAccept(ctx context.Context, stamp DropClusterKeyStamp) error {
	payload, err := encodeDropClusterKeyAcceptCmd(stamp)
	if err != nil {
		return fmt.Errorf("meta_raft: encode DropClusterKeyAccept: %w", err)
	}
	return m.Propose(ctx, MetaCmdTypeDropClusterKeyAccept, payload)
}

func encodeDropClusterKeyAcceptCmd(stamp DropClusterKeyStamp) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	voterOffs := make([]flatbuffers.UOffsetT, len(stamp.Voters))
	for i, v := range stamp.Voters {
		voterOffs[i] = b.CreateString(v)
	}
	clusterpb.DropClusterKeyAcceptCmdStartStampedVotersVector(b, len(voterOffs))
	for i := len(voterOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(voterOffs[i])
	}
	votersOff := b.EndVector(len(voterOffs))

	clusterpb.DropClusterKeyAcceptCmdStart(b)
	clusterpb.DropClusterKeyAcceptCmdAddStampedVoters(b, votersOff)
	clusterpb.DropClusterKeyAcceptCmdAddStampedConfigIndex(b, stamp.ConfigIndex)
	cmdOff := clusterpb.DropClusterKeyAcceptCmdEnd(b)
	b.Finish(cmdOff)
	return b.FinishedBytes(), nil
}

func decodeDropClusterKeyAcceptCmd(data []byte) (DropClusterKeyStamp, error) {
	if len(data) == 0 {
		return DropClusterKeyStamp{}, fmt.Errorf("DropClusterKeyAcceptCmd: empty payload")
	}
	cmd := clusterpb.GetRootAsDropClusterKeyAcceptCmd(data, 0)
	out := DropClusterKeyStamp{ConfigIndex: cmd.StampedConfigIndex()}
	out.Voters = make([]string, cmd.StampedVotersLength())
	for i := range out.Voters {
		out.Voters[i] = string(cmd.StampedVoters(i))
	}
	return out, nil
}
