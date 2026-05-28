// Package cluster: PreparePresentFlip / BeginPresentFlip raft-command proposers.
//
// PreparePresentFlip (85): Apply is a pure FSM state mark (sets a transient
// presentFlipPreparing flag, no transport side effect); its COMMIT INDEX
// is the target the leader's applied-index barrier waits on (§8b).
//
// BeginPresentFlip (86): Apply is UNCONDITIONAL on every voter (deterministic,
// log-ordered); sets persisted present_flip_begun=true (snapshot slot 15) and
// fires the onPresentFlip callback OUTSIDE f.mu (mirrors PR-1's
// onClusterKeyDropped pattern).
//
// Config-stamp note (§8c step 4 + spec gate F1): the stamped voter set is
// RECORDED (telemetry / future PR-2b audit) but does NOT gate Apply in PR-2a.
// PR-2a is lazy + leaveable-safe.
package cluster

import (
	"context"
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// PresentFlipStamp carries the voter set + raft config index observed by the
// leader at Prepare-propose time.
type PresentFlipStamp struct {
	Voters      []string
	ConfigIndex uint64
}

// MetaCmdProposer is the upstream proposal sink. Production wires it to
// MetaRaft.ProposeWait via DistributedBackend.
type MetaCmdProposer func(ctx context.Context, payload []byte, timeout time.Duration) error

// ProposePreparePresentFlip builds the MetaCmd envelope for cmd 85.
func ProposePreparePresentFlip(ctx context.Context, prop MetaCmdProposer, stamp PresentFlipStamp, timeout time.Duration) error {
	payload, err := encodePresentFlipStampCmd(stamp)
	if err != nil {
		return fmt.Errorf("encode PreparePresentFlip: %w", err)
	}
	env, err := encodeMetaCmd(MetaCmdTypePreparePresentFlip, payload)
	if err != nil {
		return fmt.Errorf("encode MetaCmd envelope: %w", err)
	}
	return prop(ctx, env, timeout)
}

// ProposeBeginPresentFlip builds the MetaCmd envelope for cmd 86.
func ProposeBeginPresentFlip(ctx context.Context, prop MetaCmdProposer, stamp PresentFlipStamp, timeout time.Duration) error {
	payload, err := encodePresentFlipStampCmd(stamp)
	if err != nil {
		return fmt.Errorf("encode BeginPresentFlip: %w", err)
	}
	env, err := encodeMetaCmd(MetaCmdTypeBeginPresentFlip, payload)
	if err != nil {
		return fmt.Errorf("encode MetaCmd envelope: %w", err)
	}
	return prop(ctx, env, timeout)
}

// encodePresentFlipStampCmd builds a FB payload carrying voters + config index.
func encodePresentFlipStampCmd(stamp PresentFlipStamp) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	voterOffs := make([]flatbuffers.UOffsetT, len(stamp.Voters))
	for i, v := range stamp.Voters {
		voterOffs[i] = b.CreateString(v)
	}
	clusterpb.PreparePresentFlipCmdStartStampedVotersVector(b, len(voterOffs))
	for i := len(voterOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(voterOffs[i])
	}
	votersOff := b.EndVector(len(voterOffs))

	clusterpb.PreparePresentFlipCmdStart(b)
	clusterpb.PreparePresentFlipCmdAddStampedVoters(b, votersOff)
	clusterpb.PreparePresentFlipCmdAddStampedConfigIndex(b, stamp.ConfigIndex)
	cmdOff := clusterpb.PreparePresentFlipCmdEnd(b)
	b.Finish(cmdOff)
	return b.FinishedBytes(), nil
}

// decodePresentFlipStampCmd reverses encodePresentFlipStampCmd. Used by the
// FSM Apply for both cmd 85 and cmd 86.
//
//nolint:unused // FSM Apply wiring lands in Task 3; referenced by present_flip_meta_raft_test.go.
func decodePresentFlipStampCmd(payload []byte) (PresentFlipStamp, error) {
	t := clusterpb.GetRootAsPreparePresentFlipCmd(payload, 0)
	out := PresentFlipStamp{ConfigIndex: t.StampedConfigIndex()}
	n := t.StampedVotersLength()
	out.Voters = make([]string, n)
	for i := 0; i < n; i++ {
		out.Voters[i] = string(t.StampedVoters(i))
	}
	return out, nil
}
