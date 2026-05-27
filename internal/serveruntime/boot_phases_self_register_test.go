package serveruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingRegistrar captures ProposeRegisterMember calls for assertions.
type recordingRegistrar struct {
	calls []registerCall
}

type registerCall struct {
	nodeID          string
	spki            [32]byte
	addr            string
	presentsPerNode bool
}

func (r *recordingRegistrar) ProposeRegisterMember(_ context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool) error {
	r.calls = append(r.calls, registerCall{nodeID: nodeID, spki: spki, addr: addr, presentsPerNode: presentsPerNode})
	return nil
}

func TestSelfRegisterMember_ProposesOwnSPKI(t *testing.T) {
	var spki [32]byte
	for i := range spki {
		spki[i] = byte(i + 1)
	}

	t.Run("happy path proposes own identity", func(t *testing.T) {
		reg := &recordingRegistrar{}
		err := selfRegisterMember(context.Background(), reg, "node-a", spki, "10.0.0.1:7000")
		require.NoError(t, err)
		require.Len(t, reg.calls, 1)
		assert.Equal(t, "node-a", reg.calls[0].nodeID)
		assert.Equal(t, spki, reg.calls[0].spki)
		assert.Equal(t, "10.0.0.1:7000", reg.calls[0].addr)
		assert.False(t, reg.calls[0].presentsPerNode, "foundation never flips the presented cert")
	})

	t.Run("zero SPKI skips cleanly", func(t *testing.T) {
		reg := &recordingRegistrar{}
		var zero [32]byte
		err := selfRegisterMember(context.Background(), reg, "node-a", zero, "10.0.0.1:7000")
		require.NoError(t, err)
		assert.Empty(t, reg.calls, "zero perNodeSPKI must not propose")
	})

	t.Run("nil registrar skips cleanly", func(t *testing.T) {
		err := selfRegisterMember(context.Background(), nil, "node-a", spki, "10.0.0.1:7000")
		require.NoError(t, err)
	})

	t.Run("double-call records twice with identical args (FSM dedups)", func(t *testing.T) {
		reg := &recordingRegistrar{}
		require.NoError(t, selfRegisterMember(context.Background(), reg, "node-a", spki, "10.0.0.1:7000"))
		require.NoError(t, selfRegisterMember(context.Background(), reg, "node-a", spki, "10.0.0.1:7000"))
		require.Len(t, reg.calls, 2)
		assert.Equal(t, reg.calls[0], reg.calls[1])
	})
}
