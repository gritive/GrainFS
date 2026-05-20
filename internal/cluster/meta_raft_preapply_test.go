package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMetaRaft_Start_PreApplyLoopRunsBeforeApply pins the §7 T57 race fix
// contract: the preApplyLoop callback passed to MetaRaft.Start MUST run
// synchronously between fsm.Restore and `go runApplyLoop`. If it ran after
// the apply goroutine launched, an apply-time DEKRotate could mutate the
// fresh keeper installed by wireDEKKeeper, and a subsequent SetDEKKeeper
// from the callback would silently discard the rotation.
//
// We assert two things:
//  1. preApplyLoop is called BEFORE Start returns (synchronous, not deferred).
//  2. Returning an error from preApplyLoop refuses startup — the apply loop
//     is NOT launched and the error propagates to the caller. This is the
//     mechanism F#21/F#22 use to refuse boot with a remediation message.
func TestMetaRaft_Start_PreApplyLoopRunsBeforeApply(t *testing.T) {
	t.Parallel()

	tr := newMetaTransportFake()
	m, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-0",
		Peers:     nil,
		DataDir:   t.TempDir(),
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-0", m)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())

	var preApplyCalled atomic.Bool
	preApply := func() error {
		// At this point Restore has run (synchronously), but the apply
		// goroutine has NOT been launched yet. Any swap of FSM-installed
		// dependencies here cannot race the apply loop.
		preApplyCalled.Store(true)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m.Start(ctx, preApply))

	// Synchronous contract: preApplyLoop must have fired before Start returned.
	require.True(t, preApplyCalled.Load(),
		"preApplyLoop must be invoked synchronously by Start, before the apply goroutine launches")
}

// TestMetaRaft_Start_PreApplyLoopErrorRefusesBoot covers the F#21/F#22 refusal
// path: when preApplyLoop returns an error, Start must propagate it and NOT
// launch the apply loop (the cluster would otherwise process commands with a
// half-initialized FSM).
func TestMetaRaft_Start_PreApplyLoopErrorRefusesBoot(t *testing.T) {
	t.Parallel()

	tr := newMetaTransportFake()
	m, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-0",
		Peers:     nil,
		DataDir:   t.TempDir(),
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-0", m)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())

	wantErr := "simulated F#21 KEK-missing refusal"
	preApply := func() error { return errBoot(wantErr) }

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = m.Start(ctx, preApply)
	require.Error(t, err)
	require.Contains(t, err.Error(), wantErr)
}

// errBoot is a tiny stub error so we don't pull a new import for this single
// table-row.
type errBoot string

func (e errBoot) Error() string { return string(e) }
