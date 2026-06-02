// Package cluster: leader-side cutover orchestration entrypoint (PR-2a §8c flow + §8e).
//
// RunPresentFlip is invoked directly for tests in PR-2a; the operator CLI
// ('grainfs cluster complete-cutover') ships in PR-2b. Flow:
//
//  1. Precondition: single-node → refuse (§8e).
//  2. Propose PreparePresentFlip; capture its committed index P.
//  3. Wait until every voter's LastApplied >= P (§8b barrier).
//  4. Propose-time re-read voter set: abort on mismatch.
//  5. Propose BeginPresentFlip.
//
// Fail-safe: any step error returns without proposing Begin → no partial flip.
package cluster

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// PresentFlipDeps wires the orchestration against the live MetaRaft +
// the cluster transport. Production wires:
//
//	Voters:           NewMetaRaftConfigReader(metaRaft).EffectiveConfiguration
//	RegistrySPKIs:    func() map[string][32]byte { return metaFSM.PeerRaftAddrToSPKI() }
//	  NOTE: EffectiveConfiguration returns raft server IDs (= transport addresses in
//	  production, set via MetaRaftConfig.RaftID). RegistrySPKIs MUST use the same
//	  key space — PeerRaftAddrToSPKI(), NOT PeerNodeIDToSPKI().
//	ProposeWithIndex: func(ctx, typ, payload) (uint64, error) that wraps
//	                  encodeMetaCmd(typ, payload) + m.node.ProposeWait(ctx, env)
//	WaitVoters:       func(ctx, idx, voters) error that wraps WaitVotersApplied
type PresentFlipDeps struct {
	SelfID           string
	Voters           func() (voters []string, configIndex uint64)
	RegistrySPKIs    func() map[string][32]byte
	ProposeWithIndex func(ctx context.Context, typ MetaCmdType, payload []byte) (uint64, error)
	WaitVoters       func(ctx context.Context, target uint64, voters []string) error
}

// RunPresentFlip executes the cutover flow described in §8c + §8e.
func RunPresentFlip(ctx context.Context, deps PresentFlipDeps, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	voters, configIdx := deps.Voters()

	// §8e: refuse on empty voter set — raft not yet initialized or config unreadable.
	if len(voters) == 0 {
		return fmt.Errorf("RunPresentFlip: empty voter set — raft configuration not initialized")
	}

	// §8e single-node refuse.
	if len(voters) == 1 && voters[0] == deps.SelfID {
		return fmt.Errorf("RunPresentFlip: refusing on single-node cluster (voters=[%s])", deps.SelfID)
	}

	// §8c step 1: voter⊆registry precondition.
	registry := deps.RegistrySPKIs()
	for _, v := range voters {
		if _, ok := registry[v]; !ok {
			return fmt.Errorf("RunPresentFlip: voter %q not in peer registry — operator must finish member registration before cutover", v)
		}
	}

	stamp := PresentFlipStamp{Voters: append([]string(nil), voters...), ConfigIndex: configIdx}

	// §8c step 2: propose PreparePresentFlip; capture committed index P.
	preparePayload, err := encodePresentFlipStampCmd(stamp)
	if err != nil {
		return fmt.Errorf("RunPresentFlip: encode Prepare: %w", err)
	}
	prepareIndex, err := deps.ProposeWithIndex(ctx, MetaCmdTypePreparePresentFlip, preparePayload)
	if err != nil {
		return fmt.Errorf("RunPresentFlip: propose Prepare: %w", err)
	}

	// §8b barrier: wait until every voter has applied up to prepareIndex.
	if err := deps.WaitVoters(ctx, prepareIndex, voters); err != nil {
		return fmt.Errorf("RunPresentFlip: barrier: %w", err)
	}

	// F1 plan-gate fix: propose-time re-read voter set; abort on mismatch.
	currentVoters, _ := deps.Voters()
	if !voterSetsEqual(stamp.Voters, currentVoters) {
		return fmt.Errorf("RunPresentFlip: voter set changed between Prepare (%v) and Begin (%v) — re-run after new voter is registered", stamp.Voters, currentVoters)
	}

	// §8c step 4: propose BeginPresentFlip.
	beginPayload, err := encodePresentFlipStampCmd(stamp)
	if err != nil {
		return fmt.Errorf("RunPresentFlip: encode Begin: %w", err)
	}
	if _, err := deps.ProposeWithIndex(ctx, MetaCmdTypeBeginPresentFlip, beginPayload); err != nil {
		return fmt.Errorf("RunPresentFlip: propose Begin: %w", err)
	}
	return nil
}

func voterSetsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string(nil), a...)
	sb := append([]string(nil), b...)
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}
