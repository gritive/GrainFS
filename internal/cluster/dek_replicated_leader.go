package cluster

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gritive/GrainFS/internal/compat"
)

// ErrDEKRotateInProgress is returned when a DEK rotation is already running on
// this leader. Sentinel so callers (post-commit hook, CLI) distinguish
// "retry later" from a hard error. Mirrors ErrKEKRotateAnotherInFlight.
var ErrDEKRotateInProgress = errors.New("ProposeDEKRotate: rotation already in progress")

// ProposeDEKBootstrap replicates the genesis node's gen-0 DEK through an
// UNGATED propose. Bootstrap-only: the genesis node is the sole voter when this
// runs, so the all-voter dek_replicated_v1 gate cannot pass (followers have not
// gossiped capability evidence yet — the gen-0 paradox). Safety is enforced at
// Apply: applyDEKReplicatedRotate accepts the MaxUint32 sentinel ONLY when no
// DEK gen exists yet AND gen == 0. Caller MUST be the genesis leader holding a
// freshly generated gen-0 keeper.
func (m *MetaRaft) ProposeDEKBootstrap(ctx context.Context, gen uint32, wrappedDEK []byte, kekVer uint32) error {
	// Genesis is gen-0 by definition. Reject any other gen on the ungated path
	// so a wiring bug cannot replicate a non-genesis gen through the bypass
	// (MEDIUM 1 / Pass 2). Apply enforces the same invariant defense-in-depth.
	if gen != 0 {
		return fmt.Errorf("ProposeDEKBootstrap: bootstrap gen must be 0, got %d", gen)
	}
	payload, err := EncodeDEKReplicatedRotateCmd(DEKReplicatedRotateCmd{
		Gen:               gen,
		WrappedDEK:        wrappedDEK,
		ExpectedActiveGen: math.MaxUint32, // sentinel: "no existing DEK"
		ActiveKEKVer:      kekVer,
	})
	if err != nil {
		return fmt.Errorf("ProposeDEKBootstrap: encode: %w", err)
	}
	return m.Propose(ctx, MetaCmdTypeDEKReplicatedRotate, payload) // UNGATED
}

// ensureDEKLeadership publishes a DEK epoch IFF none exists yet, NON-
// destructively (HIGH / Pass 4) AND leader-aware (HIGH / Pass 5). It is called
// both by the leadership watcher on the leader edge AND in-line by
// ProposeDEKRotate when a leader finds dekEpoch still nil (the watcher-
// publication window).
//
// MUST NOT cancel a live epoch (Pass 4): a destructive Swap+cancel (like the KEK
// path) would yank wctx out from under an in-flight rotation that self-published
// epA microseconds earlier, dropping the committed request with no retry. So:
// if a live epoch exists, return it; only CAS-publish when nil.
//
// MUST NOT publish while a follower (Pass 5 — ABA guard): a stale proposer that
// passed its IsLeader() check, then a real step-down ran loseDEKLeadership
// (swapping the epoch to nil), could otherwise CAS-publish a FRESH epoch while
// the node is already a follower. We check IsLeader() before AND after the CAS,
// and UNDO our own CAS (CompareAndSwap(cand, nil)) if leadership was lost around
// the publish. Returns an error instead of an epoch when not leader.
//
// NOTE: this INTENTIONALLY differs from KEKRotationLeader.SetEpochCtx, which
// destructively Swaps. KEK rotation is admin-triggered (a single human-initiated
// call under the KEK lifecycle mutex), so a fresh acquire replacing a stale
// epoch is fine. DEK rotation is SELF-acquired from a post-commit hook that can
// race the watcher, step-down, and other hooks, so it must be non-destructive +
// leader-aware. atomic.Pointer[T].CompareAndSwap is stdlib (Go 1.19+).
func (m *MetaRaft) ensureDEKLeadership() (*leadershipEpoch, error) {
	for {
		if !m.node.IsLeader() {
			m.loseDEKLeadership() // drop any stale epoch; never publish while follower
			return nil, fmt.Errorf("ProposeDEKRotate: not leader")
		}
		if ep := m.dekEpoch.Load(); ep != nil {
			return ep, nil // live epoch already published — never cancel it
		}
		ctx, cancel := context.WithCancel(context.Background())
		cand := &leadershipEpoch{ctx: ctx, cancel: cancel}
		if !m.dekEpoch.CompareAndSwap(nil, cand) {
			cancel() // lost the race; another caller published — retry-load
			continue
		}
		// Published cand. If a step-down landed concurrently (between the pre-CAS
		// IsLeader check and here), undo our own publish so dekEpoch does not stay
		// non-nil on a follower. CompareAndSwap(cand, nil) is a no-op if a
		// concurrent loseDEKLeadership already swapped it out; cancel() is
		// idempotent.
		if !m.node.IsLeader() {
			m.dekEpoch.CompareAndSwap(cand, nil)
			cancel()
			return nil, fmt.Errorf("ProposeDEKRotate: not leader")
		}
		return cand, nil
	}
}

// loseDEKLeadership is the ONLY path that cancels an existing epoch (step-down /
// watcher ctx done). Swap(nil)+cancel. Mirrors KEKRotationLeader.ClearEpoch.
func (m *MetaRaft) loseDEKLeadership() {
	if old := m.dekEpoch.Swap(nil); old != nil {
		old.cancel()
	}
}

// ProposeDEKRotate generates a fresh DEK on the LEADER, AAD-wraps it under the
// active KEK, and replicates the wrapped bytes through the GATED path so every
// node installs identical material (Phase D). Mirrors KEKRotationLeader
// discipline: not-leader guard via m.node.IsLeader(), fail-fast TryLock single-
// flight, bounded 60s timeout derived from the epoch ctx (so leader step-down
// cancels an in-flight rotation), stale-retry. The incoming ctx parameter is
// retained for DEKProposer signature compatibility; the raft waits run under the
// epoch-bounded wctx (matching the KEK path, which ignores the caller ctx).
func (m *MetaRaft) ProposeDEKRotate(ctx context.Context) error {
	keeper := m.fsm.DEKKeeper()
	if keeper == nil {
		return fmt.Errorf("ProposeDEKRotate: encryption not enabled")
	}
	// 1. Leadership AUTHORITY is m.node.IsLeader(), NOT the polled epoch (HIGH 2
	//    / Pass 3). If not leader, reject.
	if !m.node.IsLeader() {
		return fmt.Errorf("ProposeDEKRotate: not leader")
	}
	// 2. Single-flight FIRST, BEFORE touching epoch state (HIGH / Pass 4):
	//    fail-fast TryLock. Taking the lock before ensureDEKLeadership means two
	//    near-simultaneous nil-epoch callers cannot churn epoch state — only the
	//    TryLock winner proceeds; the loser returns ErrDEKRotateInProgress.
	if !m.dekRotateMu.TryLock() {
		return ErrDEKRotateInProgress
	}
	defer m.dekRotateMu.Unlock()
	// 3. Ensure (NON-destructively + leader-aware) a DEK epoch exists. The watcher
	//    publishes the epoch on the leader edge, but just after a leader win
	//    IsLeader() can be true while dekEpoch is still nil; a ConfigPut-triggered
	//    rotation in that window must NOT be dropped. ensureDEKLeadership CAS-
	//    publishes only if nil, NEVER cancels a live epoch (HIGH / Pass 4), and
	//    refuses to publish while a follower (HIGH / Pass 5 ABA guard).
	ep, err := m.ensureDEKLeadership()
	if err != nil {
		return err // not leader (step-down raced); TryLock released by deferred Unlock
	}
	// 4. Bounded timeout off the epoch ctx so step-down cancels us (mirror the
	//    KEK budget of 60s).
	wctx, cancel := context.WithTimeout(ep.ctx, 60*time.Second)
	defer cancel()

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := wctx.Err(); err != nil {
			return fmt.Errorf("ProposeDEKRotate: %w", err) // epoch cancelled or 60s elapsed
		}
		_, active := keeper.VersionsAndActive()
		expectedGen := active
		newGen := active + 1
		wrapped, kekVer, err := keeper.GenerateWrappedDEK(newGen)
		if err != nil {
			return fmt.Errorf("ProposeDEKRotate: generate: %w", err)
		}
		payload, err := EncodeDEKReplicatedRotateCmd(DEKReplicatedRotateCmd{
			Gen:               newGen,
			WrappedDEK:        wrapped,
			ExpectedActiveGen: expectedGen,
			ActiveKEKVer:      kekVer,
		})
		if err != nil {
			return fmt.Errorf("ProposeDEKRotate: encode: %w", err)
		}
		plan, err := m.capabilityGate.RequireMetaRaftCapability(
			compat.CapabilityDEKReplicatedV1, compat.OperationDEKRotate, time.Now())
		if err != nil {
			return fmt.Errorf("ProposeDEKRotate: gate: %w", err)
		}
		// Re-check leadership immediately before propose (HIGH 2 / Pass 3): a
		// stale epoch (not yet cancelled by the next watcher tick) must not let a
		// stepped-down former leader forward a SELF-initiated rotation. Authority
		// is m.node.IsLeader(); the epoch only bounds the ctx.
		if !m.node.IsLeader() {
			return fmt.Errorf("ProposeDEKRotate: not leader (lost leadership before propose)")
		}
		// Propose under the epoch-bounded wctx (NOT the caller's ctx).
		if _, err := m.ProposeWithGate(wctx, plan, MetaCmdTypeDEKReplicatedRotate, payload); err != nil {
			if !m.node.IsLeader() {
				return fmt.Errorf("ProposeDEKRotate: not leader (lost leadership mid-rotation)")
			}
			return err
		}
		// Stale-no-op detection: ProposeWithGate is wait-applied, so on nil the
		// local FSM has Applied. If active advanced to newGen we are done;
		// otherwise concurrent state moved — retry with fresh state.
		if _, a := keeper.VersionsAndActive(); a == newGen {
			return nil
		}
	}
	return fmt.Errorf("ProposeDEKRotate: exhausted %d retries (concurrent rotation churn)", maxRetries)
}
