package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// 회전 phase 사이의 grace 기간. raft commit 자체가 묵시적 ack 역할을 하므로
// 별도 ack RPC 없이도 모든 peer가 phase 변경을 적용했음을 보장한다 (raft 합의).
// grace 기간은 worker가 디스크 I/O + transport identity swap을 완료할 시간을
// 확보한다. 5초면 fsync + cert rebuild에 충분하고도 남는다.
const (
	RotationPhaseGrace    = 5 * time.Second
	RotationGlobalTimeout = 30 * time.Minute
	RotationPreviousGrace = time.Hour // previous.key 보존 기간 (D8 receipt 회전 윈도우와 정렬)
)

// ProposeRotateKeyBegin proposes RotateKeyBegin to the cluster. Caller must be
// the leader (typical: invoked by the localhost CLI socket handler after it
// has verified leadership). Blocks until the entry is applied locally.
func (m *MetaRaft) ProposeRotateKeyBegin(ctx context.Context, c RotateKeyBegin) error {
	payload, err := encodeMetaRotateKeyBeginCmd(c)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RotateKeyBegin: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRotateKeyBegin, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

func (m *MetaRaft) ProposeRotateKeySwitch(ctx context.Context, c RotateKeySwitch) error {
	payload, err := encodeMetaRotateKeySwitchCmd(c)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RotateKeySwitch: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRotateKeySwitch, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

func (m *MetaRaft) ProposeRotateKeyDrop(ctx context.Context, c RotateKeyDrop) error {
	payload, err := encodeMetaRotateKeyDropCmd(c)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RotateKeyDrop: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRotateKeyDrop, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

func (m *MetaRaft) ProposeRotateKeyAbort(ctx context.Context, c RotateKeyAbort) error {
	payload, err := encodeMetaRotateKeyAbortCmd(c)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RotateKeyAbort: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRotateKeyAbort, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// RotationState returns a snapshot of the current cluster-key rotation state.
func (m *MetaRaft) RotationState() RotationState {
	return m.fsm.rotation.State()
}

// runRotationAutoProgress is the leader-only goroutine that drives phase
// transitions. Followers' goroutine ticks but does nothing: m.IsLeader() gates
// any propose. On leader change, the new leader observes current FSM state
// and starts its own grace timer (might add a few seconds of delay; correct).
//
// Plan C ack model: raft commit is the implicit ack. Once the leader sees its
// own FSM in PhaseBegun (committed), all peers that successfully applied have
// also begun. Non-applying peers either crashed (raft removed them eventually)
// or have stale FSM (but the next phase command will be rejected by their FSM
// for being out of order, which is fine — they'll catch up via raft).
func (m *MetaRaft) runRotationAutoProgress(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	// phaseEnteredAt tracks when the leader-local goroutine first observed the
	// current phase. On leader change the new leader resets this — slight
	// extra delay, but correct (no double-propose).
	var (
		lastRotationID [16]byte
		lastPhase      = -1
		phaseEnteredAt time.Time
		// abortAttemptedFor avoids spamming raft with abort proposals every
		// second when the previous one failed (e.g., raft lag). One attempt
		// per global-timeout per rotation is plenty; the next leader (or next
		// rotation) gets a fresh shot.
		abortAttemptedFor [16]byte
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case <-tick.C:
		}
		if !m.IsLeader() {
			lastPhase = -1
			continue
		}
		st := m.fsm.rotation.State()
		now := time.Now()
		// Reset timer on phase / rotation_id change.
		if st.Phase != lastPhase || st.RotationID != lastRotationID {
			lastPhase = st.Phase
			lastRotationID = st.RotationID
			phaseEnteredAt = now
			continue
		}
		// Steady → nothing to do.
		if st.Phase == PhaseSteady {
			continue
		}
		// Global timeout check. Only attempt abort once per rotation_id.
		if now.Sub(phaseEnteredAt) > RotationGlobalTimeout {
			if abortAttemptedFor == st.RotationID {
				continue
			}
			abortAttemptedFor = st.RotationID
			log.Warn().Hex("rotation_id", st.RotationID[:]).
				Int("phase", st.Phase).
				Msg("meta_raft: rotation global timeout exceeded; aborting")
			abortCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := m.ProposeRotateKeyAbort(abortCtx, RotateKeyAbort{
				RotationID: st.RotationID,
				Reason:     "global-timeout",
			})
			cancel()
			if err != nil {
				log.Error().Err(err).Msg("meta_raft: rotation auto-abort failed; will not retry until rotation_id changes")
			}
			continue
		}
		// Phase grace check — wait for workers to apply side effects.
		if now.Sub(phaseEnteredAt) < RotationPhaseGrace {
			continue
		}
		// Advance phase.
		propCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		switch st.Phase {
		case PhaseBegun:
			err := m.ProposeRotateKeySwitch(propCtx, RotateKeySwitch{RotationID: st.RotationID})
			if err != nil {
				log.Error().Err(err).Hex("rotation_id", st.RotationID[:]).Msg("meta_raft: auto-progress Switch failed")
			}
		case PhaseSwitched:
			graceUntil := now.Add(RotationPreviousGrace).UnixNano()
			err := m.ProposeRotateKeyDrop(propCtx, RotateKeyDrop{
				RotationID: st.RotationID,
				GraceUntil: graceUntil,
			})
			if err != nil {
				log.Error().Err(err).Hex("rotation_id", st.RotationID[:]).Msg("meta_raft: auto-progress Drop failed")
			}
		}
		cancel()
	}
}

// PreviousKeyDeleter abstracts the keystore call so the cleanup goroutine can
// be tested without a real on-disk keystore. *transport.Keystore satisfies it.
type PreviousKeyDeleter interface {
	DeletePrevious() error
}

// runPreviousKeyCleanup deletes keys.d/previous.key when the FSM-recorded
// GraceUntil timestamp has passed. Runs on every node (not just leader); the
// FSM state is identical across peers and the deletion is local-disk only.
//
// Tick interval is 1 minute — much coarser than RotationPhaseGrace because
// previous.key grace is on the order of an hour. Per-deletion attempts are
// guarded by lastDeletedFor so we don't try the same delete every minute.
func (m *MetaRaft) runPreviousKeyCleanup(ctx context.Context, ks PreviousKeyDeleter) {
	if ks == nil {
		return
	}
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	var lastDeletedFor int64 // GraceUntil value we've already acted on
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.done:
			return
		case <-tick.C:
		}
		st := m.fsm.rotation.State()
		if st.GraceUntil == 0 || st.GraceUntil == lastDeletedFor {
			continue
		}
		if time.Now().UnixNano() < st.GraceUntil {
			continue
		}
		if err := ks.DeletePrevious(); err != nil {
			log.Warn().Err(err).Int64("grace_until", st.GraceUntil).
				Msg("meta_raft: failed to delete previous.key after grace expiry; will retry next tick")
			continue
		}
		lastDeletedFor = st.GraceUntil
		log.Info().Int64("grace_until", st.GraceUntil).
			Msg("meta_raft: deleted keys.d/previous.key after grace expiry")
	}
}

// StartPreviousKeyCleanup wires the cleanup goroutine. Must be called after
// Start() has set up m.done. Separate from Start() so callers (serve.go) can
// inject the keystore without forcing rotation_meta_raft.go to depend on
// transport package.
func (m *MetaRaft) StartPreviousKeyCleanup(ctx context.Context, ks PreviousKeyDeleter) {
	go m.runPreviousKeyCleanup(ctx, ks)
}

// 컴파일 타임에 clusterpb 의존성이 다른 상수와 정렬되어 있는지 확인.
var _ = clusterpb.MetaCmdTypeRotateKeyBegin
