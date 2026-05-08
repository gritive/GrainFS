package serveruntime

import (
	"github.com/rs/zerolog/log"
)

// bootState carries the rolling state of Run's boot sequence. PR 1 introduces
// only the cleanup stack; PRs 2-6 will move per-phase artifacts (db, QUIC,
// metaRaft, …) onto typed fields here, replacing local variables in Run.
//
// Lifecycle:
//
//	state := newBootState(cfg)
//	defer state.Cleanup()
//	// phases construct artifacts and register their teardown via state.AddCleanup
//
// AddCleanup is called as each artifact is created successfully — exactly the
// position a `defer X.Close()` previously occupied. Cleanup() runs the stack
// LIFO at function exit, matching Go's defer semantics.
type bootState struct {
	cfg      Config
	cleanups []func()
}

// newBootState returns an empty state bound to cfg. Caller is responsible for
// calling Cleanup (typically via defer) once.
func newBootState(cfg Config) *bootState {
	return &bootState{cfg: cfg}
}

// AddCleanup pushes fn onto the cleanup stack. Order matters: cleanups run in
// LIFO so this mirrors `defer` registration order. Nil fn is a no-op (we still
// record the slot so positional references in tests stay stable).
func (s *bootState) AddCleanup(fn func()) {
	s.cleanups = append(s.cleanups, fn)
}

// Cleanup drains the cleanup stack in LIFO order. Each cleanup is wrapped so a
// panic in one fn does not skip the rest of the stack — the panic is logged
// and swallowed. Safe to call multiple times: subsequent calls are no-ops
// because the slice is reset to nil after the first drain.
func (s *bootState) Cleanup() {
	if s == nil {
		return
	}
	cleanups := s.cleanups
	s.cleanups = nil
	for i := len(cleanups) - 1; i >= 0; i-- {
		fn := cleanups[i]
		if fn == nil {
			continue
		}
		runCleanup(i, fn)
	}
}

// runCleanup invokes fn under a recover so a panic does not stop the stack.
// Split out so the test for panic safety can assert log output without
// reaching into an inline closure.
func runCleanup(idx int, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn().
				Int("cleanup_idx", idx).
				Interface("panic", r).
				Msg("boot cleanup panicked; continuing with remaining cleanups")
		}
	}()
	fn()
}
