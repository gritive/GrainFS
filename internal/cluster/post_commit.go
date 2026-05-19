package cluster

import (
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// PostCommitHook is a function called after a MetaCmd is successfully applied
// to the FSM. cmdType identifies the command; payload is the raw inner bytes
// (cmd.DataBytes()) from the FlatBuffers envelope.
//
// Hooks are called from the FSM apply goroutine (runApplyLoop) after all state
// mutations complete. A hook MUST NOT block; use a goroutine for any I/O or
// outbound proposals.
type PostCommitHook func(cmdType clusterpb.MetaCmdType, payload []byte)

// RegisterPostCommit appends h to the list of post-commit hooks fired after
// each successful apply. Safe to call concurrently. Must be registered before
// MetaRaft.Start() to guarantee hooks are present for all log entries.
//
// Uses copy-on-write semantics: each register clones the previous slice and
// atomically swaps the pointer. The hot path (firePostCommitHooks) loads the
// pointer with a single atomic op — no mutex — so a no-hook FSM pays only a
// pointer load per apply, not a lock acquisition.
func (f *MetaFSM) RegisterPostCommit(h PostCommitHook) {
	for {
		old := f.postCommitHooks.Load()
		var next []PostCommitHook
		if old != nil {
			next = make([]PostCommitHook, len(*old)+1)
			copy(next, *old)
			next[len(*old)] = h
		} else {
			next = []PostCommitHook{h}
		}
		if f.postCommitHooks.CompareAndSwap(old, &next) {
			return
		}
	}
}

// firePostCommitHooks invokes each registered hook with the given command type
// and payload. Must be called after all state mutations succeed; not called on
// error. Lock-free: a single atomic load, fast path returns immediately when no
// hooks are registered.
func (f *MetaFSM) firePostCommitHooks(cmdType clusterpb.MetaCmdType, payload []byte) {
	hooks := f.postCommitHooks.Load()
	if hooks == nil || len(*hooks) == 0 {
		return
	}
	for _, h := range *hooks {
		h(cmdType, payload)
	}
}

// postCommitHooksField is the atomic pointer holding the current hook slice.
// Declared as a method on a typed pointer so the field type stays at the
// declaration site in meta_fsm.go.
type postCommitHooksField = atomic.Pointer[[]PostCommitHook]
