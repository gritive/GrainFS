package cluster

import "github.com/gritive/GrainFS/internal/cluster/clusterpb"

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
func (f *MetaFSM) RegisterPostCommit(h PostCommitHook) {
	f.mu.Lock()
	f.postCommitHooks = append(f.postCommitHooks, h)
	f.mu.Unlock()
}

// firePostCommitHooks invokes each registered hook with the given command type
// and payload. Must be called after all state mutations succeed; not called on
// error.
func (f *MetaFSM) firePostCommitHooks(cmdType clusterpb.MetaCmdType, payload []byte) {
	f.mu.RLock()
	hooks := f.postCommitHooks
	f.mu.RUnlock()
	for _, h := range hooks {
		h(cmdType, payload)
	}
}
