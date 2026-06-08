package cluster

import (
	"context"
	"sort"
	"sync"
)

// IndexGroupManager owns the local object-index raft groups inside the cluster
// package. indexGroup is unexported, so serveruntime (a different package)
// cannot hold a map[string]*indexGroup directly — it touches only the exported
// *IndexGroupManager. Mirrors DataGroupManager's ownership role for data groups.
//
// This slice (Task 4.5) introduces the map + Lookup so the index-group proposal
// forward receiver can resolve a forwarded group ID to its local group. Task 5
// extends the manager with instantiate/start/Shards/Close.
type IndexGroupManager struct {
	mu     sync.RWMutex
	groups map[string]*indexGroup
	// closes holds each group's v2-close func (used by Close). Kept keyed
	// alongside groups so registration is a single atomic step.
	closes map[string]func() error
	// inFlight reserves a group ID while InstantiateAndStart builds it (outside
	// the lock), so the replay scan and the onIndexGroupAdded callback never both
	// open the same groupDir's BadgerDB. Lazily initialized.
	inFlight map[string]bool
}

func NewIndexGroupManager() *IndexGroupManager {
	return &IndexGroupManager{
		groups: make(map[string]*indexGroup),
		closes: make(map[string]func() error),
	}
}

// Lookup returns the local index group for groupID, or (nil, false) when no
// group is registered (e.g. during staggered boot before the group is wired).
func (m *IndexGroupManager) Lookup(groupID string) (*indexGroup, bool) {
	m.mu.RLock()
	g, ok := m.groups[groupID]
	m.mu.RUnlock()
	return g, ok
}

// register installs a local index group under groupID with its close func.
// Last-writer-wins on a key. buildAndRegister does its own registration inline
// (it must clear the inFlight reservation under the same lock), so this helper is
// referenced only by index_group_forward_test.go to stand up a manager directly.
//
//nolint:unused // referenced by index_group_forward_test.go (run.tests:false hides it).
func (m *IndexGroupManager) register(groupID string, g *indexGroup, closeFn func() error) {
	m.mu.Lock()
	m.groups[groupID] = g
	m.closes[groupID] = closeFn
	m.mu.Unlock()
}

// IndexGroupForwardSend is the follower→leader forward primitive the manager
// binds into each group's hook. It matches IndexGroupProposeForwardSender.Send:
// given the current leader hint, the group ID, and the encoded command, it
// returns the leader's committed index. nil ⇒ leader-local / solo groups (the
// hook is left nil, so proposeOrForward proposes locally).
//
// The manager binds the per-group hook INTERNALLY (it owns the *indexGroup, whose
// node provides LeaderID()), resolving the chicken-and-egg the boot wiring cannot:
// serveruntime cannot name *indexGroup, so it supplies only this Send func and the
// manager closes the hook over each group's own node LeaderID() at call-time.
type IndexGroupForwardSend func(ctx context.Context, leaderHint, groupID string, data []byte) (uint64, error)

// InstantiateAndStart builds, starts, and registers a local index group for each
// entry (entries arrive sorted by ID from MetaFSM.IndexGroups()). It is
// IDEMPOTENT and race-safe: the genesis/restart replay scan (boot goroutine) and
// the onIndexGroupAdded callback (apply-loop goroutine) both call it and may hit
// the SAME group ID concurrently. A naive "skip if Lookup() hits" has a TOCTOU
// window where two goroutines each call newRaftNode on the same groupDir — two
// BadgerDB opens on one dir = "Another process is using this Badger database".
// We mirror the data-group inFlight guard: reserve the ID under the lock before
// the slow build, build outside the lock, then register (clearing the
// reservation). A second caller observing the reservation skips.
func (m *IndexGroupManager) InstantiateAndStart(
	ctx context.Context,
	cfg IndexGroupLifecycleConfig,
	entries []IndexGroupEntry,
	send IndexGroupForwardSend,
) error {
	for _, entry := range entries {
		m.mu.Lock()
		if _, ok := m.groups[entry.ID]; ok {
			m.mu.Unlock()
			continue // already registered
		}
		if m.inFlight == nil {
			m.inFlight = make(map[string]bool)
		}
		if m.inFlight[entry.ID] {
			m.mu.Unlock()
			continue // another goroutine is building it
		}
		m.inFlight[entry.ID] = true
		m.mu.Unlock()

		if err := m.buildAndRegister(ctx, cfg, entry, send); err != nil {
			m.mu.Lock()
			delete(m.inFlight, entry.ID)
			m.mu.Unlock()
			return err
		}
	}
	return nil
}

// buildAndRegister builds one group (outside the manager lock), starts it, and
// registers it — clearing the inFlight reservation atomically with registration.
// The forward hook is bound here AFTER construction (mirroring the test harness's
// startNodeWithForward): build with nil forward, then set g.forward over the
// group's own node before Start, so the hook reads LeaderID() live every call.
func (m *IndexGroupManager) buildAndRegister(
	ctx context.Context,
	cfg IndexGroupLifecycleConfig,
	entry IndexGroupEntry,
	send IndexGroupForwardSend,
) error {
	groupCfg := cfg
	groupCfg.Forward = nil // bound below, after the group (and its node) exist
	g, v2Close, err := instantiateLocalIndexGroup(groupCfg, entry)
	if err != nil {
		return err
	}
	if send != nil {
		groupID := entry.ID
		g.forward = func(hookCtx context.Context, data []byte) (uint64, error) {
			return send(hookCtx, g.node.LeaderID(), groupID, data)
		}
	}
	if err := g.Start(ctx); err != nil {
		g.Close()
		_ = v2Close()
		return err
	}
	// Inbound per-group raft RPCs: register the STARTED node on the shared mux
	// (mirrors the data-group ordering — register after Start, not before — so an
	// inbound AppendEntries/RequestVote never reaches an un-started node).
	if cfg.GroupMux != nil {
		cfg.GroupMux.Register(entry.ID, g.node)
	}
	m.mu.Lock()
	m.groups[entry.ID] = g
	m.closes[entry.ID] = v2Close
	delete(m.inFlight, entry.ID)
	m.mu.Unlock()
	return nil
}

// Shards returns one ObjectIndexShard per registered group, ORDERED by group ID
// (index-00, index-01, …) so Shards()[i] is the group for hash%N==i. The
// zero-pad ID scheme (serveruntime.indexGroupID) makes lexicographic sort ==
// numeric shard order. Each *indexGroup satisfies all three component interfaces,
// so it drops straight into ObjectIndexShard{Reader, Writer, Lister}.
func (m *IndexGroupManager) Shards() []ObjectIndexShard {
	m.mu.RLock()
	ids := make([]string, 0, len(m.groups))
	for id := range m.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	shards := make([]ObjectIndexShard, len(ids))
	for i, id := range ids {
		g := m.groups[id]
		shards[i] = ObjectIndexShard{Reader: g, Writer: g, Lister: g}
	}
	m.mu.RUnlock()
	return shards
}

// Close shuts down every registered group: each group's node (g.Close()) THEN
// its v2 store close func, in that order (reversing hangs — the apply loop drains
// on node close before the store can be closed). Best-effort: a store-close error
// does not stop the rest.
func (m *IndexGroupManager) Close() {
	m.mu.Lock()
	groups := m.groups
	closes := m.closes
	m.groups = make(map[string]*indexGroup)
	m.closes = make(map[string]func() error)
	m.mu.Unlock()
	for id, g := range groups {
		g.Close()
		if closeFn := closes[id]; closeFn != nil {
			_ = closeFn()
		}
	}
}
