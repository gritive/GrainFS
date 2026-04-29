package cluster

import (
	"context"
	"fmt"
	"sync"
)

// RebalanceMove records a single MoveReplica call.
type RebalanceMove struct {
	GroupID  string
	FromNode string
	ToNode   string
}

// MockGroupRebalancer records MoveReplica calls for testing.
// PR-E에서 DataGroupPlanExecutor (real §4.4 ops)로 교체.
type MockGroupRebalancer struct {
	mu    sync.Mutex
	moves []RebalanceMove
	err   error // if non-nil, MoveReplica returns this error
}

// SetError configures a fixed error returned by all future MoveReplica calls.
func (m *MockGroupRebalancer) SetError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

func (m *MockGroupRebalancer) MoveReplica(_ context.Context, groupID, fromNode, toNode string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.moves = append(m.moves, RebalanceMove{GroupID: groupID, FromNode: fromNode, ToNode: toNode})
	return nil
}

// Moves returns a copy of recorded moves.
func (m *MockGroupRebalancer) Moves() []RebalanceMove {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RebalanceMove, len(m.moves))
	copy(out, m.moves)
	return out
}

// StubGroupRebalancer는 serve.go에서 GroupRebalancer가 없을 때 사용되는 no-op 구현이다.
// PR-E DataGroupPlanExecutor가 wired되기 전까지 사용.
type StubGroupRebalancer struct{}

func (StubGroupRebalancer) MoveReplica(_ context.Context, groupID, fromNode, toNode string) error {
	return fmt.Errorf("rebalance executor not wired (PR-E): group=%s from=%s to=%s", groupID, fromNode, toNode)
}
