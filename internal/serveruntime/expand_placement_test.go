package serveruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

type fakePlanner struct {
	plan cluster.PlacementExpansionPlan
	err  error
}

func (f fakePlanner) PlanPlacementExpansion() (cluster.PlacementExpansionPlan, error) {
	return f.plan, f.err
}

type fakeRecorder struct {
	calls     int
	gotBase   []string
	gotExp    []string
	returnErr error
}

func (f *fakeRecorder) AddTopologyGeneration(_ context.Context, base, expanded []string) error {
	f.calls++
	f.gotBase = base
	f.gotExp = expanded
	return f.returnErr
}

// TestExpandPlacementClosure_RecordsOnGrowth proves a non-no-op plan records the
// generation with the planned Base/Expanded and surfaces the plan in the result.
func TestExpandPlacementClosure_RecordsOnGrowth(t *testing.T) {
	rec := &fakeRecorder{}
	fn := expandPlacementClosure(fakePlanner{plan: cluster.PlacementExpansionPlan{
		Base:     []string{"group-1", "group-2"},
		Expanded: []string{"group-1", "group-2", "group-3"},
		Added:    []string{"group-3"},
	}}, rec)

	res, err := fn(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, rec.calls, "non-no-op plan must record the generation")
	require.Equal(t, []string{"group-1", "group-2"}, rec.gotBase)
	require.Equal(t, []string{"group-1", "group-2", "group-3"}, rec.gotExp)
	require.Equal(t, []string{"group-3"}, res.Added)
	require.False(t, res.NoOp)
}

// TestExpandPlacementClosure_NoOpSkipsPropose proves a no-op plan records NOTHING
// — the load-bearing guard against writing a useless/duplicate generation.
func TestExpandPlacementClosure_NoOpSkipsPropose(t *testing.T) {
	rec := &fakeRecorder{}
	fn := expandPlacementClosure(fakePlanner{plan: cluster.PlacementExpansionPlan{
		Base:     []string{"group-1"},
		Expanded: []string{"group-1"},
		NoOp:     true,
	}}, rec)

	res, err := fn(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, rec.calls, "no-op plan must NOT record a generation")
	require.True(t, res.NoOp)
}

// TestExpandPlacementClosure_PlanErrorPropagates proves a planning failure is
// surfaced and no generation is recorded.
func TestExpandPlacementClosure_PlanErrorPropagates(t *testing.T) {
	rec := &fakeRecorder{}
	fn := expandPlacementClosure(fakePlanner{err: errors.New("no placement groups")}, rec)

	_, err := fn(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, rec.calls)
}

// TestMakeExpandPlacementFunc_NilDeps proves the concrete-typed nil guard returns
// a nil func (handler → 503) rather than a non-nil closure that would panic.
func TestMakeExpandPlacementFunc_NilDeps(t *testing.T) {
	require.Nil(t, makeExpandPlacementFunc(nil, nil))
}
