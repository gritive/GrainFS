package execution

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlannerChoosesClusterForScrubWhenClusterAvailable(t *testing.T) {
	planner := Planner{ClusterAvailable: true}
	op := Operation{Kind: OperationScrub, Scrub: ScrubOperation{Bucket: "b1"}}

	plan, err := planner.Plan(op)

	require.NoError(t, err)
	require.Equal(t, StrategyCluster, plan.Strategy)
	require.Equal(t, op, plan.Operation)
}

func TestPlannerChoosesSingleForScrubWhenClusterUnavailable(t *testing.T) {
	planner := Planner{ClusterAvailable: false}
	op := Operation{Kind: OperationScrub, Scrub: ScrubOperation{Bucket: "b1"}}

	plan, err := planner.Plan(op)

	require.NoError(t, err)
	require.Equal(t, StrategySingle, plan.Strategy)
	require.Equal(t, op, plan.Operation)
}

func TestPlannerRejectsInvalidOperation(t *testing.T) {
	_, err := (Planner{ClusterAvailable: true}).Plan(Operation{Kind: OperationScrub})
	require.True(t, errors.Is(err, ErrInvalidOperation))
}
