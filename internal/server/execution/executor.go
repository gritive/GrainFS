package execution

import "context"

type Strategy string

const (
	StrategySingle  Strategy = "single"
	StrategyCluster Strategy = "cluster"
)

type Plan struct {
	Strategy  Strategy
	Operation Operation
}

type Result struct {
	Scrub ScrubResult
}

type ScrubResult struct {
	SessionID string
	Created   bool
}

type Executor interface {
	Execute(ctx context.Context, plan Plan) (Result, error)
}
