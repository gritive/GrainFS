package execution

type Planner struct {
	ClusterAvailable bool
}

func (p Planner) Plan(op Operation) (Plan, error) {
	if err := op.Validate(); err != nil {
		return Plan{}, err
	}
	if p.ClusterAvailable {
		return Plan{Strategy: StrategyCluster, Operation: op}, nil
	}
	return Plan{Strategy: StrategySingle, Operation: op}, nil
}
