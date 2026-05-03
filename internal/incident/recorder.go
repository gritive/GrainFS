package incident

import "context"

type Recorder struct {
	store   StateStore
	reducer Reducer
}

func NewRecorder(store StateStore, reducer Reducer) *Recorder {
	return &Recorder{store: store, reducer: reducer}
}

func (r *Recorder) Record(ctx context.Context, facts []Fact) error {
	state, err := r.reducer.Reduce(facts)
	if err != nil {
		return err
	}
	return r.store.Put(ctx, state)
}
