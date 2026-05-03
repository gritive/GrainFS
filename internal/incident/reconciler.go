package incident

import (
	"context"
	"reflect"
	"time"
)

type ReconcilerOptions struct {
	Limit int
	Since time.Duration
}

type ReconcileResult struct {
	Checked    int
	DriftFound int
	DriftFixed int
	Errors     int
}

type Reconciler struct {
	store    StateStore
	events   EventReader
	receipts ReceiptLookup
	reducer  Reducer
	opts     ReconcilerOptions
}

func NewReconciler(store StateStore, events EventReader, receipts ReceiptLookup, reducer Reducer, opts ReconcilerOptions) *Reconciler {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Since <= 0 {
		opts.Since = time.Hour
	}
	return &Reconciler{store: store, events: events, receipts: receipts, reducer: reducer, opts: opts}
}

func (r *Reconciler) RunOnce(ctx context.Context) (ReconcileResult, error) {
	var result ReconcileResult
	states, err := r.store.List(ctx, r.opts.Limit)
	if err != nil {
		return result, err
	}
	for _, stored := range states {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Checked++
		facts, err := r.events.Facts(ctx, stored.ID, time.Now().Add(-r.opts.Since))
		if err != nil {
			result.Errors++
			continue
		}
		if receiptID, ok, err := r.receipts.ReceiptForCorrelation(ctx, stored.ID); err != nil {
			result.Errors++
			continue
		} else if ok {
			facts = append(facts, Fact{CorrelationID: stored.ID, Type: FactReceiptSigned, ReceiptID: receiptID, At: time.Now().UTC()})
		}
		expected, err := r.reducer.Reduce(facts)
		if err != nil {
			result.Errors++
			continue
		}
		if !reflect.DeepEqual(stored, expected) {
			result.DriftFound++
			if err := r.store.Put(ctx, expected); err != nil {
				result.Errors++
				continue
			}
			result.DriftFixed++
		}
	}
	return result, nil
}
