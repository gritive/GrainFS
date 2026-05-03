package incident

import (
	"context"
	"time"
)

type StateStore interface {
	Put(ctx context.Context, state IncidentState) error
	Get(ctx context.Context, id string) (IncidentState, bool, error)
	List(ctx context.Context, limit int) ([]IncidentState, error)
}

type EventReader interface {
	Facts(ctx context.Context, id string, since time.Time) ([]Fact, error)
}

type ReceiptLookup interface {
	ReceiptForCorrelation(ctx context.Context, id string) (receiptID string, found bool, err error)
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
