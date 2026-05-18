package cluster

import "errors"

// ErrStalePlacement signals the placement group changed between the
// coordinator's placement resolve and FSM apply (rebalance window). The
// coordinator (Task 21) performs transparent retry up to 2 times before
// returning 503 SlowDown to the client.
var ErrStalePlacement = errors.New("append: placement group changed mid-request")
