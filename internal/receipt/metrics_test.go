package receipt

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestBroadcastMetrics_IncrementsEachCounter(t *testing.T) {
	// Snapshot starting values — tests share the process-wide registry.
	startTotal := testutil.ToFloat64(BroadcastTotal)
	startHit := testutil.ToFloat64(BroadcastHit)
	startMiss := testutil.ToFloat64(BroadcastMiss)
	startTimeout := testutil.ToFloat64(BroadcastTimeout)
	startPartial := testutil.ToFloat64(BroadcastPartialSuccess)

	m := BroadcastMetrics{}
	m.OnBroadcastStart()
	m.OnBroadcastHit()
	m.OnBroadcastMiss()
	m.OnBroadcastTimeout()
	m.OnBroadcastPartialSuccess(3, 5)

	assert.Equal(t, startTotal+1, testutil.ToFloat64(BroadcastTotal))
	assert.Equal(t, startHit+1, testutil.ToFloat64(BroadcastHit))
	assert.Equal(t, startMiss+1, testutil.ToFloat64(BroadcastMiss))
	assert.Equal(t, startTimeout+1, testutil.ToFloat64(BroadcastTimeout))
	assert.Equal(t, startPartial+1, testutil.ToFloat64(BroadcastPartialSuccess))
}

func TestBroadcastMetrics_PartialSuccessIgnoresCounts(t *testing.T) {
	// Current implementation is a simple Inc regardless of responded/total.
	// Documents the contract so a future histogram migration is caught.
	start := testutil.ToFloat64(BroadcastPartialSuccess)
	m := BroadcastMetrics{}
	m.OnBroadcastPartialSuccess(1, 100)
	m.OnBroadcastPartialSuccess(99, 100)
	assert.Equal(t, start+2, testutil.ToFloat64(BroadcastPartialSuccess))
}
