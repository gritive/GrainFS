package resourceguard

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

// capturingRecorder records the raw facts slice handed to Record, so tests can
// assert the field-level output of recordResourceDecision directly — the
// reduced incident state masks per-fact differences like Cause on the derived
// facts.
type capturingRecorder struct{ facts []incident.Fact }

func (c *capturingRecorder) Record(_ context.Context, facts []incident.Fact) error {
	c.facts = append(c.facts, facts...)
	return nil
}

// TestRecordResourceDecision_DerivedFactToggles pins the two per-resource
// toggles that the generic recorder carries, in BOTH directions:
//
//   - causeOnDerived: vlog stamps Cause onto the derived Diagnosed fact;
//     FD/goroutine leave it at the zero value.
//   - diagMessage: vlog rewrites the Warn/Critical message with the per-DB
//     breakdown; FD/goroutine pass decision.Message through unchanged.
//
// These assertions read the facts slice (not the reduced incident) because the
// reducer derives Cause from the Observed fact, hiding the causeOnDerived
// difference. Neutering either toggle in recordVlogDecision turns this RED.
func TestRecordResourceDecision_DerivedFactToggles(t *testing.T) {
	ctx := context.Background()
	warn := &resourcewatch.Decision{
		Level:   resourcewatch.LevelWarn,
		Message: "ratio 0.41/0.40",
		Snapshot: resourcewatch.Sample{
			Categories:  map[resourcewatch.Category]int{resourcewatch.DBCategoryGroupRaft: 3 << 30},
			CollectedAt: time.Unix(100, 0).UTC(),
		},
	}

	// vlog: cause propagated to the derived fact + breakdown message.
	var vlogRec capturingRecorder
	require.NoError(t, recordVlogDecision(ctx, &vlogRec, "node-1", warn))
	require.Len(t, vlogRec.facts, 2)
	vlogDiag := vlogRec.facts[1]
	assert.Equal(t, incident.FactDiagnosed, vlogDiag.Type)
	assert.Equal(t, incident.CauseVlogPressure, vlogDiag.Cause, "vlog derived fact must carry Cause (causeOnDerived)")
	assert.Contains(t, vlogDiag.Message, "top:", "vlog Warn message must be the breakdown (diagMessage)")

	// fd: derived fact cause stays zero + plain decision.Message.
	var fdRec capturingRecorder
	require.NoError(t, recordFDDecision(ctx, &fdRec, "node-1", warn))
	require.Len(t, fdRec.facts, 2)
	fdDiag := fdRec.facts[1]
	assert.Equal(t, incident.FactDiagnosed, fdDiag.Type)
	assert.Equal(t, incident.Cause(""), fdDiag.Cause, "fd derived fact must leave Cause zero")
	assert.Equal(t, "ratio 0.41/0.40", fdDiag.Message, "fd Warn message must be decision.Message unchanged")

	// The shared Observed fact carries Cause for every resource.
	assert.Equal(t, incident.CauseVlogPressure, vlogRec.facts[0].Cause)
	assert.Equal(t, incident.CauseFDExhaustionRisk, fdRec.facts[0].Cause)
}
