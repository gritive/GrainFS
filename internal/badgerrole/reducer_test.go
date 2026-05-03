package badgerrole

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReduceStartupDecisions(t *testing.T) {
	tests := []struct {
		name         string
		decisions    []Decision
		wantMode     StartupMode
		wantBlocked  []string
		wantReadOnly []string
		wantDisabled []string
	}{
		{
			name: "all ok starts writable",
			decisions: []Decision{
				ok(RoleMeta),
				ok(RoleMetaRaftLog),
				ok(RoleGroupState),
				ok(RoleIncidentState),
			},
			wantMode: StartupModeWritable,
		},
		{
			name: "optional receipt failure disables feature but starts writable",
			decisions: []Decision{
				ok(RoleMeta),
				failed(RoleReceipts, DecisionOpenFailed, errors.New("lock held")),
			},
			wantMode:     StartupModeWritable,
			wantDisabled: []string{"heal-receipt"},
		},
		{
			name: "read only eligible group failure starts read only",
			decisions: []Decision{
				ok(RoleMeta),
				failed(RoleGroupState, DecisionOpenFailed, errors.New("value log corrupt")),
			},
			wantMode:     StartupModeReadOnly,
			wantReadOnly: []string{"group_state"},
		},
		{
			name: "critical metadata failure blocks start",
			decisions: []Decision{
				failed(RoleMeta, DecisionOpenFailed, errors.New("manifest missing")),
				ok(RoleGroupState),
			},
			wantMode:    StartupModeBlocked,
			wantBlocked: []string{"meta"},
		},
		{
			name: "quarantine required blocks until explicit transaction succeeds",
			decisions: []Decision{
				ok(RoleMeta),
				{Role: RoleVolumeCatalog, Status: DecisionQuarantineRequired, Action: RecoveryActionNeedsQuarantine, Reason: "manifest mismatch"},
			},
			wantMode:    StartupModeBlocked,
			wantBlocked: []string{"volume_catalog"},
		},
		{
			name: "unknown role blocks start",
			decisions: []Decision{
				ok(RoleMeta),
				{Role: Role("new_role"), Status: DecisionUnknownRole, Reason: "not registered"},
			},
			wantMode:    StartupModeBlocked,
			wantBlocked: []string{"new_role"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReduceStartupDecisions(DefaultRegistry(), tt.decisions)
			assert.Equal(t, tt.wantMode, got.Mode)
			for _, want := range tt.wantBlocked {
				assertReasonContains(t, got.BlockedReasons, want)
			}
			for _, want := range tt.wantReadOnly {
				assertReasonContains(t, got.ReadOnlyReasons, want)
			}
			for _, want := range tt.wantDisabled {
				assert.Contains(t, got.DisabledFeatures, want)
			}
		})
	}
}

func TestReduceStartupDecisionsIsDeterministic(t *testing.T) {
	a := []Decision{
		failed(RoleGroupState, DecisionOpenFailed, errors.New("bad group")),
		failed(RoleReceipts, DecisionOpenFailed, errors.New("bad receipts")),
		ok(RoleMeta),
	}
	b := []Decision{
		ok(RoleMeta),
		failed(RoleReceipts, DecisionOpenFailed, errors.New("bad receipts")),
		failed(RoleGroupState, DecisionOpenFailed, errors.New("bad group")),
	}

	gotA := ReduceStartupDecisions(DefaultRegistry(), a)
	gotB := ReduceStartupDecisions(DefaultRegistry(), b)

	require.Equal(t, gotA.Mode, gotB.Mode)
	assert.Equal(t, gotA.BlockedReasons, gotB.BlockedReasons)
	assert.Equal(t, gotA.ReadOnlyReasons, gotB.ReadOnlyReasons)
	assert.Equal(t, gotA.DisabledFeatures, gotB.DisabledFeatures)
}

func ok(role Role) Decision {
	return Decision{Role: role, Status: DecisionOK, Action: RecoveryActionNone}
}

func failed(role Role, status DecisionStatus, err error) Decision {
	return Decision{Role: role, Status: status, Err: err, Reason: err.Error()}
}

func assertReasonContains(t *testing.T, reasons []string, want string) {
	t.Helper()
	for _, reason := range reasons {
		if assert.Contains(t, reason, want) {
			return
		}
	}
	assert.Failf(t, "reason not found", "no reason in %v contained %q", reasons, want)
}
