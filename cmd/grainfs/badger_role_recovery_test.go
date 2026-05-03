package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/gritive/GrainFS/internal/badgerrole"
)

func TestBadgerStartupReducerBlocksCriticalMetaFailure(t *testing.T) {
	got := badgerrole.ReduceStartupDecisions(badgerrole.DefaultRegistry(), []badgerrole.Decision{
		{
			Role:   badgerrole.RoleMeta,
			Status: badgerrole.DecisionOpenFailed,
			Action: badgerrole.RecoveryActionBlockStart,
			Reason: "manifest missing",
			Err:    errors.New("manifest missing"),
		},
	})

	assert.Equal(t, badgerrole.StartupModeBlocked, got.Mode)
	assert.Contains(t, got.BlockedReasons[0], "meta")
}

func TestBadgerStartupReducerAdmitsReadOnlyForGroupFailure(t *testing.T) {
	got := badgerrole.ReduceStartupDecisions(badgerrole.DefaultRegistry(), []badgerrole.Decision{
		{Role: badgerrole.RoleMeta, Status: badgerrole.DecisionOK},
		{
			Role:    badgerrole.RoleGroupState,
			GroupID: "group-a",
			Status:  badgerrole.DecisionOpenFailed,
			Action:  badgerrole.RecoveryActionStartReadOnly,
			Reason:  "value log corrupt",
			Err:     errors.New("value log corrupt"),
		},
	})

	assert.Equal(t, badgerrole.StartupModeReadOnly, got.Mode)
	assert.Contains(t, got.ReadOnlyReasons[0], "group_state")
}

func TestRuntimeGroupInstantiationFailureDoesNotPanic(t *testing.T) {
	err := handleRuntimeGroupInstantiationError("group-a", errors.New("open failed"))
	assert.ErrorContains(t, err, "group-a")
}
