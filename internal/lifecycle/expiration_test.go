package lifecycle_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
)

func TestExpirationTrigger_Days_NextUTCMidnight(t *testing.T) {
	// LM at 2026-05-19 13:30 UTC, Days=2 → trigger 2026-05-22 00:00 UTC
	// (start-of-day(LM) + (Days+1) days).
	lm := time.Date(2026, 5, 19, 13, 30, 0, 0, time.UTC).Unix()
	trig := lifecycle.ExpirationTriggerDays(lm, 2)
	require.Equal(t, time.Date(2026, 5, 22, 0, 0, 0, 0, time.UTC), trig)
}

func TestExpirationTrigger_Days_AtMidnight(t *testing.T) {
	// LM exactly at UTC midnight; same rule applies.
	lm := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC).Unix()
	trig := lifecycle.ExpirationTriggerDays(lm, 1)
	require.Equal(t, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC), trig)
}

func TestExpirationTrigger_Days_LastSecondOfDay(t *testing.T) {
	// LM at 23:59:59 UTC; same day → start-of-day unchanged → Days=1 trigger
	// is 2 days later.
	lm := time.Date(2026, 5, 19, 23, 59, 59, 0, time.UTC).Unix()
	trig := lifecycle.ExpirationTriggerDays(lm, 1)
	require.Equal(t, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC), trig)
}

func TestExpirationTrigger_Days_NonUTCTimezone(t *testing.T) {
	// LM expressed in a non-UTC timezone but converted to Unix — same trigger.
	// 2026-05-19 13:30 KST (UTC+9) = 2026-05-19 04:30 UTC.
	kst := time.FixedZone("KST", 9*3600)
	lm := time.Date(2026, 5, 19, 13, 30, 0, 0, kst).Unix()
	trig := lifecycle.ExpirationTriggerDays(lm, 1)
	require.Equal(t, time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC), trig)
}

func TestExpirationTrigger_Date_PassThrough(t *testing.T) {
	d := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	require.Equal(t, d, lifecycle.ExpirationTriggerDate(d))
}
