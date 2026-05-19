package lifecycle

import "time"

// ExpirationTriggerDays returns the wall-clock time at which an object
// last modified at lastModified (Unix seconds) becomes eligible for
// expiration under a Days=N rule. AWS / MinIO semantics: trigger is
// start-of-day(LM_UTC) + (N+1) days, i.e. the UTC midnight AFTER the
// N-th full day has elapsed. Simple LM + N*24h would be off by up to
// 23h59m59s relative to the wall-clock and breaks aws-cli interop.
func ExpirationTriggerDays(lastModified int64, days int) time.Time {
	mod := time.Unix(lastModified, 0).UTC()
	dayStart := time.Date(mod.Year(), mod.Month(), mod.Day(), 0, 0, 0, 0, time.UTC)
	return dayStart.AddDate(0, 0, days+1)
}

// ExpirationTriggerDate returns the rule's explicit Date trigger. The
// caller guarantees Date is UTC midnight (Validate enforces it).
func ExpirationTriggerDate(t time.Time) time.Time { return t }
