package alerts

import (
	"sync"
	"time"
)

// DegradedConfig parameterises a DegradedTracker. Zero values pick the
// defaults from the Phase 16 design doc:
//
//	ExitStableWindow = 30s   (must see 30s of healthy signal before exit)
//	FlapWindow       = 5min  (sliding window for flap counting)
//	FlapThreshold    = 3     (>3 transitions → hold in degraded)
type DegradedConfig struct {
	ExitStableWindow time.Duration
	FlapWindow       time.Duration
	FlapThreshold    int
	Clock            func() time.Time
	// OnHold fires once when the flap counter trips and the tracker enters
	// hold mode. Servers wire this to the alerts.Dispatcher so a flapping
	// component pages oncall (the actual "alert" wired up at the use site).
	OnHold func(reason string)
	// OnStateChange fires on every degraded↔healthy transition, with the new
	// Degraded() value. Callers use this to keep downstream mirrors (e.g.,
	// Prometheus gauges) bit-exact-consistent with the tracker's own view.
	//
	// IMPORTANT: This callback runs WHILE the tracker's internal lock is held.
	// That is deliberate — Prometheus gauge.Set is concurrent-safe and trivially
	// fast, so coupling it to the state transition eliminates the window where
	// a concurrent Report could flip state between the tracker update and the
	// mirror update.
	//
	// The callback MUST NOT call back into the tracker (Report/Degraded/Status)
	// or it will deadlock. Contrast with OnHold, which runs OUTSIDE the lock
	// because it fires a webhook (slow, potentially re-entrant).
	OnStateChange func(degraded bool)
}

// DegradedStatus is the snapshot returned from Tracker.Status, intended for
// the dashboard banner and the /api/admin/alerts/status endpoint.
type DegradedStatus struct {
	Degraded     bool
	Held         bool   // true when the flap counter is keeping us degraded
	LastReason   string // err_code that pushed us into degraded
	LastResource string // bucket/key/node-id, depending on caller
	EnteredAt    time.Time
	FlapCount    int // transitions inside the current flap window
}

// DegradedTracker tracks a binary "is the system degraded?" signal with
// hysteresis and flap protection. Callers Report(true, reason, resource) on
// every fault and Report(false, "", "") on every healthy heartbeat; the
// tracker collapses noisy signals into a stable boolean for the dashboard.
//
// All methods are safe for concurrent use.
type DegradedTracker struct {
	cfg DegradedConfig

	mu              sync.Mutex
	degraded        bool
	held            bool
	lastReason      string
	lastResource    string
	enteredAt       time.Time
	healthySince    time.Time // most recent time we saw a healthy report after a fault
	transitions    []time.Time // recent fault→heal transition timestamps
	holdReleaseAt  time.Time   // when held=true, the time after which hold can release
}

// NewDegradedTracker constructs a tracker with safe defaults applied.
func NewDegradedTracker(cfg DegradedConfig) *DegradedTracker {
	// ExitStableWindow zero is intentional ("exit immediately on healthy
	// report") and useful for tests; only fall back to the production
	// default when the field is left negative.
	if cfg.ExitStableWindow < 0 {
		cfg.ExitStableWindow = 30 * time.Second
	}
	if cfg.ExitStableWindow == 0 && cfg.FlapWindow == 0 && cfg.FlapThreshold == 0 && cfg.Clock == nil {
		// Fully zero-value config = "give me production defaults"; preserve
		// the prior behavior so callers that pass DegradedConfig{} keep the
		// 30s/5min/3-flap defaults instead of getting an exit-on-tick tracker.
		cfg.ExitStableWindow = 30 * time.Second
	}
	if cfg.FlapWindow == 0 {
		cfg.FlapWindow = 5 * time.Minute
	}
	if cfg.FlapThreshold == 0 {
		cfg.FlapThreshold = 3
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &DegradedTracker{cfg: cfg}
}

// Report feeds one health observation into the tracker.
//   - faulty=true: reason and resource describe what failed.
//   - faulty=false: tracker treats it as a healthy heartbeat; the other
//     two args are ignored.
func (t *DegradedTracker) Report(faulty bool, reason, resource string) {
	var holdReason string
	defer func() {
		// Invoke OnHold AFTER releasing the lock so the callback can
		// safely call back into this tracker (or send a webhook) without
		// deadlocking. Synchronous so tests can assert on its side effects
		// immediately after Report returns.
		if holdReason != "" && t.cfg.OnHold != nil {
			t.cfg.OnHold(holdReason)
		}
	}()
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.cfg.Clock()

	if faulty {
		// Entry is immediate (no warm-up window) — operators want to know
		// the moment something is wrong. Every fresh entry into degraded
		// counts as one transition for the flap counter (whether we just
		// exited or are still mid-recovery from a previous flap).
		wasHealthy := !t.degraded
		if !t.degraded {
			t.enteredAt = now
			t.transitions = append(t.transitions, now)
			if r := t.checkFlapThresholdLocked(now); r != "" {
				holdReason = r
			}
		} else if !t.healthySince.IsZero() {
			// We were already degraded but recovery had started; the new
			// fault is also a flap signal.
			t.transitions = append(t.transitions, now)
			if r := t.checkFlapThresholdLocked(now); r != "" {
				holdReason = r
			}
		}
		t.degraded = true
		t.lastReason = reason
		t.lastResource = resource
		t.healthySince = time.Time{}
		if wasHealthy && t.cfg.OnStateChange != nil {
			// Fire inside the lock so gauge mirrors stay bit-exact-consistent
			// with the tracker's view. See DegradedConfig.OnStateChange godoc.
			t.cfg.OnStateChange(true)
		}
		return
	}

	// Healthy report.
	if !t.degraded {
		// Already healthy — nothing to do.
		return
	}
	if t.healthySince.IsZero() {
		t.healthySince = now
	}
	if t.held && now.After(t.holdReleaseAt) {
		// Hold expired — drop hold state. We still need ExitStableWindow
		// of continuous healthy signal to actually exit.
		t.held = false
	}
	if t.held {
		return
	}
	if now.Sub(t.healthySince) >= t.cfg.ExitStableWindow {
		t.degraded = false
		t.healthySince = time.Time{}
		if t.cfg.OnStateChange != nil {
			t.cfg.OnStateChange(false)
		}
	}
}

// Degraded returns whether the tracker currently considers the system
// degraded. The dashboard / admin API reads this on every poll.
func (t *DegradedTracker) Degraded() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.degraded
}

// Status returns a value-copy snapshot — mutating the result has no effect
// on the tracker.
func (t *DegradedTracker) Status() DegradedStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return DegradedStatus{
		Degraded:     t.degraded,
		Held:         t.held,
		LastReason:   t.lastReason,
		LastResource: t.lastResource,
		EnteredAt:    t.enteredAt,
		FlapCount:    len(t.recentTransitionsLocked(t.cfg.Clock())),
	}
}

// checkFlapThresholdLocked trims old transitions and trips hold mode if
// the remaining count meets FlapThreshold. Returns a non-empty reason
// string if the caller should fire OnHold (after releasing the lock).
func (t *DegradedTracker) checkFlapThresholdLocked(now time.Time) string {
	t.transitions = t.recentTransitionsLocked(now)
	if t.held || len(t.transitions) < t.cfg.FlapThreshold {
		return ""
	}
	t.held = true
	// Hold lasts one full FlapWindow from the last transition — a
	// component that flapped FlapThreshold+ times must demonstrate a
	// clean window before we trust it again.
	t.holdReleaseAt = now.Add(t.cfg.FlapWindow)
	return "flap threshold exceeded"
}

// recentTransitionsLocked returns the subset of t.transitions that fall
// within the FlapWindow ending at now.
func (t *DegradedTracker) recentTransitionsLocked(now time.Time) []time.Time {
	cutoff := now.Add(-t.cfg.FlapWindow)
	for i, ts := range t.transitions {
		if ts.After(cutoff) {
			return t.transitions[i:]
		}
	}
	return nil
}
