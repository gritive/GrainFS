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
	// hold mode. Runs synchronously in the actor goroutine before Report()
	// returns. MUST NOT call Report/Degraded/Status synchronously — deadlock.
	// Use a goroutine if you need to call back into the tracker.
	OnHold func(reason string)
	// OnStateChange fires on every degraded↔healthy transition, with the new
	// Degraded() value. Runs synchronously in the actor goroutine before
	// Report() returns. MUST NOT call Report/Degraded/Status synchronously —
	// deadlock. The actor serializes this callback with all state access, so
	// the callback always sees a consistent snapshot.
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

// reportReq is sent to the actor goroutine by Report().
type reportReq struct {
	faulty   bool
	reason   string
	resource string
	reply    chan<- struct{}
}

// DegradedTracker tracks a binary "is the system degraded?" signal with
// hysteresis and flap protection. Callers Report(true, reason, resource) on
// every fault and Report(false, "", "") on every healthy heartbeat; the
// tracker collapses noisy signals into a stable boolean for the dashboard.
//
// The actor goroutine owns all mutable state — no mutex required. Report() is
// synchronous (blocks until processed). Degraded() and Status() are query
// calls that also block until answered.
//
// All methods are safe for concurrent use.
type DegradedTracker struct {
	cfg DegradedConfig

	reportCh   chan reportReq
	degradedCh chan chan bool
	statusCh   chan chan DegradedStatus

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewDegradedTracker constructs a tracker with safe defaults applied and
// starts the actor goroutine.
func NewDegradedTracker(cfg DegradedConfig) *DegradedTracker {
	if cfg.ExitStableWindow < 0 {
		cfg.ExitStableWindow = 30 * time.Second
	}
	if cfg.ExitStableWindow == 0 && cfg.FlapWindow == 0 && cfg.FlapThreshold == 0 && cfg.Clock == nil {
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

	t := &DegradedTracker{
		cfg:        cfg,
		reportCh:   make(chan reportReq, 16),
		degradedCh: make(chan chan bool, 1),
		statusCh:   make(chan chan DegradedStatus, 1),
		stopCh:     make(chan struct{}),
	}
	go t.run()
	return t
}

// Stop shuts down the actor goroutine. Idempotent.
func (t *DegradedTracker) Stop() {
	t.stopOnce.Do(func() { close(t.stopCh) })
}

// trackerState holds all mutable state owned by the actor goroutine.
type trackerState struct {
	degraded      bool
	held          bool
	lastReason    string
	lastResource  string
	enteredAt     time.Time
	healthySince  time.Time
	transitions   []time.Time
	holdReleaseAt time.Time
}

func (t *DegradedTracker) run() {
	var s trackerState
	for {
		select {
		case <-t.stopCh:
			return
		case req := <-t.reportCh:
			holdReason, stateChanged, newDegraded := t.processReport(&s, req.faulty, req.reason, req.resource)
			// Callbacks fire from actor goroutine before the ack — mirrors the original
			// sync behavior where Report() blocks until all callbacks complete.
			// Callbacks MUST NOT call Report/Degraded/Status synchronously; doing so
			// would deadlock because the actor is busy. Use a goroutine if needed.
			if stateChanged && t.cfg.OnStateChange != nil {
				t.cfg.OnStateChange(newDegraded)
			}
			if holdReason != "" && t.cfg.OnHold != nil {
				t.cfg.OnHold(holdReason)
			}
			req.reply <- struct{}{}
		case reply := <-t.degradedCh:
			reply <- s.degraded
		case reply := <-t.statusCh:
			reply <- DegradedStatus{
				Degraded:     s.degraded,
				Held:         s.held,
				LastReason:   s.lastReason,
				LastResource: s.lastResource,
				EnteredAt:    s.enteredAt,
				FlapCount:    len(recentTransitions(s.transitions, t.cfg.FlapWindow, t.cfg.Clock())),
			}
		}
	}
}

// processReport applies one health observation to the state machine.
// Returns (holdReason, stateChanged, newDegraded).
func (t *DegradedTracker) processReport(s *trackerState, faulty bool, reason, resource string) (string, bool, bool) {
	now := t.cfg.Clock()
	if faulty {
		wasHealthy := !s.degraded
		if !s.degraded {
			s.enteredAt = now
			s.transitions = append(s.transitions, now)
		} else if !s.healthySince.IsZero() {
			s.transitions = append(s.transitions, now)
		}
		holdReason := checkFlapThreshold(s, now, t.cfg.FlapWindow, t.cfg.FlapThreshold)
		s.degraded = true
		s.lastReason = reason
		s.lastResource = resource
		s.healthySince = time.Time{}
		if wasHealthy {
			return holdReason, true, true
		}
		return holdReason, false, false
	}

	// Healthy report.
	if !s.degraded {
		return "", false, false
	}
	if s.healthySince.IsZero() {
		s.healthySince = now
	}
	if s.held && now.After(s.holdReleaseAt) {
		s.held = false
	}
	if s.held {
		return "", false, false
	}
	if now.Sub(s.healthySince) >= t.cfg.ExitStableWindow {
		s.degraded = false
		s.healthySince = time.Time{}
		return "", true, false
	}
	return "", false, false
}

func checkFlapThreshold(s *trackerState, now time.Time, flapWindow time.Duration, flapThreshold int) string {
	s.transitions = recentTransitions(s.transitions, flapWindow, now)
	if s.held || len(s.transitions) < flapThreshold {
		return ""
	}
	s.held = true
	s.holdReleaseAt = now.Add(flapWindow)
	return "flap threshold exceeded"
}

func recentTransitions(transitions []time.Time, flapWindow time.Duration, now time.Time) []time.Time {
	cutoff := now.Add(-flapWindow)
	for i, ts := range transitions {
		if ts.After(cutoff) {
			if i == 0 {
				return transitions
			}
			// Copy to release the head of the backing array; without this the
			// slice header keeps the full historical allocation alive.
			trimmed := make([]time.Time, len(transitions)-i)
			copy(trimmed, transitions[i:])
			return trimmed
		}
	}
	return nil
}

// Report feeds one health observation into the tracker.
// Blocks until the actor processes it (ensures ordering with Degraded/Status).
func (t *DegradedTracker) Report(faulty bool, reason, resource string) {
	reply := make(chan struct{}, 1)
	select {
	case t.reportCh <- reportReq{faulty: faulty, reason: reason, resource: resource, reply: reply}:
		select {
		case <-reply:
		case <-t.stopCh:
		}
	case <-t.stopCh:
	}
}

// Degraded returns whether the tracker currently considers the system degraded.
func (t *DegradedTracker) Degraded() bool {
	reply := make(chan bool, 1)
	select {
	case t.degradedCh <- reply:
		select {
		case v := <-reply:
			return v
		case <-t.stopCh:
			return false
		}
	case <-t.stopCh:
		return false
	}
}

// Status returns a value-copy snapshot of the current tracker state.
func (t *DegradedTracker) Status() DegradedStatus {
	reply := make(chan DegradedStatus, 1)
	select {
	case t.statusCh <- reply:
		select {
		case v := <-reply:
			return v
		case <-t.stopCh:
			return DegradedStatus{}
		}
	case <-t.stopCh:
		return DegradedStatus{}
	}
}
