// Phase 16 Week 4 — alert state surfaced to the dashboard banner.
//
// Endpoints (registered in server.go):
//
//	GET  /api/admin/alerts/status   → JSON snapshot for the banner
//	POST /api/admin/alerts/resend   → operator-driven retry of the last
//	                                  failed alert (Force Resend button)
package server

import (
	"context"
	"sync"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/metrics"
)

// AlertsState owns the dashboard's view of the cluster's alert plumbing:
// degraded tracker + last-failed alert + delivery counters. The Server
// holds a single AlertsState and exposes it via /api/admin/alerts/*.
type AlertsState struct {
	dispatcher *alerts.Dispatcher
	tracker    *alerts.DegradedTracker

	mu             sync.Mutex
	lastFailed     *alerts.Alert
	lastFailedErr  string
	lastFailedAt   time.Time
	deliveredOK    uint64
	deliveryFailed uint64
}

// NewAlertsState wires the dispatcher and tracker together. The dispatcher's
// failure callback is captured here so the state can record the last failed
// alert for the Force Resend button.
func NewAlertsState(webhookURL string, opts alerts.Options, trackerCfg alerts.DegradedConfig) *AlertsState {
	s := &AlertsState{}
	s.dispatcher = alerts.NewDispatcher(webhookURL, opts, func(a alerts.Alert, err error) {
		s.recordFailure(a, err)
	})
	// When the tracker trips into hold mode, send a critical webhook so
	// the on-call human knows the system is being held degraded for them.
	// Fire the send in a goroutine: OnHold already runs outside the tracker
	// lock, but it runs on the caller's goroutine (scrubber, raft monitor,
	// disk collector). A synchronous webhook retry could block that caller
	// for tens of seconds. The dispatcher's onFailure callback still records
	// delivery failures for the dashboard banner and Force Resend, so
	// fire-and-forget is safe.
	trackerCfg.OnHold = func(reason string) {
		go func() {
			_ = s.dispatcher.Send(alerts.Alert{
				Type:     "degraded_hold",
				Severity: alerts.SeverityCritical,
				Message:  "Tracker held in degraded mode: " + reason,
			})
		}()
	}
	// Mirror tracker state into the Prometheus gauge. Runs in the actor
	// goroutine (see DegradedConfig.OnStateChange godoc), so the gauge
	// cannot observe a stale value between a concurrent Report and the
	// mirror update.
	trackerCfg.OnStateChange = func(degraded bool) {
		if degraded {
			metrics.Degraded.Set(1)
		} else {
			metrics.Degraded.Set(0)
		}
	}
	s.tracker = alerts.NewDegradedTracker(trackerCfg)
	return s
}

// Send fires an alert through the dispatcher and updates counters/metrics.
// Safe to call from any goroutine.
func (s *AlertsState) Send(a alerts.Alert) error {
	err := s.dispatcher.Send(a)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.deliveryFailed++
		metrics.AlertDeliveryAttempts.WithLabelValues("failed").Inc()
		return err
	}
	s.deliveredOK++
	metrics.AlertDeliveryAttempts.WithLabelValues("success").Inc()
	return nil
}

// Tracker exposes the DegradedTracker for callers (scrubber, raft, disk
// monitor) that need to push fault/healthy reports. The gauge wrapper that
// used to live here was removed; gauge mirroring now happens inside the
// tracker via DegradedConfig.OnStateChange, which is bit-exact-consistent
// with tracker state.
func (s *AlertsState) Tracker() *alerts.DegradedTracker {
	return s.tracker
}

// Close stops the DegradedTracker actor goroutine. Call once when the server
// shuts down. Idempotent.
func (s *AlertsState) Close() {
	s.tracker.Stop()
}

// Alerts returns the AlertsState wired into this server, or nil if alerts
// were not configured. Used by other server components (raft monitor, disk
// collector) to push fault/healthy reports.
func (s *Server) Alerts() *AlertsState { return s.alerts }

func (s *AlertsState) recordFailure(a alerts.Alert, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFailed = &a
	s.lastFailedErr = err.Error()
	s.lastFailedAt = time.Now()
	metrics.AlertDeliveryFailedTotal.Inc()
}

// alertsStatusResponse mirrors the dashboard banner contract.
type alertsStatusResponse struct {
	Degraded          bool      `json:"degraded"`
	Held              bool      `json:"held"`
	LastReason        string    `json:"last_reason,omitempty"`
	LastResource      string    `json:"last_resource,omitempty"`
	EnteredAt         time.Time `json:"entered_at,omitempty"`
	FlapCount         int       `json:"flap_count"`
	DeliveredOK       uint64    `json:"delivered_ok"`
	DeliveryFailed    uint64    `json:"delivery_failed"`
	LastFailedType    string    `json:"last_failed_type,omitempty"`
	LastFailedErr     string    `json:"last_failed_err,omitempty"`
	LastFailedAt      time.Time `json:"last_failed_at,omitempty"`
	WebhookConfigured bool      `json:"webhook_configured"`
}

func (s *Server) registerAlertsAPI(h *server.Hertz) {
	if s.alerts == nil {
		return
	}
	h.GET("/api/admin/alerts/status", localhostOnly(), s.alertsStatus)
	h.POST("/api/admin/alerts/resend", localhostOnly(), s.alertsResend)
}

func (s *Server) alertsStatus(_ context.Context, c *app.RequestContext) {
	st := s.alerts.tracker.Status()
	s.alerts.mu.Lock()
	resp := alertsStatusResponse{
		Degraded:          st.Degraded,
		Held:              st.Held,
		LastReason:        st.LastReason,
		LastResource:      st.LastResource,
		EnteredAt:         st.EnteredAt,
		FlapCount:         st.FlapCount,
		DeliveredOK:       s.alerts.deliveredOK,
		DeliveryFailed:    s.alerts.deliveryFailed,
		WebhookConfigured: s.alerts.dispatcher != nil,
	}
	if s.alerts.lastFailed != nil {
		resp.LastFailedType = s.alerts.lastFailed.Type
		resp.LastFailedErr = s.alerts.lastFailedErr
		resp.LastFailedAt = s.alerts.lastFailedAt
	}
	s.alerts.mu.Unlock()
	c.JSON(consts.StatusOK, resp)
}

func (s *Server) alertsResend(_ context.Context, c *app.RequestContext) {
	s.alerts.mu.Lock()
	last := s.alerts.lastFailed
	s.alerts.mu.Unlock()
	if last == nil {
		c.JSON(consts.StatusOK, map[string]any{"resent": false, "reason": "no failed alert to resend"})
		return
	}
	if err := s.alerts.Send(*last); err != nil {
		c.JSON(consts.StatusBadGateway, map[string]any{"resent": false, "error": err.Error()})
		return
	}
	// Clear the failed slot so the banner disappears on next status poll.
	s.alerts.mu.Lock()
	s.alerts.lastFailed = nil
	s.alerts.lastFailedErr = ""
	s.alerts.mu.Unlock()
	c.JSON(consts.StatusOK, map[string]any{"resent": true})
}
