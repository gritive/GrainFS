package server

import "time"

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

func (s *AlertsState) StatusSnapshot() alertsStatusResponse {
	st := s.tracker.Status()
	resp := alertsStatusResponse{
		Degraded:          st.Degraded,
		Held:              st.Held,
		LastReason:        st.LastReason,
		LastResource:      st.LastResource,
		EnteredAt:         st.EnteredAt,
		FlapCount:         st.FlapCount,
		DeliveredOK:       s.deliveredOK.Load(),
		DeliveryFailed:    s.deliveryFailed.Load(),
		WebhookConfigured: s.dispatcher != nil,
	}
	if snap := s.lastFailed.Load(); snap != nil {
		resp.LastFailedType = snap.Alert.Type
		resp.LastFailedErr = snap.ErrMessage
		resp.LastFailedAt = snap.At
	}
	return resp
}

// ResendLastFailed re-fires the last-failed alert through the dispatcher.
// Returns (true, nil) when a snapshot was present and the resend was dispatched
// (fire-and-forget — the actual delivery result lands in onResult). The
// last-failed slot is cleared optimistically so the banner disappears on the
// next status poll; if the resend ultimately fails, onResult re-populates it.
func (s *AlertsState) ResendLastFailed() (bool, error) {
	snap := s.lastFailed.Load()
	if snap == nil {
		return false, nil
	}
	s.Send(snap.Alert)
	// Clear the failed slot optimistically so the banner disappears.
	s.lastFailed.CompareAndSwap(snap, nil)
	return true, nil
}
