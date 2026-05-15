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
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := alertsStatusResponse{
		Degraded:          st.Degraded,
		Held:              st.Held,
		LastReason:        st.LastReason,
		LastResource:      st.LastResource,
		EnteredAt:         st.EnteredAt,
		FlapCount:         st.FlapCount,
		DeliveredOK:       s.deliveredOK,
		DeliveryFailed:    s.deliveryFailed,
		WebhookConfigured: s.dispatcher != nil,
	}
	if s.lastFailed != nil {
		resp.LastFailedType = s.lastFailed.Type
		resp.LastFailedErr = s.lastFailedErr
		resp.LastFailedAt = s.lastFailedAt
	}
	return resp
}

func (s *AlertsState) ResendLastFailed() (bool, error) {
	s.mu.Lock()
	last := s.lastFailed
	s.mu.Unlock()
	if last == nil {
		return false, nil
	}
	if err := s.Send(*last); err != nil {
		return false, err
	}
	// Clear the failed slot so the banner disappears on next status poll.
	s.mu.Lock()
	s.lastFailed = nil
	s.lastFailedErr = ""
	s.mu.Unlock()
	return true, nil
}
