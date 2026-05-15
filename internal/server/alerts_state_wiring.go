package server

import (
	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/metrics"
)

// NewAlertsState wires the dispatcher and tracker together. The dispatcher's
// failure callback is captured here so the state can record the last failed
// alert for the Force Resend button.
func NewAlertsState(webhookURL string, opts alerts.Options, trackerCfg alerts.DegradedConfig) *AlertsState {
	return newAlertsStateFromDispatcher(trackerCfg, func(s *AlertsState) *alerts.Dispatcher {
		return alerts.NewDispatcher(webhookURL, opts, func(a alerts.Alert, err error) {
			s.recordFailure(a, err)
		})
	})
}

// NewAlertsStateWithConfig builds an AlertsState whose dispatcher reads its
// webhook URL + wrapped secret from cfg on every Send. This is the production
// wiring used by serveruntime: a cluster-config PATCH that flips the URL or
// rotates the secret takes effect on the next alert without a process restart.
//
// cfg must not be nil; enc may be nil (in which case the secret is treated as
// disabled even if the wrapped blob is populated, matching the static
// empty-secret path).
//
// alertKind is forwarded to the underlying Dispatcher and surfaces as the
// alert_kind label on WebhookSignatureDecryptFailureTotal so operators can
// tell which AlertsState saw stale wrapped secrets after a rotate-key.
func NewAlertsStateWithConfig(
	cfg alerts.AlertCfgReader,
	enc alerts.SecretDecrypter,
	secretAAD []byte,
	opts alerts.Options,
	trackerCfg alerts.DegradedConfig,
	alertKind string,
) *AlertsState {
	return newAlertsStateFromDispatcher(trackerCfg, func(s *AlertsState) *alerts.Dispatcher {
		return alerts.NewDispatcherWithConfig(cfg, enc, secretAAD, opts, func(a alerts.Alert, err error) {
			s.recordFailure(a, err)
		}, alertKind)
	})
}

func newAlertsStateFromDispatcher(trackerCfg alerts.DegradedConfig, build func(*AlertsState) *alerts.Dispatcher) *AlertsState {
	s := &AlertsState{}
	s.dispatcher = build(s)
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
		// Call secondary callbacks (e.g. Server.degradedFlag.Store) with a lock-free read.
		if cbs := s.secondaryCallbacks.Load(); cbs != nil {
			for _, cb := range *cbs {
				cb(degraded)
			}
		}
	}
	s.tracker = alerts.NewDegradedTracker(trackerCfg)
	return s
}
