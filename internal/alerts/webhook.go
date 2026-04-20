// Package alerts delivers operational alerts to a Slack-compatible webhook.
//
// Phase 16 Week 4 surface:
//   - Slack JSON body so the same URL works for Slack incoming webhooks AND
//     anything that accepts {"text": ...} (Mattermost, Discord, custom relays)
//   - Optional HMAC-SHA256 signature in X-GrainFS-Signature so the receiver
//     can verify the payload originated from this cluster's PSK.
//   - Per-(alert_type, resource) dedup with a configurable window — typically
//     10 minutes — so a flapping condition does not page on every flap.
//   - Bounded exponential backoff retry on 5xx and network errors. After the
//     retry budget is exhausted, the registered failure callback fires so the
//     server can persist an `alert_delivery_failed` event for the dashboard
//     banner and Force Resend button.
//
// Out of scope for this package: PagerDuty native event format (Phase 17),
// per-severity routing to multiple URLs (single URL today), and queueing
// across process restarts (callers persist via the eventstore).
package alerts

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Severity is the urgency tier surfaced both in the Slack message and in
// downstream routing. critical = wake someone up; warning = send to channel.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityWarning  Severity = "warning"
)

// Alert is a single operational event to be delivered via webhook.
type Alert struct {
	Type     string   // e.g. "raft_quorum_lost", "disk_full_imminent"
	Severity Severity // critical | warning
	Resource string   // node id / bucket / etc. — distinguishes dedup keys
	Message  string   // human-readable one-liner
	Time     time.Time
}

// FailureCallback is invoked once per Alert after all retries are exhausted.
// The Server uses it to record an `alert_delivery_failed` event so the
// dashboard banner can prompt the operator to investigate.
type FailureCallback func(Alert, error)

// Options configure a Dispatcher. Zero values pick safe defaults.
type Options struct {
	// Secret enables HMAC-SHA256 signing in X-GrainFS-Signature.
	// Empty string disables signing.
	Secret string
	// DedupWindow suppresses repeat alerts with the same (Type, Resource)
	// pair. Default 10 minutes. Set to 0 to disable.
	DedupWindow time.Duration
	// MaxRetries is the number of retry attempts AFTER the first try.
	// Default 5 (so up to 6 total attempts).
	MaxRetries int
	// BackoffBase is the initial retry delay; subsequent retries grow
	// exponentially up to BackoffCap. Defaults: 500ms / 30s.
	BackoffBase time.Duration
	BackoffCap  time.Duration
	// HTTPClient overrides the default 5s-timeout client (mainly for tests).
	HTTPClient *http.Client
	// Clock returns the current time. Defaults to time.Now. Tests inject
	// a fake clock to drive dedup deterministically.
	Clock func() time.Time
}

// Dispatcher delivers Alerts to a single webhook URL.
type Dispatcher struct {
	url       string
	opts      Options
	onFailure FailureCallback

	mu sync.Mutex
	// lastSent tracks dedup-key → last delivery timestamp. Entries are
	// written when a delivery completes (success OR failure — failure-path
	// recording protects against outage-storm webhook spam) and are read by
	// claimSend to suppress repeat pages inside the dedup window.
	//
	// INVARIANT: Alert.Resource (and thus the dedup key) is expected to be
	// low-cardinality — node ids, cluster ids, bucket names, small named
	// sets. Callers that want to dedup on high-cardinality values (per-object
	// keys, per-request ids) MUST hash or otherwise bound the set first, or
	// lastSent grows without bound. There is no automatic sweep: the current
	// production caller set (degraded_hold only) fits trivially in memory.
	// Revisit if a future high-cardinality caller appears.
	lastSent map[string]time.Time
	// inFlight tracks dedup-keys whose delivery is currently in progress
	// (claimed by claimSend, not yet released). Holding the key here while
	// HTTP retries run prevents a second concurrent Send with the same key
	// from bypassing dedup during the unlocked HTTP window.
	inFlight map[string]struct{}
}

// NewDispatcher constructs a Dispatcher. An empty url turns Send into a
// no-op so operators who never set --alert-webhook are not punished.
func NewDispatcher(url string, opts Options, onFailure FailureCallback) *Dispatcher {
	if opts.DedupWindow == 0 {
		opts.DedupWindow = 10 * time.Minute
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 5
	}
	if opts.BackoffBase == 0 {
		opts.BackoffBase = 500 * time.Millisecond
	}
	if opts.BackoffCap == 0 {
		opts.BackoffCap = 30 * time.Second
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Dispatcher{
		url:       url,
		opts:      opts,
		onFailure: onFailure,
		lastSent:  map[string]time.Time{},
		inFlight:  map[string]struct{}{},
	}
}

// Send delivers a in best-effort fashion. Returns nil on success or after
// dedup suppression; returns the last delivery error after retry exhaustion
// (and invokes onFailure once before returning).
//
// Send is synchronous so the caller controls scheduling. Wrap in a goroutine
// when calling from a hot path.
func (d *Dispatcher) Send(a Alert) error {
	if d.url == "" {
		return nil // operator declined to configure webhooks
	}
	if a.Time.IsZero() {
		a.Time = d.opts.Clock()
	}

	key := dedupKey(a)
	if !d.claimSend(key) {
		return nil
	}
	// defer guarantees release even if the HTTP transport or marshal path
	// panics — without it, a panic would leave inFlight holding the key
	// permanently and silently drop every future alert of this type/resource
	// until the process restarts.
	defer d.releaseInFlight(key)

	body, err := json.Marshal(slackPayload(a))
	if err != nil {
		return fmt.Errorf("marshal alert: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= d.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(d.backoff(attempt))
		}
		lastErr = d.deliver(body)
		if lastErr == nil {
			return nil
		}
	}

	if d.onFailure != nil {
		d.onFailure(a, lastErr)
	}
	return lastErr
}

// claimSend atomically checks dedup state and reserves the key for delivery.
// Returns true iff the caller owns this alert's delivery. False means either
// (a) a prior delivery is still in-flight, or (b) a prior delivery landed
// inside the dedup window. Dedup key is (Type, Resource); Severity is NOT
// part of the key so a warning→critical escalation is NOT suppressed.
func (d *Dispatcher) claimSend(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, busy := d.inFlight[key]; busy {
		return false
	}
	if d.opts.DedupWindow > 0 {
		if last, ok := d.lastSent[key]; ok {
			if d.opts.Clock().Sub(last) < d.opts.DedupWindow {
				return false
			}
		}
	}
	d.inFlight[key] = struct{}{}
	return true
}

// releaseInFlight drops the in-flight reservation and records the delivery
// time for future dedup. Records on BOTH success and failure so an outage
// storm (repeated 5xx from the receiver) doesn't produce webhook spam — the
// failed delivery is one page's worth of signal and the dedup window should
// still throttle follow-up pages for the same condition. The AlertsState
// Force Resend path remains available for operator-driven retry.
func (d *Dispatcher) releaseInFlight(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inFlight, key)
	if d.opts.DedupWindow > 0 {
		d.lastSent[key] = d.opts.Clock()
	}
}

func dedupKey(a Alert) string {
	return a.Type + "|" + a.Resource
}

// deliver does one POST. 2xx → success. Anything else is a retryable error.
func (d *Dispatcher) deliver(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.opts.Secret != "" {
		req.Header.Set("X-GrainFS-Signature", sign(body, d.opts.Secret))
	}

	resp, err := d.opts.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return fmt.Errorf("webhook responded %d", resp.StatusCode)
}

// backoff returns the wait duration before retry attempt #n (n>=1), capped
// at BackoffCap. Pure exponential — jitter intentionally omitted for
// deterministic testability; a future commit can add jitter once we hit
// real fanout problems.
func (d *Dispatcher) backoff(attempt int) time.Duration {
	delay := d.opts.BackoffBase << (attempt - 1)
	if delay <= 0 || delay > d.opts.BackoffCap {
		return d.opts.BackoffCap
	}
	return delay
}

// sign returns the hex-encoded HMAC-SHA256 of body keyed by secret.
func sign(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// slackPayload returns the JSON body for one Alert in Slack incoming-webhook
// format. The 'text' field is the only required Slack field — by sticking to
// it we maximise compatibility with Slack-clones (Mattermost, Discord webhooks
// configured for Slack-compat mode, custom relays).
func slackPayload(a Alert) map[string]any {
	icon := ":warning:"
	if a.Severity == SeverityCritical {
		icon = ":rotating_light:"
	}
	resource := ""
	if a.Resource != "" {
		resource = " (" + a.Resource + ")"
	}
	text := fmt.Sprintf("%s GrainFS [%s] %s%s — %s",
		icon, a.Severity, a.Type, resource, a.Message)
	return map[string]any{
		"text": text,
		"attachments": []map[string]any{
			{
				"color":     colorFor(a.Severity),
				"footer":    "grainfs",
				"ts":        a.Time.Unix(),
				"mrkdwn_in": []string{"text"},
				"fields": []map[string]any{
					{"title": "Type", "value": a.Type, "short": true},
					{"title": "Severity", "value": string(a.Severity), "short": true},
					{"title": "Resource", "value": a.Resource, "short": true},
				},
			},
		},
	}
}

func colorFor(s Severity) string {
	if s == SeverityCritical {
		return "danger"
	}
	return "warning"
}

// ErrEmptyURL is returned when a caller passes an empty webhook URL to a
// helper that requires one. Currently unused at the public surface but kept
// for callers building higher-level abstractions.
var ErrEmptyURL = errors.New("webhook URL is empty")
