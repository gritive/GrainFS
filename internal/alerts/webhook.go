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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
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

// AlertCfgReader is the slice of cluster-config that the webhook dispatcher
// reads on every Send so URL/secret rotations land without a restart.
// *cluster.ClusterConfig satisfies this contract.
type AlertCfgReader interface {
	AlertWebhook() string
	AlertWebhookSecretWrapped() []byte
}

// SecretDecrypter unwraps the cluster-config secret blob produced by
// EncryptWithAAD. *encrypt.Encryptor satisfies this.
type SecretDecrypter interface {
	DecryptWithAAD(ciphertext, aad []byte) ([]byte, error)
}

// Dispatcher delivers Alerts to a single webhook URL.
//
// Two modes:
//   - Static (NewDispatcher): URL + Options.Secret are fixed at construction.
//     Used by tests and any caller that does not run under cluster-config.
//   - Live (NewDispatcherWithConfig): URL + wrapped secret are read from an
//     AlertCfgReader on every Send so rotations via cluster-config PATCH take
//     effect without restart. secretAAD must be supplied for unwrap.
type Dispatcher struct {
	// Static config (used when cfg == nil).
	url string

	// Live config (used when cfg != nil); cfg.AlertWebhook() returning "" makes
	// Send a no-op exactly like the empty static URL case.
	cfg       AlertCfgReader
	enc       SecretDecrypter
	secretAAD []byte
	alertKind string // bounded enum used as the alert_kind metric label

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

	// lastDecryptWarnAt rate-limits observeSecretDecryptFailure warn logs to
	// once per minute per Dispatcher so a persistent decrypt failure cannot
	// flood the log. Mu-protected.
	lastDecryptWarnAt time.Time

	// Actor fields (이번 PR에서 도입). 기존 mu/lastSent/inFlight/lastDecryptWarnAt는
	// 다음 Task들에서 dispatchEnv로 이주 후 제거.
	inbox        chan dispatchCmd
	releaseInbox chan dispatchCmd
	stop         chan struct{}
	done         chan struct{}

	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerWG     sync.WaitGroup
	started      atomic.Bool
	stopping     atomic.Bool
	stopOnce     sync.Once

	lastDropWarnAt atomic.Int64

	envPtr *dispatchEnv
}

// NewDispatcher constructs a static-config Dispatcher. An empty url turns Send
// into a no-op so operators who never set --alert-webhook are not punished.
func NewDispatcher(url string, opts Options, onFailure FailureCallback) *Dispatcher {
	d := newDispatcher(opts, onFailure)
	d.url = url
	d.envPtr.url = url
	return d
}

// NewDispatcherWithConfig constructs a Dispatcher that reads URL + wrapped
// secret from cfg on every Send. enc unwraps the secret using secretAAD; if
// enc is nil or the wrapped secret is empty, no signature header is written
// (matches the empty-secret behaviour of the static constructor). cfg.AlertWebhook()
// returning "" makes Send a no-op so cluster-config can disable alerts live.
//
// Options.Secret is ignored in this mode — the live wrapped secret takes
// precedence so a stale flag value cannot shadow a rotated cluster-config secret.
//
// alertKind is recorded as the alert_kind label on
// WebhookSignatureDecryptFailureTotal so multi-dispatcher deployments can
// distinguish which dispatcher saw stale wrapped secrets after rotate-key.
// Use a small bounded enum (e.g., "cluster", "degraded", "incident").
func NewDispatcherWithConfig(cfg AlertCfgReader, enc SecretDecrypter, secretAAD []byte, opts Options, onFailure FailureCallback, alertKind string) *Dispatcher {
	d := newDispatcher(opts, onFailure)
	d.cfg = cfg
	d.enc = enc
	d.secretAAD = secretAAD
	d.alertKind = alertKind
	d.envPtr.cfg = cfg
	d.envPtr.enc = enc
	d.envPtr.secretAAD = secretAAD
	d.envPtr.alertKind = alertKind
	return d
}

func newDispatcher(opts Options, onFailure FailureCallback) *Dispatcher {
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
	d := &Dispatcher{
		opts:      opts,
		onFailure: onFailure,
		lastSent:  map[string]time.Time{},
		inFlight:  map[string]struct{}{},

		inbox:        make(chan dispatchCmd, 32),
		releaseInbox: make(chan dispatchCmd, 16),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	d.envPtr = &dispatchEnv{
		lastSent: map[string]time.Time{},
		inFlight: map[string]struct{}{},
		opts:     opts,
	}
	return d
}

// Send delivers a in best-effort fashion. Returns nil on success or after
// dedup suppression; returns the last delivery error after retry exhaustion
// (and invokes onFailure once before returning).
//
// Send is synchronous so the caller controls scheduling. Wrap in a goroutine
// when calling from a hot path.
func (d *Dispatcher) Send(a Alert) error {
	// Resolve URL + secret at fire time so cluster-config rotations land
	// without a restart. Static-mode dispatchers fall through with the
	// constructor-supplied url and Options.Secret.
	url, secret := d.resolveLive()
	if url == "" {
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
			// ctx-aware sleep (deliver는 다음 task부터 ctx 받음. 여기서는
			// 내부 context.Background() 사용 — graceful Stop은 후속 Task에서)
			timer := time.NewTimer(d.backoff(attempt))
			<-timer.C
		}
		lastErr = d.deliver(url, secret, body)
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

// resolveLive returns the URL + signing secret to use for the next delivery.
// In live (cluster-config) mode the values come from the AlertCfgReader and
// SecretDecrypter on every call so a config PATCH between sends lands without
// a restart. In static mode the constructor values are returned.
//
// On wrapped-secret decrypt failure (typically: stale wrapped blob after a
// cluster rotate-key) observeSecretDecryptFailure increments
// WebhookSignatureDecryptFailureTotal and emits a rate-limited warn log;
// the delivery still proceeds with secret="" so a single rotation regression
// never DoSes the alert pipeline.
func (d *Dispatcher) resolveLive() (string, string) {
	if d.cfg == nil {
		return d.url, d.opts.Secret
	}
	url := d.cfg.AlertWebhook()
	wrapped := d.cfg.AlertWebhookSecretWrapped()
	if len(wrapped) == 0 || d.enc == nil {
		return url, ""
	}
	secret, err := d.enc.DecryptWithAAD(wrapped, d.secretAAD)
	if err != nil {
		d.observeSecretDecryptFailure(err)
		return url, ""
	}
	return url, string(secret)
}

// deliver does one POST. 2xx → success. Anything else is a retryable error.
func (d *Dispatcher) deliver(url, secret string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if secret != "" {
		req.Header.Set("X-GrainFS-Signature", sign(body, secret))
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

// env returns the controller's owned dispatch environment. Test-only access path.
func (d *Dispatcher) env() *dispatchEnv {
	return d.envPtr
}

// Start launches the controller goroutine. Idempotent (CompareAndSwap guard).
func (d *Dispatcher) Start(ctx context.Context) {
	if !d.started.CompareAndSwap(false, true) {
		return
	}
	d.workerCtx, d.workerCancel = context.WithCancel(ctx)
	go d.controllerLoop()
}

func (d *Dispatcher) controllerLoop() {
	defer close(d.done)
	for {
		select {
		case <-d.stop:
			d.drainResidualSendCmds()
			return
		case cmd := <-d.inbox:
			cmd.apply(d.envPtr)
		case rcmd := <-d.releaseInbox:
			rcmd.apply(d.envPtr)
		}
	}
}

func (d *Dispatcher) drainResidualSendCmds() {
	for {
		select {
		case cmd := <-d.inbox:
			if _, ok := cmd.(sendCmd); ok {
				d.observeDrop(dropReasonStopped)
			}
		default:
			return
		}
	}
}

// Stop stub (정식 구현은 Task 9):
func (d *Dispatcher) Stop(ctx context.Context) error {
	if !d.started.Load() {
		return nil
	}
	d.stopOnce.Do(func() {
		d.stopping.Store(true)
		close(d.stop)
		<-d.done
	})
	return nil
}

// observeDrop records a drop event. Task 5에서 metric + warn log 추가.
func (d *Dispatcher) observeDrop(reason dropReason) {
	// Task 5에서 metric + warn log 추가
	_ = reason
}

// ErrEmptyURL is returned when a caller passes an empty webhook URL to a
// helper that requires one. Currently unused at the public surface but kept
// for callers building higher-level abstractions.
var ErrEmptyURL = errors.New("webhook URL is empty")

// observeSecretDecryptFailure is called when DecryptWithAAD fails inside
// resolveLive. It increments the per-(alert_kind, err_class) counter on
// every call and emits one warn log per minute per Dispatcher. Delivery
// continues unsigned — single rotation regression never silences the alert
// pipeline.
func (d *Dispatcher) observeSecretDecryptFailure(err error) {
	cls := classifyDecryptErr(err)
	metrics.WebhookSignatureDecryptFailureTotal.WithLabelValues(d.alertKind, cls).Inc()

	now := d.opts.Clock()
	d.mu.Lock()
	shouldLog := now.Sub(d.lastDecryptWarnAt) > time.Minute
	if shouldLog {
		d.lastDecryptWarnAt = now
	}
	d.mu.Unlock()

	if shouldLog {
		log.Warn().
			Str("event", "webhook_signature_decrypt_failure").
			Str("alert_kind", d.alertKind).
			Str("err_class", cls).
			Err(err).
			Msg("webhook signing secret decrypt failed; delivering unsigned. Likely stale wrapped-secret after cluster rotate-key — PATCH cluster_config with a fresh alert-webhook-secret-plaintext to re-wrap.")
	}
}

// classifyDecryptErr maps a DecryptWithAAD error to a small bounded enum.
// Metric labels MUST go through this function — never label with raw
// err.Error() (unbounded cardinality, leaks key material into series names).
func classifyDecryptErr(err error) string {
	if err == nil {
		return "none"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "key not found"), strings.Contains(msg, "unknown key id"):
		return "key_not_found"
	case strings.Contains(msg, "message authentication failed"),
		strings.Contains(msg, "aad"), strings.Contains(msg, "AAD"):
		return "aad_mismatch"
	case strings.Contains(msg, "magic header"),
		strings.Contains(msg, "too short"),
		strings.Contains(msg, "decode"), strings.Contains(msg, "unmarshal"),
		strings.Contains(msg, "header"):
		return "decode_error"
	default:
		return "other"
	}
}
