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
	// OnResult is invoked from the controller goroutine after delivery
	// completes — once per accepted alert. The error is nil on success,
	// non-nil after all retries are exhausted or ctx cancellation. OnResult
	// must not block; persistent I/O must be fanned out via a separate
	// goroutine.
	OnResult func(Alert, error)
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
	opts Options

	// Actor fields. State (url, cfg, enc, secretAAD, alertKind, lastSent,
	// inFlight, lastDecryptWarnAt) lives in envPtr and is mutated only inside
	// the controller goroutine.
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
	// Legacy FailureCallback parameter is retained for signature compatibility
	// during the actor migration. Post-migration all production callers pass
	// nil and wire OnResult via Options instead. If a caller still supplies
	// onFailure (only relevant if a test does), we silently ignore it — the
	// expected delivery channel is Options.OnResult.
	_ = onFailure
	d := &Dispatcher{
		opts: opts,

		inbox:        make(chan dispatchCmd, 32),
		releaseInbox: make(chan dispatchCmd, 16),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	d.envPtr = &dispatchEnv{
		lastSent: map[string]time.Time{},
		inFlight: map[string]struct{}{},
		opts:     opts,
		onResult: opts.OnResult,
	}
	d.envPtr.spawn = d.spawnWorker
	return d
}

// Send delivers an alert in fire-and-forget mode. Returns immediately.
// Acceptance is visible only via metrics (AlertDispatchDroppedTotal).
// OnResult callback (Options.OnResult) is invoked from the controller
// goroutine after retry completes (success or final failure). OnResult must
// not block.
//
// Pre-Start, post-Stop, or inbox-full callers see the alert silently dropped
// with the corresponding reason label on AlertDispatchDroppedTotal.
func (d *Dispatcher) Send(a Alert) {
	notStarted := !d.started.Load()
	stopping := d.stopping.Load()
	if notStarted || stopping {
		reason := dropReasonNotStarted
		if stopping {
			reason = dropReasonStopped
		}
		d.observeDrop(reason)
		return
	}
	if a.Time.IsZero() {
		a.Time = d.opts.Clock()
	}
	select {
	case d.inbox <- sendCmd{alert: a}:
	default:
		d.observeDrop(dropReasonInboxFull)
	}
}

func dedupKey(a Alert) string {
	return a.Type + "|" + a.Resource
}

// spawnWorker starts an ephemeral delivery goroutine. Wired into envPtr.spawn
// by newDispatcher so sendCmd.apply can launch a worker without holding the
// controller goroutine. workerWG tracks the worker for Stop's drain.
func (d *Dispatcher) spawnWorker(a Alert, url, secret string) {
	d.workerWG.Add(1)
	go d.retryAndDeliver(d.workerCtx, a, url, secret)
}

// retryAndDeliver runs the bounded exponential backoff retry loop for a single
// alert. On every iteration it either delivers (and releases) or, on retry,
// sleeps until either the backoff elapses or ctx is cancelled. The result is
// always reported through releaseToController so the controller can update
// lastSent/inFlight and fire OnResult — the worker never touches that state
// directly.
func (d *Dispatcher) retryAndDeliver(ctx context.Context, alert Alert, url, secret string) {
	defer d.workerWG.Done()
	body, err := json.Marshal(slackPayload(alert))
	if err != nil {
		d.releaseToController(alert, fmt.Errorf("marshal alert: %w", err))
		return
	}
	var lastErr error
	for attempt := 0; attempt <= d.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(d.backoff(attempt)):
			case <-ctx.Done():
				d.releaseToController(alert, ctx.Err())
				return
			}
		}
		lastErr = d.deliver(ctx, url, secret, body)
		if lastErr == nil {
			d.releaseToController(alert, nil)
			return
		}
	}
	d.releaseToController(alert, lastErr)
	// Note: Options.OnResult is invoked from the controller's releaseCmd.apply,
	// so the release channel is the single source of result notification — no
	// duplicate failure callback here.
}

// releaseToController enqueues a releaseCmd for the controller. If the
// controller has already stopped (stop channel closed), the result is recorded
// directly on metrics so we still observe success/failure counts even when
// shutdown races the worker.
func (d *Dispatcher) releaseToController(a Alert, err error) {
	select {
	case d.releaseInbox <- releaseCmd{key: dedupKey(a), alert: a, err: err}:
	case <-d.stop:
		if err == nil {
			metrics.AlertDeliveryAttempts.WithLabelValues("success").Inc()
		} else {
			metrics.AlertDeliveryAttempts.WithLabelValues("failed").Inc()
		}
	}
}

// deliver does one POST. 2xx → success. Anything else is a retryable error.
// ctx propagates Stop's workerCancel into the HTTP transport so a long-blocked
// Do() unblocks promptly on shutdown.
func (d *Dispatcher) deliver(ctx context.Context, url, secret string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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

// observeDrop increments AlertDispatchDroppedTotal and emits a rate-limited
// warn log (at most once per minute per Dispatcher via CAS on lastDropWarnAt).
func (d *Dispatcher) observeDrop(reason dropReason) {
	alertKind := ""
	if d.envPtr != nil {
		alertKind = d.envPtr.alertKind
	}
	metrics.AlertDispatchDroppedTotal.
		WithLabelValues(alertKind, string(reason)).Inc()
	now := d.opts.Clock().UnixNano()
	prev := d.lastDropWarnAt.Load()
	if time.Duration(now-prev) > time.Minute &&
		d.lastDropWarnAt.CompareAndSwap(prev, now) {
		log.Warn().
			Str("event", "webhook_alert_dropped").
			Str("alert_kind", alertKind).
			Str("reason", string(reason)).
			Msg("alert dispatcher dropped alert")
	}
}

// ErrEmptyURL is returned when a caller passes an empty webhook URL to a
// helper that requires one. Currently unused at the public surface but kept
// for callers building higher-level abstractions.
var ErrEmptyURL = errors.New("webhook URL is empty")

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
