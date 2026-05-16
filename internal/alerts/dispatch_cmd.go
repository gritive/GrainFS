package alerts

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// dispatchCmd is the visitor interface for commands processed by the
// Dispatcher controller goroutine. Both sendCmd (from caller goroutines) and
// releaseCmd (from ephemeral worker goroutines) implement this so the
// controller loop body is a single cmd.apply(&env) call regardless of channel.
type dispatchCmd interface {
	apply(*dispatchEnv)
}

// sendCmd carries an alert from a caller to the controller for dedup/claim +
// ephemeral worker spawn.
type sendCmd struct {
	alert Alert
}

func (c sendCmd) apply(env *dispatchEnv) {
	key := dedupKey(c.alert)
	if _, busy := env.inFlight[key]; busy {
		return
	}
	if env.opts.DedupWindow > 0 {
		if last, ok := env.lastSent[key]; ok {
			if env.opts.Clock().Sub(last) < env.opts.DedupWindow {
				return
			}
		}
	}
	env.inFlight[key] = struct{}{}
	url, secret := env.resolveLive()
	if url == "" {
		delete(env.inFlight, key)
		return
	}
	if env.spawnTestHook != nil {
		env.spawnTestHook(c.alert, url, secret)
		return
	}
	env.spawn(c.alert, url, secret)
}

// releaseCmd carries delivery result from a worker to the controller for
// lastSent/inFlight bookkeeping and onResult callback.
type releaseCmd struct {
	key   string
	alert Alert
	err   error
}

func (c releaseCmd) apply(env *dispatchEnv) {
	delete(env.inFlight, c.key)
	if env.opts.DedupWindow > 0 {
		env.lastSent[c.key] = env.opts.Clock()
	}
	if env.onResult != nil {
		env.onResult(c.alert, c.err)
	}
}

// dispatchEnv is the controller's owned environment. Mutated only inside the
// controller goroutine (in cmd.apply) so no mutex is required.
type dispatchEnv struct {
	lastSent          map[string]time.Time
	inFlight          map[string]struct{}
	lastDecryptWarnAt time.Time

	opts      Options
	cfg       AlertCfgReader
	enc       SecretDecrypter
	secretAAD []byte
	alertKind string
	onResult  func(Alert, error)
	url       string

	// spawn invokes retryAndDeliver in a new goroutine in production. Tests
	// may set spawnTestHook to capture spawn invocations without running HTTP retry.
	spawn         func(alert Alert, url, secret string)
	spawnTestHook func(alert Alert, url, secret string)
}

func (env *dispatchEnv) resolveLive() (string, string) {
	if env.cfg == nil {
		return env.url, env.opts.Secret
	}
	url := env.cfg.AlertWebhook()
	wrapped := env.cfg.AlertWebhookSecretWrapped()
	if len(wrapped) == 0 || env.enc == nil {
		return url, ""
	}
	secret, err := env.enc.DecryptWithAAD(wrapped, env.secretAAD)
	if err != nil {
		env.observeSecretDecryptFailure(err)
		return url, ""
	}
	return url, string(secret)
}

func (env *dispatchEnv) observeSecretDecryptFailure(err error) {
	cls := classifyDecryptErr(err)
	metrics.WebhookSignatureDecryptFailureTotal.WithLabelValues(env.alertKind, cls).Inc()
	now := env.opts.Clock()
	if now.Sub(env.lastDecryptWarnAt) > time.Minute {
		env.lastDecryptWarnAt = now
		log.Warn().
			Str("event", "webhook_signature_decrypt_failure").
			Str("alert_kind", env.alertKind).
			Str("err_class", cls).
			Err(err).
			Msg("webhook signing secret decrypt failed; delivering unsigned. Likely stale wrapped-secret after cluster rotate-key — PATCH cluster_config with a fresh alert-webhook-secret-plaintext to re-wrap.")
	}
}

// dropReason enumerates the bounded set of labels for AlertDispatchDroppedTotal.
type dropReason string

const (
	dropReasonInboxFull  dropReason = "inbox_full"
	dropReasonNotStarted dropReason = "not_started"
	dropReasonStopped    dropReason = "stopped"
)

// Suppress unused warnings for symbols whose users come in later tasks.
var _ = fmt.Errorf
var _ = context.Canceled
