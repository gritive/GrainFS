package clusteradmin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// ClusterConfigResponse is the JSON shape for GET /v1/cluster/config.
type ClusterConfigResponse struct {
	Rev         uint64            `json:"rev"`
	UpdatedAtMs int64             `json:"updated_at_unix_ms"`
	Effective   map[string]any    `json:"effective"`
	Source      map[string]string `json:"source"` // "default" | "explicit"
}

// ClusterConfigReader is the subset of MetaFSM used by the handler.
type ClusterConfigReader interface {
	ClusterConfig() *cluster.ClusterConfig
}

// ClusterConfigProposer is the raft-side write path. In production this is the
// cluster.ClusterConfigProposer wired to MetaRaft.Propose; tests inject a
// fake that calls fsm.ApplyClusterConfigPatchForTest directly.
type ClusterConfigProposer interface {
	ProposeClusterConfigPatch(p cluster.ClusterConfigPatch) error
}

// ClusterConfigHandler serves GET/PATCH /v1/cluster/config over the admin UDS.
type ClusterConfigHandler struct {
	fsm       ClusterConfigReader
	proposer  ClusterConfigProposer // raft propose; nil for read-only paths
	encryptor *encrypt.Encryptor    // nil in --no-encryption mode; PATCH with secret then early-rejects 403
}

// NewClusterConfigHandler constructs the handler. proposer may be nil when
// only GET is wired; encryptor may be nil in --no-encryption mode (PATCH with
// alert-webhook-secret is then rejected with 403 before raft propose).
func NewClusterConfigHandler(fsm ClusterConfigReader, proposer ClusterConfigProposer, enc *encrypt.Encryptor) *ClusterConfigHandler {
	return &ClusterConfigHandler{fsm: fsm, proposer: proposer, encryptor: enc}
}

func (h *ClusterConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.serveGet(w, r)
	case http.MethodPatch:
		h.servePatch(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *ClusterConfigHandler) serveGet(w http.ResponseWriter, _ *http.Request) {
	cfg := h.fsm.ClusterConfig()

	effective := map[string]any{
		"balancer-enabled":               cfg.BalancerEnabled(),
		"balancer-imbalance-trigger-pct": cfg.BalancerImbalanceTriggerPct(),
		"balancer-imbalance-stop-pct":    cfg.BalancerImbalanceStopPct(),
		"balancer-migration-rate":        cfg.BalancerMigrationRate(),
		"balancer-leader-tenure-min":     cfg.BalancerLeaderTenureMin().String(),
		"balancer-warmup-timeout":        cfg.BalancerWarmupTimeout().String(),
		"balancer-cb-threshold":          cfg.BalancerCBThreshold(),
		"balancer-migration-max-retries": cfg.BalancerMigrationMaxRetries(),
		"balancer-migration-pending-ttl": cfg.BalancerMigrationPendingTTL().String(),
		"balancer-gossip-interval":       cfg.BalancerGossipInterval().String(),
		"alert-webhook":                  cfg.AlertWebhook(),
		"alert-webhook-secret":           redactedSecret(cfg.AlertWebhookSecretWrapped()),
		"disk-warn-threshold":            cfg.DiskWarnFrac(),
		"disk-critical-threshold":        cfg.DiskCriticalFrac(),
		"snapshot-interval":              cfg.SnapshotInterval().String(),
		"snapshot-retain":                cfg.SnapshotRetain(),
	}
	keys := cluster.AllConfigKeys()
	source := make(map[string]string, len(keys))
	for _, k := range keys {
		source[k] = cfg.SourceForKey(k)
	}

	out := ClusterConfigResponse{
		Rev:         cfg.Rev(),
		UpdatedAtMs: cfg.UpdatedAt().UnixMilli(),
		Effective:   effective,
		Source:      source,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// redactedSecret returns "" when no secret is set and "<redacted>" otherwise.
// The wire never carries the plaintext or even the ciphertext — operators
// rotate via PATCH with a fresh value.
func redactedSecret(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return "<redacted>"
}

// ClusterConfigPatchRequest is the JSON body for PATCH /v1/cluster/config.
// Field names mirror the kebab-case config keys. Pointers distinguish absent
// from zero. `reset_keys` removes explicit overrides. `alert-webhook-secret`
// is the plaintext; the handler wraps it via EncryptWithAAD before propose.
type ClusterConfigPatchRequest struct {
	BalancerEnabled             *bool    `json:"balancer-enabled,omitempty"`
	BalancerImbalanceTriggerPct *float64 `json:"balancer-imbalance-trigger-pct,omitempty"`
	BalancerImbalanceStopPct    *float64 `json:"balancer-imbalance-stop-pct,omitempty"`
	BalancerMigrationRate       *int32   `json:"balancer-migration-rate,omitempty"`
	BalancerLeaderTenureMin     *string  `json:"balancer-leader-tenure-min,omitempty"` // ParseDuration
	BalancerWarmupTimeout       *string  `json:"balancer-warmup-timeout,omitempty"`
	BalancerCBThreshold         *float64 `json:"balancer-cb-threshold,omitempty"`
	BalancerMigrationMaxRetries *int32   `json:"balancer-migration-max-retries,omitempty"`
	BalancerMigrationPendingTTL *string  `json:"balancer-migration-pending-ttl,omitempty"`
	BalancerGossipInterval      *string  `json:"balancer-gossip-interval,omitempty"`
	AlertWebhook                *string  `json:"alert-webhook,omitempty"`
	AlertWebhookSecretPlaintext *string  `json:"alert-webhook-secret,omitempty"`
	DiskWarnFrac                *float64 `json:"disk-warn-threshold,omitempty"`
	DiskCriticalFrac            *float64 `json:"disk-critical-threshold,omitempty"`
	SnapshotInterval            *string  `json:"snapshot-interval,omitempty"` // ParseDuration; "0" disables
	SnapshotRetain              *int32   `json:"snapshot-retain,omitempty"`
	ResetKeys                   []string `json:"reset_keys,omitempty"`
}

// errSecretRequiresEncryption signals that a PATCH set the webhook secret
// while the node has --no-encryption.
var errSecretRequiresEncryption = errors.New("alert-webhook-secret requires encryption to be enabled")

func (h *ClusterConfigHandler) servePatch(w http.ResponseWriter, r *http.Request) {
	if h.proposer == nil {
		http.Error(w, "PATCH /v1/cluster/config requires a configured raft proposer", http.StatusServiceUnavailable)
		return
	}

	var body ClusterConfigPatchRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	patch, err := body.toPatch(h.encryptor)
	if err != nil {
		if errors.Is(err, errSecretRequiresEncryption) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if rev := r.Header.Get("If-Match-Rev"); rev != "" {
		n, perr := strconv.ParseUint(rev, 10, 64)
		if perr != nil {
			http.Error(w, "invalid If-Match-Rev: "+perr.Error(), http.StatusBadRequest)
			return
		}
		patch.ExpectedRev = n
	}

	// Operator-side audit log: emitted BEFORE propose so even a failed propose
	// is traceable. peer_uid is best-effort (UDS SO_PEERCRED); 0 when unknown.
	pc := peerUIDFrom(r)
	log.Info().
		Str("event", "cluster_config_patch_received").
		Uint32("actor_uid", pc.UID).
		Bool("actor_uid_resolved", pc.Resolved).
		Msg("cluster config PATCH received")

	if err := h.proposer.ProposeClusterConfigPatch(patch); err != nil {
		switch {
		case errors.Is(err, cluster.ErrClusterConfigCAS):
			http.Error(w, err.Error(), http.StatusConflict)
		case strings.Contains(err.Error(), "encryption disabled"):
			http.Error(w, err.Error(), http.StatusForbidden)
		case strings.Contains(err.Error(), "invalid"):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]uint64{"rev": h.fsm.ClusterConfig().Rev()})
}

// toPatch maps the JSON request into a cluster.ClusterConfigPatch, parsing
// Duration strings and wrapping alert-webhook-secret via EncryptWithAAD with
// cluster.ClusterConfigAlertSecretAAD (A2 plan-eng-review: per-field AAD binds
// the ciphertext to this config field and prevents cross-substitution).
func (req ClusterConfigPatchRequest) toPatch(enc *encrypt.Encryptor) (cluster.ClusterConfigPatch, error) {
	var p cluster.ClusterConfigPatch

	p.BalancerEnabled = req.BalancerEnabled
	p.BalancerImbalanceTriggerPct = req.BalancerImbalanceTriggerPct
	p.BalancerImbalanceStopPct = req.BalancerImbalanceStopPct
	p.BalancerMigrationRate = req.BalancerMigrationRate
	p.BalancerCBThreshold = req.BalancerCBThreshold
	p.BalancerMigrationMaxRetries = req.BalancerMigrationMaxRetries
	p.AlertWebhook = req.AlertWebhook
	p.DiskWarnFrac = req.DiskWarnFrac
	p.DiskCriticalFrac = req.DiskCriticalFrac
	p.SnapshotRetain = req.SnapshotRetain
	p.ResetKeys = req.ResetKeys

	parseDur := func(name string, s *string) (*time.Duration, error) {
		if s == nil {
			return nil, nil
		}
		d, err := time.ParseDuration(*s)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", name, err)
		}
		return &d, nil
	}
	var err error
	if p.BalancerLeaderTenureMin, err = parseDur("balancer-leader-tenure-min", req.BalancerLeaderTenureMin); err != nil {
		return p, err
	}
	if p.BalancerWarmupTimeout, err = parseDur("balancer-warmup-timeout", req.BalancerWarmupTimeout); err != nil {
		return p, err
	}
	if p.BalancerMigrationPendingTTL, err = parseDur("balancer-migration-pending-ttl", req.BalancerMigrationPendingTTL); err != nil {
		return p, err
	}
	if p.BalancerGossipInterval, err = parseDur("balancer-gossip-interval", req.BalancerGossipInterval); err != nil {
		return p, err
	}
	if p.SnapshotInterval, err = parseDur("snapshot-interval", req.SnapshotInterval); err != nil {
		return p, err
	}

	if req.AlertWebhookSecretPlaintext != nil {
		if enc == nil {
			return p, errSecretRequiresEncryption
		}
		wrapped, werr := enc.EncryptWithAAD([]byte(*req.AlertWebhookSecretPlaintext), cluster.ClusterConfigAlertSecretAAD)
		if werr != nil {
			return p, fmt.Errorf("wrap alert-webhook-secret: %w", werr)
		}
		p.AlertWebhookSecretWrapped = wrapped
	}
	return p, nil
}
