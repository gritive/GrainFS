package adminapi

import (
	"encoding/json"
	"net/http"

	"github.com/gritive/GrainFS/internal/cluster"
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
// node's RaftMeta.Propose; tests inject a fake. Unused by GET; reserved for
// the PATCH path implemented in Task 10.
type ClusterConfigProposer interface {
	ProposeClusterConfigPatch(p cluster.ClusterConfigPatch) error
}

// ClusterConfigHandler serves GET/PATCH /v1/cluster/config over the admin UDS.
type ClusterConfigHandler struct {
	fsm      ClusterConfigReader
	proposer ClusterConfigProposer // raft propose; nil for read-only paths
}

// NewClusterConfigHandler constructs the handler. proposer may be nil when
// only GET is wired (Task 9); Task 10 wires the real raft proposer for PATCH.
func NewClusterConfigHandler(fsm ClusterConfigReader, proposer ClusterConfigProposer) *ClusterConfigHandler {
	return &ClusterConfigHandler{fsm: fsm, proposer: proposer}
}

func (h *ClusterConfigHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.serveGet(w, r)
	case http.MethodPatch:
		// Task 10 will replace this stub with the real propose path.
		http.Error(w, "PATCH /v1/cluster/config not implemented", http.StatusNotImplemented)
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
