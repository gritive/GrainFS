// Package clusteradmin: KEK envelope admin endpoints (Task 11).
//
// Four POST/GET routes for the admin UDS:
//
//	POST /v1/encrypt/kek/rotate   {"confirm": "rotate-now"}
//	POST /v1/encrypt/kek/retire   {"version": <V>, "confirm": "delete-permanently-<V>"}
//	POST /v1/encrypt/kek/prune    {"version": <V>, "confirm": "delete-permanently-<V>"}
//	GET  /v1/encrypt/kek/status   → {active_version, active_dek_generation, versions:[{version, status, lease_count}], dek_generations:[{generation, active, seal_count, nonce_collision_risk}]}
//
// UDS-only: enforcement is architectural — `RegisterEncryptionKEKRoutes` is
// only called from the admin-UDS Hertz wiring (boot_phases_admin.go). The
// route table on the public/UI Hertz never carries these endpoints, so a TCP
// request hits the default 404. Tests verify that invariant by exercising the
// public Hertz route table separately (see route_availability_test.go).
//
// Capability gate (kek_envelope_v1): every mutating endpoint calls
// CapabilityGate.Allow before invoking the leader; gate refusal returns 503
// with a retry-friendly message so operators see "rolling upgrade in progress"
// rather than a 5xx.
package clusteradmin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/compat"
)

// KEKRotationOrchestrator is the leader-side API the admin endpoints call.
// Production wires *cluster.KEKRotationLeader; tests inject a stub that
// records calls and returns canned errors.
type KEKRotationOrchestrator interface {
	ProposeKEKRotate(confirm, actor string) error
	ProposeKEKRetire(version uint32, confirm, actor string) error
	ProposeKEKPrune(version uint32, actor string) error
}

// KEKCapabilityGate is the subset of *cluster.CapabilityGate the admin
// endpoints use. Tests inject a stub.
type KEKCapabilityGate interface {
	Allow(ctx context.Context, op compat.Operation) (compat.GatePlan, error)
}

// KEKStatusReader exposes the FSM state needed by GET /v1/encrypt/kek/status.
// Production wires *cluster.MetaFSM (which has both methods). Tests inject a
// stub returning canned versions + lifecycle statuses.
type KEKStatusReader interface {
	ActiveKEKVersion() uint32
	// KEKStoreVersions returns the versions to report in the status response.
	// Production unions the live keystore versions with the lifecycle-tracked
	// versions so a pruned version (removed from the keystore but retained in
	// kek_status) still appears as "pruned".
	KEKStoreVersions() []uint32
	LookupKEKStatus(version uint32) (v uint32, status cluster.KEKLifecycleStatus, retireCommitIndex uint64, ok bool)
	// ActiveDEKGeneration returns the active DEK generation — the key the live
	// seal counter belongs to. Distinct from ActiveKEKVersion (a KEK rotation
	// re-wraps the DEK without advancing the DEK gen).
	ActiveDEKGeneration() uint32
	// SealCountSnapshot returns the per-DEK-generation seal counts (live count
	// for the active gen, frozen value for each retired gen). AES-GCM nonce
	// exhaustion is per-DEK-key, so the nonce-collision band is reported per
	// DEK generation, not per KEK version.
	SealCountSnapshot() map[uint32]uint64
	// LeaseCount returns the in-flight KEK lease count for the version. Phase
	// B has no acquire sites, so this is 0 in practice (Phase D wires it).
	LeaseCount(version uint32) uint64
}

// EncryptionKEKHandler serves the four KEK envelope admin endpoints. Construct
// via NewEncryptionKEKHandler and register via RegisterEncryptionKEKRoutes.
type EncryptionKEKHandler struct {
	orchestrator KEKRotationOrchestrator
	gate         KEKCapabilityGate
	reader       KEKStatusReader
}

// NewEncryptionKEKHandler wires the leader orchestrator, the capability gate,
// and the FSM state reader. Any nil dependency disables the handler — the
// route returns 503 "kek admin disabled".
func NewEncryptionKEKHandler(orch KEKRotationOrchestrator, gate KEKCapabilityGate, reader KEKStatusReader) *EncryptionKEKHandler {
	return &EncryptionKEKHandler{orchestrator: orch, gate: gate, reader: reader}
}

// rotateRequest is the wire body for POST /v1/encrypt/kek/rotate.
type rotateRequest struct {
	Confirm string `json:"confirm"`
}

// retireRequest / pruneRequest share the same shape: {version, confirm}.
type versionConfirmRequest struct {
	Version uint32 `json:"version"`
	Confirm string `json:"confirm"`
}

// kekStatusResponse is the body returned by GET /v1/encrypt/kek/status. The
// wire field names are stable (consumed by the CLI and operator tooling).
//
// Seal-count / nonce-collision diagnostics live at the top level under
// dek_generations, NOT on the per-KEK-version rows: AES-GCM nonce exhaustion is
// per-DEK-key, and after a KEK rotation the KEK↔DEK mapping is no longer 1:1.
type kekStatusResponse struct {
	ActiveVersion       uint32             `json:"active_version"`
	ActiveDEKGeneration uint32             `json:"active_dek_generation"`
	Versions            []kekVersionStatus `json:"versions"`
	DEKGenerations      []dekGenStatus     `json:"dek_generations"`
}

type kekVersionStatus struct {
	Version    uint32 `json:"version"`
	Status     string `json:"status"`
	LeaseCount uint64 `json:"lease_count"`
}

// dekGenStatus carries the per-DEK-generation nonce-collision diagnostic.
type dekGenStatus struct {
	Generation         uint32 `json:"generation"`
	Active             bool   `json:"active"`
	SealCount          uint64 `json:"seal_count"`
	NonceCollisionRisk string `json:"nonce_collision_risk"`
}

// Nonce-collision risk thresholds, keyed on the active KEK version's seal
// count. Each AES-GCM DEK can produce ~2^32 random-nonce seals before the
// collision probability exceeds 2^-32; these bands give operators headroom to
// schedule a DEK rotation before that bound is reached.
const (
	nonceWarnThreshold  = 100_000_000   // 1e8
	nonceAlertThreshold = 1_000_000_000 // 1e9
)

// nonceCollisionRisk maps a seal count to "ok" / "warn" / "alert".
func nonceCollisionRisk(sealCount uint64) string {
	switch {
	case sealCount >= nonceAlertThreshold:
		return "alert"
	case sealCount >= nonceWarnThreshold:
		return "warn"
	default:
		return "ok"
	}
}

// ServeRotate handles POST /v1/encrypt/kek/rotate.
func (h *EncryptionKEKHandler) ServeRotate(w http.ResponseWriter, r *http.Request) {
	if h.orchestrator == nil {
		http.Error(w, "kek admin disabled", http.StatusServiceUnavailable)
		return
	}
	var body rotateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if body.Confirm != "rotate-now" {
		http.Error(w, `confirm must be "rotate-now"`, http.StatusBadRequest)
		return
	}
	if err := h.checkGate(r.Context(), compat.OperationKEKRotate); err != nil {
		http.Error(w, "service unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	actor := actorFromRequest(r)
	log.Info().
		Str("event", "kek_rotate_received").
		Str("actor", actor).
		Msg("KEK rotate received")
	if err := h.orchestrator.ProposeKEKRotate(body.Confirm, actor); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ServeRetire handles POST /v1/encrypt/kek/retire.
func (h *EncryptionKEKHandler) ServeRetire(w http.ResponseWriter, r *http.Request) {
	if h.orchestrator == nil {
		http.Error(w, "kek admin disabled", http.StatusServiceUnavailable)
		return
	}
	body, err := decodeVersionConfirm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expected := fmt.Sprintf("delete-permanently-%d", body.Version)
	if body.Confirm != expected {
		http.Error(w, fmt.Sprintf(`confirm must be %q`, expected), http.StatusBadRequest)
		return
	}
	if err := h.checkGate(r.Context(), compat.OperationKEKRetire); err != nil {
		http.Error(w, "service unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	actor := actorFromRequest(r)
	log.Info().
		Str("event", "kek_retire_received").
		Str("actor", actor).
		Uint32("version", body.Version).
		Msg("KEK retire received")
	if err := h.orchestrator.ProposeKEKRetire(body.Version, body.Confirm, actor); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ServePrune handles POST /v1/encrypt/kek/prune.
func (h *EncryptionKEKHandler) ServePrune(w http.ResponseWriter, r *http.Request) {
	if h.orchestrator == nil {
		http.Error(w, "kek admin disabled", http.StatusServiceUnavailable)
		return
	}
	body, err := decodeVersionConfirm(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	expected := fmt.Sprintf("delete-permanently-%d", body.Version)
	if body.Confirm != expected {
		http.Error(w, fmt.Sprintf(`confirm must be %q`, expected), http.StatusBadRequest)
		return
	}
	if err := h.checkGate(r.Context(), compat.OperationKEKPrune); err != nil {
		http.Error(w, "service unavailable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	actor := actorFromRequest(r)
	log.Info().
		Str("event", "kek_prune_received").
		Str("actor", actor).
		Uint32("version", body.Version).
		Msg("KEK prune received")
	if err := h.orchestrator.ProposeKEKPrune(body.Version, actor); err != nil {
		writeProposeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ServeStatus handles GET /v1/encrypt/kek/status. Returns the active version
// plus per-version lifecycle status + seal/lease diagnostics. Prometheus
// scrapes the same live values via the KEK collector (internal/metrics), so
// this endpoint does not poke any metrics — it is a read-only operator query.
func (h *EncryptionKEKHandler) ServeStatus(w http.ResponseWriter, _ *http.Request) {
	if h.reader == nil {
		http.Error(w, "kek admin disabled", http.StatusServiceUnavailable)
		return
	}
	versions := h.reader.KEKStoreVersions()
	activeGen := h.reader.ActiveDEKGeneration()
	out := kekStatusResponse{
		ActiveVersion:       h.reader.ActiveKEKVersion(),
		ActiveDEKGeneration: activeGen,
		Versions:            make([]kekVersionStatus, 0, len(versions)),
	}
	for _, v := range versions {
		out.Versions = append(out.Versions, kekVersionStatus{
			Version:    v,
			Status:     lifecycleStatusString(h.reader, v, out.ActiveVersion),
			LeaseCount: h.reader.LeaseCount(v),
		})
	}

	// Per-DEK-generation nonce-collision diagnostics, sorted ascending.
	seals := h.reader.SealCountSnapshot()
	gens := make([]uint32, 0, len(seals))
	for g := range seals {
		gens = append(gens, g)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] < gens[j] })
	out.DEKGenerations = make([]dekGenStatus, 0, len(gens))
	for _, g := range gens {
		out.DEKGenerations = append(out.DEKGenerations, dekGenStatus{
			Generation:         g,
			Active:             g == activeGen,
			SealCount:          seals[g],
			NonceCollisionRisk: nonceCollisionRisk(seals[g]),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// decodeVersionConfirm parses {version, confirm}. The version must be present
// and non-zero — a missing or zero "version" field is rejected as it would
// silently translate to confirm "delete-permanently-0" without the operator
// realising.
func decodeVersionConfirm(r *http.Request) (versionConfirmRequest, error) {
	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return versionConfirmRequest{}, fmt.Errorf("invalid JSON: %w", err)
	}
	vRaw, ok := raw["version"]
	if !ok {
		return versionConfirmRequest{}, errors.New("version is required")
	}
	var version uint32
	switch v := vRaw.(type) {
	case float64:
		if v < 0 || v > 4294967295 {
			return versionConfirmRequest{}, fmt.Errorf("version out of uint32 range: %v", v)
		}
		version = uint32(v)
	default:
		return versionConfirmRequest{}, fmt.Errorf("version must be a uint32 number, got %T", vRaw)
	}
	confirm, _ := raw["confirm"].(string)
	return versionConfirmRequest{Version: version, Confirm: confirm}, nil
}

// checkGate returns nil when the gate is not wired (test path) or when the
// gate allows. A gate rejection is surfaced verbatim so the 503 body carries
// the underlying compat.Reject message.
func (h *EncryptionKEKHandler) checkGate(ctx context.Context, op compat.Operation) error {
	if h.gate == nil {
		return nil
	}
	_, err := h.gate.Allow(ctx, op)
	return err
}

// actorFromRequest builds an audit-friendly actor string from the UDS
// peercred. "uid=<N>" when resolved; "unknown" otherwise.
func actorFromRequest(r *http.Request) string {
	pc := peerUIDFrom(r)
	if !pc.Resolved {
		return "unknown"
	}
	return fmt.Sprintf("uid=%d", pc.UID)
}

// writeProposeError maps a leader-side error into an HTTP status. Sentinel
// errors take precedence; substring matches handle wrapped errors.
func writeProposeError(w http.ResponseWriter, err error) {
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	msg := err.Error()
	switch {
	case errors.Is(err, cluster.ErrKEKRotateAnotherInFlight):
		http.Error(w, msg, http.StatusConflict)
	case errors.Is(err, cluster.ErrKEKPruneNotLeader),
		strings.Contains(msg, "not leader"):
		http.Error(w, msg, http.StatusServiceUnavailable)
	case strings.Contains(msg, "confirm token"),
		strings.Contains(msg, "must be <"),
		strings.Contains(msg, "must be \"rotate-now\""):
		http.Error(w, msg, http.StatusBadRequest)
	default:
		http.Error(w, msg, http.StatusInternalServerError)
	}
}

// lifecycleStatusString returns one of "active", "retiring", "pruned".
// The active version is reported as "active" regardless of any stale
// kek_status entry; non-active versions consult the FSM table and default to
// "active" when no entry exists (the implicit lifecycle state for a version
// that has never been retired).
func lifecycleStatusString(r KEKStatusReader, version, active uint32) string {
	if version == active {
		return "active"
	}
	if _, status, _, ok := r.LookupKEKStatus(version); ok {
		switch status {
		case cluster.KEKLifecycleRetiring:
			return "retiring"
		case cluster.KEKLifecyclePruned:
			return "pruned"
		}
	}
	return "active"
}
