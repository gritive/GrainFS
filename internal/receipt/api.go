package receipt

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	// defaultListLimit caps list responses when the client omits ?limit=.
	// Tuned for dashboard pagination without one-shot huge payloads.
	defaultListLimit = 100
	// maxListLimit is the ceiling regardless of what ?limit= asks for.
	maxListLimit = 1000
)

// RouteLookup resolves a receipt ID to the peer address (if known from
// gossip). Satisfied by *RoutingCache.
type RouteLookup interface {
	Lookup(receiptID string) (nodeID string, ok bool)
}

// PeerQuerier handles cross-node receipt fetches. QuerySingle targets one
// peer (used on RoutingCache hit); Query fans out to all peers (fallback
// when no route is known or the targeted peer fails). Satisfied by
// cluster.ReceiptBroadcaster.
type PeerQuerier interface {
	QuerySingle(ctx context.Context, peer, receiptID string) ([]byte, bool, error)
	Query(ctx context.Context, receiptID string) ([]byte, bool, error)
}

// API wires the read-side HTTP endpoints for Slice 2:
//   - GET /api/receipts/:id          → single-receipt lookup
//   - GET /api/receipts?from=&to=    → range list
//
// The framework-level routing (Hertz, middleware, HMAC auth) lives in
// internal/server — this package exposes stdlib http.ResponseWriter
// handlers so it stays framework-agnostic and easy to unit-test.
type API struct {
	store    *Store
	routes   RouteLookup
	querier  PeerQuerier
	maxRange time.Duration // 0 = unlimited; otherwise caps (to-from) on List
	logger   zerolog.Logger
}

// NewAPI builds an API. routes may be nil on a single-node deployment.
// querier may be nil when this node has no peers configured (local-only
// lookups only). maxRange caps how wide a List query can be — pass the
// configured retention so authenticated callers can't force unbounded
// ts:* index scans. Zero disables the cap (tests / trusted callers).
func NewAPI(store *Store, routes RouteLookup, querier PeerQuerier, maxRange time.Duration) *API {
	return &API{
		store:    store,
		routes:   routes,
		querier:  querier,
		maxRange: maxRange,
		logger:   log.With().Str("component", "receipt-api").Logger(),
	}
}

// ServeGetReceipt handles a single-receipt lookup.
//
// Resolution order:
//  1. Local store (instant path, most hits).
//  2. RoutingCache — if a peer recently gossiped this id, query that
//     peer directly. On peer failure, fall through to step 3 rather
//     than surfacing a partial outage.
//  3. Broadcast fan-out — last-resort path for receipts outside the
//     rolling window. Subject to the broadcaster's 3s timeout.
//
// On broadcast timeout returns 503 + X-Heal-Timeout so SREs can
// distinguish "never existed" (404) from "cluster was unreachable".
func (a *API) ServeGetReceipt(w http.ResponseWriter, r *http.Request, id string) {
	if id == "" {
		http.Error(w, `{"error":"missing receipt id"}`, http.StatusBadRequest)
		return
	}

	// 1. Local.
	if raw, ok := a.store.LookupReceiptJSON(id); ok {
		writeJSON(w, http.StatusOK, raw)
		return
	}

	// 2. Routing cache + single-peer query.
	if a.routes != nil && a.querier != nil {
		if peer, ok := a.routes.Lookup(id); ok {
			raw, found, err := a.querier.QuerySingle(r.Context(), peer, id)
			if err == nil && found {
				writeJSON(w, http.StatusOK, raw)
				return
			}
			// Fall through: cached peer didn't have it (rolling window
			// drift) or the call failed. Broadcast will pick up the slack.
			if err != nil {
				a.logger.Warn().Str("id", id).Str("peer", peer).Err(err).Msg("receipt-api: routed peer query failed, falling back to broadcast")
			}
		}
	}

	// 3. Broadcast fan-out.
	if a.querier == nil {
		http.Error(w, `{"error":"receipt not found"}`, http.StatusNotFound)
		return
	}
	raw, found, err := a.querier.Query(r.Context(), id)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			w.Header().Set("X-Heal-Timeout", "broadcast")
			http.Error(w, `{"error":"cluster broadcast timed out"}`, http.StatusServiceUnavailable)
			return
		}
		a.logger.Warn().Str("id", id).Err(err).Msg("receipt-api: broadcast error")
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, `{"error":"receipt not found"}`, http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, raw)
}

// ServeGetByCorrelationID handles GET /api/receipts?correlation_id=X.
// Returns the single receipt for a repair session identified by its correlation
// ID, or 404 if no receipt was finalized for that session.
func (a *API) ServeGetByCorrelationID(w http.ResponseWriter, correlationID string) {
	r, err := a.store.GetByCorrelationID(correlationID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			http.Error(w, `{"error":"receipt not found"}`, http.StatusNotFound)
			return
		}
		a.logger.Warn().Str("correlation_id", correlationID).Err(err).Msg("receipt-api: get by correlation_id failed")
		http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	body, err := json.Marshal(r)
	if err != nil {
		http.Error(w, `{"error":"encode failed"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, body)
}

// ServeListReceipts handles GET /api/receipts?from=&to=&limit= (time-range list)
// or GET /api/receipts?correlation_id=X (session lookup).
// Times use RFC 3339 with optional fractional seconds.
func (a *API) ServeListReceipts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Correlation-ID shortcut: bypass time-range logic.
	if cid := q.Get("correlation_id"); cid != "" {
		a.ServeGetByCorrelationID(w, cid)
		return
	}

	fromStr, toStr := q.Get("from"), q.Get("to")
	if fromStr == "" || toStr == "" {
		http.Error(w, `{"error":"from and to are required (RFC 3339)"}`, http.StatusBadRequest)
		return
	}
	from, err := time.Parse(time.RFC3339Nano, fromStr)
	if err != nil {
		http.Error(w, `{"error":"invalid from timestamp"}`, http.StatusBadRequest)
		return
	}
	to, err := time.Parse(time.RFC3339Nano, toStr)
	if err != nil {
		http.Error(w, `{"error":"invalid to timestamp"}`, http.StatusBadRequest)
		return
	}
	// Cap the query window to retention so an authenticated caller cannot
	// sweep an unbounded ts:* range. Data outside retention is already
	// TTL-expired, so the cap never hides real results.
	if a.maxRange > 0 && to.Sub(from) > a.maxRange {
		http.Error(w, `{"error":"time range exceeds retention window"}`, http.StatusBadRequest)
		return
	}

	limit := defaultListLimit
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			http.Error(w, `{"error":"invalid limit"}`, http.StatusBadRequest)
			return
		}
		if n > maxListLimit {
			n = maxListLimit
		}
		limit = n
	}

	receipts, err := a.store.List(from, to, limit)
	if err != nil {
		a.logger.Warn().Err(err).Msg("receipt-api: list failed")
		http.Error(w, `{"error":"list failed"}`, http.StatusInternalServerError)
		return
	}
	// Always return an array, never null, so clients can iterate safely.
	if receipts == nil {
		receipts = []*HealReceipt{}
	}
	body, err := json.Marshal(receipts)
	if err != nil {
		http.Error(w, `{"error":"encode failed"}`, http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
