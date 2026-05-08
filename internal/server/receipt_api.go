package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
)

// registerReceiptAPI wires the Phase 16 Slice 2 heal-receipt read endpoints:
//
//	GET /api/receipts/:id
//	GET /api/receipts?from=&to=&limit=
//
// Both endpoints require S3 HMAC authentication — no separate auth boundary.
// SRE / postmortem tooling presents the cluster access/secret key pair it
// already uses for S3 operations; unauthenticated calls get 403 from the
// existing s3AuthMiddleware (see ServeHTTP wiring).
//
// When WithReceiptAPI is not set, the handlers are not registered and the
// routes 404 — safer than a naked "not configured" error that leaks that
// the feature exists on this node.
func (s *Server) registerReceiptAPI(h *server.Hertz) {
	if s.receiptAPI == nil {
		return
	}

	// Build handler chains conditionally. When s.verifier is nil (e.g.,
	// test setups that skip the IAM wiring), the global auth middleware
	// was also skipped in New() — attaching authMiddleware here would
	// NPE on s.verifier.Verify. Matching the global pattern keeps
	// behavior consistent: nil verifier means no auth across every path.
	getByID := func(_ context.Context, c *app.RequestContext) {
		id := c.Param("id")
		s.receiptAPI.ServeGetReceipt(newResponseWriter(c), toHTTPRequest(c), id)
	}
	listRange := func(_ context.Context, c *app.RequestContext) {
		s.receiptAPI.ServeListReceipts(newResponseWriter(c), toHTTPRequest(c))
	}

	if s.verifier != nil {
		h.GET("/api/receipts/:id", s.authMiddleware(), getByID)
		h.GET("/api/receipts", s.authMiddleware(), listRange)
	} else {
		h.GET("/api/receipts/:id", getByID)
		h.GET("/api/receipts", listRange)
	}
}
