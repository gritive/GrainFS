package server

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) icebergCommitTable(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	var req struct {
		Requirements []json.RawMessage `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid table commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ident := icebergcatalog.Identifier{Namespace: []string{c.Param("namespace")}, Name: c.Param("table")}

	// Server-side optimistic-concurrency retry for unconditional commits.
	// When the client sent no requirements, ErrCommitFailed means the FSM
	// state moved between LoadTable and propose — re-load the latest state
	// and try again. Updates that produce per-call unique payloads (e.g.,
	// SetProperties with a unique key, AddSnapshot with a unique id) compose
	// correctly under retry. Non-empty requirements opt out: the client has
	// expressed an optimistic-lock expectation and a 409 is the
	// spec-compliant answer.
	maxAttempts := 1
	if len(req.Requirements) == 0 {
		maxAttempts = maxIcebergUnconditionalCommitRetries
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		tbl, err := store.LoadTable(ctx, ident)
		if err != nil {
			writeIcebergMappedError(c, err)
			return
		}
		committed, applied, commitErr := s.commitIcebergTableFrom(ctx, c, store, tbl, req.Requirements, req.Updates)
		if applied {
			writeIcebergTable(c, committed)
			return
		}
		if !errors.Is(commitErr, icebergcatalog.ErrCommitFailed) {
			// commitIcebergTableFrom already wrote the response for non-race errors.
			return
		}
		lastErr = commitErr
		// Race; loop to reload state and retry. The previous metadata.json we
		// wrote at the just-computed nextMetadataLocation is now orphaned —
		// best-effort cleanup is a follow-up; the file is small and the next
		// commit's writer will land at a different path.
	}
	// All retries exhausted: surface the original 409 the client would have
	// seen without retry. This preserves spec semantics for the rare case
	// that retries fail to converge under sustained contention.
	writeIcebergMappedError(c, lastErr)
}

func (s *Server) icebergCommitTransaction(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	var req struct {
		TableChanges []struct {
			Identifier   icebergcatalog.Identifier `json:"identifier"`
			Requirements []json.RawMessage         `json:"requirements"`
			Updates      []json.RawMessage         `json:"updates"`
		} `json:"table-changes"`
	}
	if err := decodeIcebergBody(c.Request.Body(), &req); err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid transaction commit request")
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	committed := make(map[string]*icebergcatalog.Table)
	for _, change := range req.TableChanges {
		key := icebergIdentifierKey(change.Identifier)
		tbl := committed[key]
		if tbl == nil {
			var err error
			tbl, err = store.LoadTable(ctx, change.Identifier)
			if err != nil {
				writeIcebergMappedError(c, err)
				return
			}
		}
		next, applied, commitErr := s.commitIcebergTableFrom(ctx, c, store, tbl, change.Requirements, change.Updates)
		if !applied {
			// For the transaction commit endpoint we deliberately do not
			// retry: a transaction's table-changes may have inter-table
			// invariants that the caller expressed implicitly, and a
			// blind retry across changes could produce a different
			// composite outcome. If 409s here become a real workload
			// concern, retry is a follow-up that needs careful design.
			if errors.Is(commitErr, icebergcatalog.ErrCommitFailed) {
				writeIcebergMappedError(c, commitErr)
			}
			return
		}
		committed[key] = next
	}
	c.JSON(consts.StatusOK, map[string]any{})
}
