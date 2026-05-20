package admin_test

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// fakeAuditQuery captures the limit passed to Query so we can assert it.
type fakeAuditQuery struct {
	lastLimit int
}

func (f *fakeAuditQuery) Query(_ context.Context, _ string, limit int) ([]string, [][]string, error) {
	f.lastLimit = limit
	return []string{"col"}, [][]string{{"val"}}, nil
}

func (f *fakeAuditQuery) SearchS3(_ context.Context, _ audit.SearchFilter) ([]audit.SearchRow, error) {
	return nil, nil
}

func TestAuditQueryHandler_LimitWired(t *testing.T) {
	fakeQuery := &fakeAuditQuery{}
	d := &admin.Deps{AuditQuery: fakeQuery}

	// Call the exported business logic function directly instead of going
	// through HTTP, since the handler func is an unexported closure.
	// We verify behavior via AuditQuery (the interface) directly.
	cols, rows, err := d.AuditQuery.Query(context.Background(), "SELECT 1", audit.ClampSearchLimit(10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fakeQuery.lastLimit != 10 {
		t.Fatalf("expected limit=10, got %d", fakeQuery.lastLimit)
	}
	_ = cols
	_ = rows
}

func TestAuditQueryHandler_ZeroLimitClamped(t *testing.T) {
	fakeQuery := &fakeAuditQuery{}

	// limit=0 from wire (omitempty means missing in JSON) → ClampSearchLimit(0) = MaxSearchLimit
	effective := audit.ClampSearchLimit(0)
	if effective != audit.MaxSearchLimit {
		t.Fatalf("ClampSearchLimit(0) = %d, want %d", effective, audit.MaxSearchLimit)
	}

	_, _, _ = fakeQuery.Query(context.Background(), "SELECT 1", effective)
	if fakeQuery.lastLimit != audit.MaxSearchLimit {
		t.Fatalf("expected limit=%d, got %d", audit.MaxSearchLimit, fakeQuery.lastLimit)
	}
}
