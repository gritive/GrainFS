package serveruntime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAuditSearchEndpointUsesLoopback(t *testing.T) {
	tests := map[string]string{
		"127.0.0.1:9000": "http://127.0.0.1:9000",
		"0.0.0.0:9000":   "http://127.0.0.1:9000",
		":9000":          "http://127.0.0.1:9000",
		"[::]:9000":      "http://127.0.0.1:9000",
	}
	for addr, want := range tests {
		if got := auditSearchEndpoint(addr); got != want {
			t.Fatalf("auditSearchEndpoint(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestAuditSearchEndpointRejectsEphemeralPort(t *testing.T) {
	if got := auditSearchEndpoint("127.0.0.1:0"); got != "" {
		t.Fatalf("auditSearchEndpoint ephemeral port = %q, want empty", got)
	}
}

func TestWarmupAuditSearchRetriesUntilReady(t *testing.T) {
	attempts := 0
	err := warmupAuditSearch(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("not listening yet")
		}
		return nil
	}, time.Second, time.Millisecond, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("warmupAuditSearch error = %v, want nil", err)
	}
	if attempts != 3 {
		t.Fatalf("warmupAuditSearch attempts = %d, want 3", attempts)
	}
}
