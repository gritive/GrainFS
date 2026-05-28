package serveruntime

import (
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestNormalizeInviteTTL(t *testing.T) {
	got, err := normalizeInviteTTL(0)
	if err != nil {
		t.Fatalf("default ttl: %v", err)
	}
	if got != defaultInviteTTL {
		t.Fatalf("default ttl = %s, want %s", got, defaultInviteTTL)
	}

	got, err = normalizeInviteTTL(30 * time.Minute)
	if err != nil {
		t.Fatalf("custom ttl: %v", err)
	}
	if got != 30*time.Minute {
		t.Fatalf("custom ttl = %s", got)
	}

	if _, err := normalizeInviteTTL(cluster.MaxInviteTTL + time.Nanosecond); err == nil {
		t.Fatal("ttl above max must be rejected")
	}
}
