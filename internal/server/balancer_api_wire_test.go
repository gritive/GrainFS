package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/adminapi"
)

func TestBalancerWire_ZeroTime_OmitsField(t *testing.T) {
	resp := adminapi.BalancerStatus{
		Available: true,
		Nodes:     []adminapi.BalancerNodeStatus{{NodeID: "n1"}}, // JoinedAt/UpdatedAt empty
	}
	buf, _ := json.Marshal(resp)
	if strings.Contains(string(buf), "joined_at") {
		t.Fatalf("expected joined_at omitted on zero time, got: %s", buf)
	}
	if strings.Contains(string(buf), "updated_at") {
		t.Fatalf("expected updated_at omitted on zero time, got: %s", buf)
	}
}
