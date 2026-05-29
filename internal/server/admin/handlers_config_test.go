package admin_test

import (
	"context"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestConfigSetEntry_RefusesReservedPDPTokenKey(t *testing.T) {
	ctx := context.Background()
	d := &admin.Deps{ConfigProposer: &routeConfigService{}}

	err := admin.ConfigSetEntry(ctx, d, "iam.pdp.token", `{"ct_b64":"YWJj","dek_gen":1}`)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("generic config set of iam.pdp.token must be refused, got %v", err)
	}

	// A normal key still flows through to the proposer.
	if err := admin.ConfigSetEntry(ctx, d, "iam.pdp", `{"enabled":false}`); err != nil {
		t.Fatalf("normal key must pass: %v", err)
	}
}

func TestConfigUnsetEntry_RefusesReservedPDPTokenKey(t *testing.T) {
	ctx := context.Background()
	d := &admin.Deps{ConfigProposer: &routeConfigService{}}

	err := admin.ConfigUnsetEntry(ctx, d, "iam.pdp.token")
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("generic config unset of iam.pdp.token must be refused, got %v", err)
	}

	// A normal key still flows through to the proposer.
	if err := admin.ConfigUnsetEntry(ctx, d, "iam.pdp"); err != nil {
		t.Fatalf("normal key must pass: %v", err)
	}
}
