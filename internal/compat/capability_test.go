package compat

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRegistryRejectsDuplicateCapabilityNames(t *testing.T) {
	reg, err := NewRegistry([]Capability{
		{Name: "migration_cutover_v1", Scope: ScopeMetaRaft, Severity: SeverityHard, IntroducedVersion: "0.0.193.0", Description: "first"},
		{Name: "migration_cutover_v1", Scope: ScopeMetaRaft, Severity: SeverityHard, IntroducedVersion: "0.0.193.0", Description: "second"},
	})
	if err == nil {
		t.Fatal("NewRegistry duplicate capability: want error, got nil")
	}
	if reg != nil {
		t.Fatalf("NewRegistry duplicate capability: want nil registry, got %#v", reg)
	}
}

func TestRegistryRejectsInvalidScopeAndSeverity(t *testing.T) {
	_, err := NewRegistry([]Capability{{Name: "bad_scope", Scope: Scope("bad"), Severity: SeverityHard}})
	if err == nil || !strings.Contains(err.Error(), "invalid scope") {
		t.Fatalf("bad scope error = %v, want invalid scope", err)
	}
	_, err = NewRegistry([]Capability{{Name: "bad_severity", Scope: ScopeMetaRaft, Severity: Severity("bad")}})
	if err == nil || !strings.Contains(err.Error(), "invalid severity") {
		t.Fatalf("bad severity error = %v, want invalid severity", err)
	}
}

func TestGateRejectErrorClassifiesMissingUnknownAndStale(t *testing.T) {
	err := &GateRejectError{
		Plan: GatePlan{
			Capability: CapabilityMigrationCutoverV1,
			Scope:      ScopeMetaRaft,
			Severity:   SeverityHard,
			Operation:  OperationMigrationCutover,
			ConfigID:   42,
			Missing:    []NodeID{"node-2"},
			Unknown:    []NodeID{"node-3"},
			Stale:      []StaleNode{{NodeID: "node-4", LastSeen: time.Unix(10, 0)}},
		},
	}
	if !errors.Is(err, ErrCapabilityRejected) {
		t.Fatalf("GateRejectError must wrap ErrCapabilityRejected")
	}
	got := err.Error()
	for _, want := range []string{"migration_cutover_v1", "missing=[node-2]", "unknown=[node-3]", "stale=[node-4"} {
		if !strings.Contains(got, want) {
			t.Fatalf("GateRejectError.Error() = %q, missing %q", got, want)
		}
	}
}

func TestPublicMessageHidesNodeIDs(t *testing.T) {
	err := &GateRejectError{
		Plan: GatePlan{
			Capability: CapabilityMigrationCutoverV1,
			Scope:      ScopeMetaRaft,
			Severity:   SeverityHard,
			Operation:  OperationMigrationCutover,
			Missing:    []NodeID{"secret-node-id"},
		},
	}
	got := err.PublicMessage()
	if strings.Contains(got, "secret-node-id") {
		t.Fatalf("PublicMessage leaked node ID: %q", got)
	}
	if !strings.Contains(got, "migration_cutover") {
		t.Fatalf("PublicMessage = %q, want bounded operation name", got)
	}
}

func TestRecordRejectUsesBoundedLabels(t *testing.T) {
	plan := GatePlan{
		Capability: CapabilityMigrationCutoverV1,
		Scope:      ScopeMetaRaft,
		Severity:   SeverityHard,
		Operation:  OperationMigrationCutover,
		Missing:    []NodeID{"node-with-secret-address"},
	}
	labels := RejectLabels(plan, false)
	if labels["operation"] != string(OperationMigrationCutover) {
		t.Fatalf("operation label = %q", labels["operation"])
	}
	for key, value := range labels {
		if strings.Contains(value, "node-with-secret-address") {
			t.Fatalf("label %s leaked node ID: %q", key, value)
		}
	}
	if _, ok := labels["path"]; ok {
		t.Fatalf("RejectLabels must not include raw path label")
	}
}

func TestDefaultRegistryIncludesMultipartListingCapability(t *testing.T) {
	capDef, ok := DefaultRegistry.Lookup(CapabilityMultipartListingV1)
	if !ok {
		t.Fatalf("DefaultRegistry missing %s", CapabilityMultipartListingV1)
	}
	if capDef.Scope != ScopePeerTransport {
		t.Fatalf("multipart listing scope = %s, want %s", capDef.Scope, ScopePeerTransport)
	}
	if capDef.IntroducedVersion != "0.0.213.0" {
		t.Fatalf("multipart listing introduced version = %s", capDef.IntroducedVersion)
	}
	if OperationListParts != Operation("list_parts") {
		t.Fatalf("OperationListParts = %s", OperationListParts)
	}
	if OperationCreateMultipartUpload != Operation("create_multipart_upload") {
		t.Fatalf("OperationCreateMultipartUpload = %s", OperationCreateMultipartUpload)
	}
	if OperationListMultipartUploads != Operation("list_multipart_uploads") {
		t.Fatalf("OperationListMultipartUploads = %s", OperationListMultipartUploads)
	}
}

func TestRegistryKEKEnvelopeV1Registered(t *testing.T) {
	cap, ok := DefaultRegistry.Lookup(CapabilityKEKEnvelopeV1)
	if !ok {
		t.Fatalf("kek_envelope_v1 not registered")
	}
	if cap.Scope != ScopeMetaRaft {
		t.Errorf("scope = %q, want meta_raft", cap.Scope)
	}
	if cap.Severity != SeverityHard {
		t.Errorf("severity = %q, want hard", cap.Severity)
	}
	if cap.IntroducedVersion == "" {
		t.Errorf("IntroducedVersion empty")
	}
}

func TestKEKOperationsGated(t *testing.T) {
	for _, op := range []Operation{
		OperationKEKRotate,
		OperationKEKRetire,
		OperationKEKPrune,
		OperationKEKLeaseSnapshot,
		OperationKEKStatusQuery,
	} {
		caps, ok := DefaultRegistry.RequiredCapabilitiesForOperation(op)
		if !ok {
			t.Errorf("operation %q not gated", op)
			continue
		}
		found := false
		for _, c := range caps {
			if c == CapabilityKEKEnvelopeV1 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("operation %q missing kek_envelope_v1 gate", op)
		}
	}
}

func TestKEKOperationStringValues(t *testing.T) {
	pinned := map[Operation]string{
		OperationKEKRotate:        "kek_rotate",
		OperationKEKRetire:        "kek_retire",
		OperationKEKPrune:         "kek_prune",
		OperationKEKLeaseSnapshot: "kek_lease_snapshot",
		OperationKEKStatusQuery:   "kek_status_query",
	}
	for op, want := range pinned {
		if string(op) != want {
			t.Errorf("operation %q != %q", string(op), want)
		}
	}
}
