package cluster

// reserved_bucket_test.go — Task 12: CmdCreateBucket and CmdDeleteBucket are
// retired no-ops in the group-0 FSM (bucket control-plane moved to meta-raft).
// The reserved-name guard that used to live in the apply handlers is now
// enforced by MetaBucketStore (meta-raft CreateBucket/DeleteBucket commands).
//
// These tests verify that the RETIRED FSM apply slots are replay-safe no-ops
// for ALL bucket names — including reserved ones. The reserved-name enforcement
// is covered by the MetaBucketStore tests (testbackend_metabucket_test.go and
// bucket_write_test.go).

import (
	"testing"

	"github.com/gritive/GrainFS/internal/badgermeta"
)

// TestApplyCreateBucket_RefusesReserved — renamed: retired group-0 slot is now
// a replay-safe no-op for all names including reserved ones.
func TestApplyCreateBucket_RetiredSlot_IsNoOpForAllNames(t *testing.T) {
	for _, name := range []string{"_grainfs", "_grainfs-audit", "default", "analytics", "my-bucket"} {
		name := name
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: name})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			// All names must be no-ops — including reserved ones. The reserved guard
			// moved to MetaBucketStore; the group-0 FSM slot is unconditionally retired.
			if err := fsm.Apply(data); err != nil {
				t.Fatalf("retired Apply(CreateBucket %q): got %v, want nil (no-op)", name, err)
			}
		})
	}
}

// TestApplyCreateBucket_BypassReserved_AllowsReserved — updated: BypassReserved
// flag is still accepted by EncodeCommand (wire format stable); the retired FSM
// slot ignores it and is a no-op for all names.
func TestApplyCreateBucket_BypassReserved_RetiredNoOp(t *testing.T) {
	for _, name := range []string{"_grainfs", "default", "_grainfs-audit"} {
		name := name
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: name, BypassReserved: true})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			if err := fsm.Apply(data); err != nil {
				t.Fatalf("retired bypass=true Apply(CreateBucket %q): got %v, want nil", name, err)
			}
		})
	}
}

// TestApplyDeleteBucket_RefusesReserved — updated: retired group-0 slot is now
// a replay-safe no-op for all names including reserved ones.
func TestApplyDeleteBucket_RetiredSlot_IsNoOpForAllNames(t *testing.T) {
	for _, name := range []string{"_grainfs", "_grainfs-audit", "default", "analytics"} {
		name := name
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdDeleteBucket, DeleteBucketCmd{Bucket: name})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			// Retired slot must be a no-op for all names — reserved guard moved to MetaBucketStore.
			if err := fsm.Apply(data); err != nil {
				t.Fatalf("retired Apply(DeleteBucket %q): got %v, want nil (no-op)", name, err)
			}
		})
	}
}
