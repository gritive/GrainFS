package cluster

import (
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/badgermeta"
)

func TestApplyCreateBucket_RefusesReserved(t *testing.T) {
	for _, c := range []struct {
		name    string
		wantErr bool
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		{"default", true},
		{"analytics", false},
		{"my-bucket", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: c.name})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			err = fsm.Apply(data)
			if c.wantErr {
				if err == nil {
					t.Fatalf("Apply(CreateBucket %q): got nil, want error", c.name)
				}
				if !strings.Contains(err.Error(), "reserved") {
					t.Errorf("Apply(CreateBucket %q): error %q does not mention 'reserved'", c.name, err)
				}
			} else {
				if err != nil {
					t.Fatalf("Apply(CreateBucket %q): got %v, want nil", c.name, err)
				}
			}
		})
	}
}

func TestApplyCreateBucket_BypassReserved_AllowsReserved(t *testing.T) {
	for _, name := range []string{"_grainfs", "default", "_grainfs-audit"} {
		t.Run(name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: name, BypassReserved: true})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			if err := fsm.Apply(data); err != nil {
				t.Fatalf("bypass=true should allow reserved name %q, got %v", name, err)
			}
		})
	}
}

func TestApplyDeleteBucket_RefusesReserved(t *testing.T) {
	for _, c := range []struct {
		name    string
		wantErr bool
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		{"default", true},
		{"analytics", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			db := newTestDB(t)
			fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

			data, err := EncodeCommand(CmdDeleteBucket, DeleteBucketCmd{Bucket: c.name})
			if err != nil {
				t.Fatalf("EncodeCommand: %v", err)
			}
			err = fsm.Apply(data)
			if c.wantErr {
				if err == nil {
					t.Fatalf("Apply(DeleteBucket %q): got nil, want error", c.name)
				}
				if !strings.Contains(err.Error(), "reserved") {
					t.Errorf("Apply(DeleteBucket %q): error %q does not mention 'reserved'", c.name, err)
				}
			} else {
				// non-reserved bucket: delete may return ErrNotFound but not a "reserved" error
				if err != nil && strings.Contains(err.Error(), "reserved") {
					t.Fatalf("Apply(DeleteBucket %q): unexpected 'reserved' error: %v", c.name, err)
				}
			}
		})
	}
}
