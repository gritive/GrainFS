package cluster

import (
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/iam/bucketpolicy"
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
			fsm := NewFSM(db, newStateKeyspaceEmpty())

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
			fsm := NewFSM(db, newStateKeyspaceEmpty())

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
			fsm := NewFSM(db, newStateKeyspaceEmpty())

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

func TestApplyBucketPolicyPut_RefusesInternalBucketOnly(t *testing.T) {
	for _, c := range []struct {
		name    string
		wantErr bool
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		// "default" bucket SHOULD allow policy operations (operators override implicit-anon)
		{"default", false},
		{"analytics", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := bucketpolicy.NewInMemoryStore()
			f := NewMetaFSM()
			f.SetBucketPolicyStore(store)

			raw := []byte(`{"Statement":[]}`)
			err := f.applyCmd(buildBucketPolicyPutCmd(t, c.name, raw))
			if c.wantErr {
				if err == nil {
					t.Fatalf("BucketPolicyPut %q: got nil, want error", c.name)
				}
				if !strings.Contains(err.Error(), "internal") {
					t.Errorf("BucketPolicyPut %q: error %q does not mention 'internal'", c.name, err)
				}
			} else {
				if err != nil {
					t.Fatalf("BucketPolicyPut %q: got %v, want nil", c.name, err)
				}
			}
		})
	}
}

func TestApplyBucketPolicyDelete_RefusesInternalBucketOnly(t *testing.T) {
	for _, c := range []struct {
		name        string
		refuseGuard bool // guard fires with "internal" message
	}{
		{"_grainfs", true},
		{"_grainfs-audit", true},
		// "default" and regular buckets: guard does NOT fire (operator may delete policy)
		{"default", false},
		{"analytics", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := bucketpolicy.NewInMemoryStore()
			f := NewMetaFSM()
			f.SetBucketPolicyStore(store)

			err := f.applyCmd(buildBucketPolicyDeleteCmd(t, c.name))
			if c.refuseGuard {
				if err == nil {
					t.Fatalf("BucketPolicyDelete %q: got nil, want guard error", c.name)
				}
				if !strings.Contains(err.Error(), "internal") {
					t.Errorf("BucketPolicyDelete %q: error %q does not mention 'internal'", c.name, err)
				}
			} else {
				// Guard must not fire: if there's an error it should be a store error, not a guard error.
				if err != nil && strings.Contains(err.Error(), "internal") {
					t.Fatalf("BucketPolicyDelete %q: guard fired unexpectedly: %v", c.name, err)
				}
			}
		})
	}
}
