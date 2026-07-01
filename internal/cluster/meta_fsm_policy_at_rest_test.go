package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSMSnapshotSealsBucketPolicyAtRest proves the live meta-FSM bucket policy
// is sealed inside the snapshot envelope at rest: (a) GSNE-enveloped, (b) the policy
// plaintext needle does NOT leak into the sealed bytes, (c) the policy round-trips
// through Restore (anti-vacuous — proves the policy really was in the snapshotted state).
func TestMetaFSMSnapshotSealsBucketPolicyAtRest(t *testing.T) {
	const needle = "metafsm-policy-at-rest-needle-7a3f"
	const bucket = "policy-at-rest-bucket"
	policy := []byte(`{"Version":"2012-10-17","Statement":[{"Sid":"` + needle +
		`","Effect":"Allow","Principal":"*","Action":"s3:GetObject",` +
		`"Resource":"arn:aws:s3:::` + bucket + `/*"}]}`)
	src := NewMetaFSM()
	wireTestKEK(t, src)
	if err := src.applyCmd(makeAddNodeCmd(t, "node-1", "addr-1:7001", 0)); err != nil {
		t.Fatalf("add node: %v", err)
	}
	if err := src.applyCmd(makePutShardGroupCmd(t, "group-0", []string{"node-1"})); err != nil {
		t.Fatalf("put shard group: %v", err)
	}
	if err := src.applyCmd(makePutBucketAssignmentCmd(t, bucket, "group-0")); err != nil {
		t.Fatalf("put bucket assignment: %v", err)
	}
	if err := src.applyCmd(makeSetBucketPolicyCmd(t, bucket, policy)); err != nil {
		t.Fatalf("set bucket policy: %v", err)
	}
	snapBytes, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !bytes.HasPrefix(snapBytes, []byte("GSNE")) {
		t.Fatalf("snapshot is not GSNE-enveloped; prefix=%q", snapBytes[:4])
	}
	if bytes.Contains(snapBytes, []byte(needle)) {
		t.Fatalf("plaintext policy needle leaked into sealed snapshot")
	}
	dst := NewMetaFSM()
	wireTestKEK(t, dst)
	if err := dst.Restore(raft.SnapshotMeta{}, snapBytes); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	rec, ok := dst.BucketRecord(bucket)
	if !ok {
		t.Fatalf("restored FSM missing bucket record for %q", bucket)
	}
	if !bytes.Equal(rec.Policy, policy) {
		t.Fatalf("restored policy = %q, want %q", rec.Policy, policy)
	}
}
