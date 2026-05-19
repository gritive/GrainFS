package iam

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// TestMetaProposer_DispatchesCorrectCmdTypes verifies that each method
// calls the underlying ProposeFunc with the matching MetaCmdType. Payload
// bytes are validated by Applier round-trip in fsm_test.go; here we only
// check the type dispatch is right.
func TestMetaProposer_DispatchesCorrectCmdTypes(t *testing.T) {
	captured := make([]clusterpb.MetaCmdType, 0)
	p := &MetaProposer{
		Propose: func(ctx context.Context, t clusterpb.MetaCmdType, payload []byte) error {
			captured = append(captured, t)
			return nil
		},
	}
	ctx := context.Background()
	_ = p.ProposeSACreate(ctx, ServiceAccount{ID: "sa-1"})
	_ = p.ProposeSADelete(ctx, "sa-1")
	_ = p.ProposeKeyCreate(ctx, AccessKey{AccessKey: "AK", SAID: "sa-1"})
	_ = p.ProposeKeyRevoke(ctx, "AK")

	want := []clusterpb.MetaCmdType{
		clusterpb.MetaCmdTypeIAMSACreate,
		clusterpb.MetaCmdTypeIAMSADelete,
		clusterpb.MetaCmdTypeIAMKeyCreate,
		clusterpb.MetaCmdTypeIAMKeyRevoke,
	}
	if len(captured) < len(want) {
		t.Fatalf("captured %d, want %d (got %v)", len(captured), len(want), captured)
	}
	for i, w := range want {
		if captured[i] != w {
			t.Errorf("idx %d: got %v, want %v", i, captured[i], w)
		}
	}
}

func TestBuildKeyCreatePayloadDoesNotContainPlaintextSecret(t *testing.T) {
	enc := newTestEncryptor(t)
	const secret = "plain-secret-that-must-never-be-persisted"
	wrapped, err := WrapSecret(enc, "sa-1", secret)
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}

	payload := buildKeyCreatePayload(AccessKey{
		AccessKey:    "AK",
		SAID:         "sa-1",
		SecretKey:    secret,
		SecretKeyEnc: wrapped,
		CreatedAt:    time.Unix(1, 0),
	})

	if bytes.Contains(payload, []byte(secret)) {
		t.Fatal("plaintext secret found in IAM key-create raft payload")
	}
}

func TestMetaProposer_ProposeBucketUpstreamPut(t *testing.T) {
	var seenType clusterpb.MetaCmdType
	var seenPayload []byte
	mp := &MetaProposer{
		Propose: func(_ context.Context, ct clusterpb.MetaCmdType, p []byte) error {
			seenType = ct
			seenPayload = append([]byte(nil), p...)
			return nil
		},
	}
	u := BucketUpstream{
		Bucket: "shared", Endpoint: "http://up:9000", AccessKey: "AK",
		SecretKeyEnc: []byte{1, 2, 3}, CreatedAt: time.Now().UTC(),
	}
	if err := mp.ProposeBucketUpstreamPut(context.Background(), u); err != nil {
		t.Fatalf("ProposeBucketUpstreamPut: %v", err)
	}
	if seenType != clusterpb.MetaCmdTypeIAMBucketUpstreamPut {
		t.Errorf("cmd type: got %v want IAMBucketUpstreamPut", seenType)
	}
	pb := iampb.GetRootAsBucketUpstreamPutPayload(seenPayload, 0)
	if string(pb.Bucket()) != "shared" || string(pb.AccessKey()) != "AK" {
		t.Errorf("payload decode: got bucket=%q ak=%q", pb.Bucket(), pb.AccessKey())
	}
}

func TestMetaProposer_ProposeBucketUpstreamDelete(t *testing.T) {
	var seenType clusterpb.MetaCmdType
	var seenPayload []byte
	mp := &MetaProposer{
		Propose: func(_ context.Context, ct clusterpb.MetaCmdType, p []byte) error {
			seenType = ct
			seenPayload = append([]byte(nil), p...)
			return nil
		},
	}
	if err := mp.ProposeBucketUpstreamDelete(context.Background(), "shared"); err != nil {
		t.Fatalf("ProposeBucketUpstreamDelete: %v", err)
	}
	if seenType != clusterpb.MetaCmdTypeIAMBucketUpstreamDelete {
		t.Errorf("cmd type: got %v want IAMBucketUpstreamDelete", seenType)
	}
	pb := iampb.GetRootAsBucketUpstreamDeletePayload(seenPayload, 0)
	if string(pb.Bucket()) != "shared" {
		t.Errorf("payload bucket: got %q want shared", pb.Bucket())
	}
}
