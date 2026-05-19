package cluster

import (
	"testing"
)

func TestPolicyAttachCodec_SAAttachRoundTrip(t *testing.T) {
	data, err := EncodePolicyAttachToSAPutPayload("sa-1", "readonly")
	if err != nil {
		t.Fatalf("EncodePolicyAttachToSAPutPayload: %v", err)
	}
	gotSA, gotPolicy, err := DecodePolicyAttachToSAPutPayload(data)
	if err != nil {
		t.Fatalf("DecodePolicyAttachToSAPutPayload: %v", err)
	}
	if gotSA != "sa-1" || gotPolicy != "readonly" {
		t.Fatalf("got sa_id=%q policy=%q, want sa-1/readonly", gotSA, gotPolicy)
	}
}

func TestPolicyAttachCodec_SADetachRoundTrip(t *testing.T) {
	data, err := EncodePolicyAttachToSADeletePayload("sa-2", "admin")
	if err != nil {
		t.Fatalf("EncodePolicyAttachToSADeletePayload: %v", err)
	}
	gotSA, gotPolicy, err := DecodePolicyAttachToSADeletePayload(data)
	if err != nil {
		t.Fatalf("DecodePolicyAttachToSADeletePayload: %v", err)
	}
	if gotSA != "sa-2" || gotPolicy != "admin" {
		t.Fatalf("got sa_id=%q policy=%q, want sa-2/admin", gotSA, gotPolicy)
	}
}

func TestPolicyAttachCodec_GroupAttachRoundTrip(t *testing.T) {
	data, err := EncodePolicyAttachToGroupPutPayload("engineers", "bucket-rw")
	if err != nil {
		t.Fatalf("EncodePolicyAttachToGroupPutPayload: %v", err)
	}
	gotGroup, gotPolicy, err := DecodePolicyAttachToGroupPutPayload(data)
	if err != nil {
		t.Fatalf("DecodePolicyAttachToGroupPutPayload: %v", err)
	}
	if gotGroup != "engineers" || gotPolicy != "bucket-rw" {
		t.Fatalf("got group=%q policy=%q, want engineers/bucket-rw", gotGroup, gotPolicy)
	}
}

func TestPolicyAttachCodec_GroupDetachRoundTrip(t *testing.T) {
	data, err := EncodePolicyAttachToGroupDeletePayload("ops", "bucket-ro")
	if err != nil {
		t.Fatalf("EncodePolicyAttachToGroupDeletePayload: %v", err)
	}
	gotGroup, gotPolicy, err := DecodePolicyAttachToGroupDeletePayload(data)
	if err != nil {
		t.Fatalf("DecodePolicyAttachToGroupDeletePayload: %v", err)
	}
	if gotGroup != "ops" || gotPolicy != "bucket-ro" {
		t.Fatalf("got group=%q policy=%q, want ops/bucket-ro", gotGroup, gotPolicy)
	}
}
