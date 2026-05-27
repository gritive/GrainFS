package cluster

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// TestCodecRoundTrip_InviteMint encodes and decodes an InviteMint command and
// asserts all fields survive the round-trip.
func TestCodecRoundTrip_InviteMint(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	id := "inv-rt-1"
	expiry := time.Now().Add(10 * time.Minute).UnixNano()

	data, err := encodeInviteMintCmd(id, pub, expiry)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotID, gotPub, gotExpiry, err := decodeInviteMintCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotID != id {
		t.Errorf("id: got %q, want %q", gotID, id)
	}
	if string(gotPub) != string(pub) {
		t.Error("pub mismatch")
	}
	if gotExpiry != expiry {
		t.Errorf("expiry: got %d, want %d", gotExpiry, expiry)
	}
}

// TestCodecRoundTrip_InviteConsume encodes and decodes an InviteConsume command.
func TestCodecRoundTrip_InviteConsume(t *testing.T) {
	id := "inv-rt-2"
	consumedAt := time.Now().UnixNano()

	data, err := encodeInviteConsumeCmd(id, consumedAt)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotID, gotAt, err := decodeInviteConsumeCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotID != id {
		t.Errorf("id: got %q, want %q", gotID, id)
	}
	if gotAt != consumedAt {
		t.Errorf("consumedAt: got %d, want %d", gotAt, consumedAt)
	}
}

// TestCodecRoundTrip_RegisterPendingLearner encodes and decodes a
// RegisterPendingLearner command.
func TestCodecRoundTrip_RegisterPendingLearner(t *testing.T) {
	nodeID := "node-rt"
	s := spki(7)
	addr := "10.0.1.2:7001"

	data, err := encodeRegisterPendingLearnerCmd(nodeID, s, addr)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotNode, gotSPKI, gotAddr, err := decodeRegisterPendingLearnerCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID: got %q, want %q", gotNode, nodeID)
	}
	if gotSPKI != s {
		t.Error("spki mismatch")
	}
	if gotAddr != addr {
		t.Errorf("addr: got %q, want %q", gotAddr, addr)
	}
}

// TestCodecRoundTrip_PromoteMember encodes and decodes a PromoteMember command.
func TestCodecRoundTrip_PromoteMember(t *testing.T) {
	nodeID := "node-promote-rt"

	data, err := encodePromoteMemberCmd(nodeID)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotNode, err := decodePromoteMemberCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID: got %q, want %q", gotNode, nodeID)
	}
}

// TestCodecRoundTrip_RevokePeer encodes and decodes a RevokePeer command.
func TestCodecRoundTrip_RevokePeer(t *testing.T) {
	nodeID := "node-revoke-rt"

	data, err := encodeRevokePeerCmd(nodeID)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotNode, err := decodeRevokePeerCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID: got %q, want %q", gotNode, nodeID)
	}
}

// TestCodecRoundTrip_InvitePending encodes and decodes an InvitePending command.
func TestCodecRoundTrip_InvitePending(t *testing.T) {
	inviteID := "inv-pending-rt"
	nodeID := "node-pending-rt"
	s := spki(9)
	addr := "10.0.2.3:7002"

	data, err := encodeInvitePendingCmd(inviteID, nodeID, s, addr)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	gotInvite, gotNode, gotSPKI, gotAddr, err := decodeInvitePendingCmd(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotInvite != inviteID {
		t.Errorf("inviteID: got %q, want %q", gotInvite, inviteID)
	}
	if gotNode != nodeID {
		t.Errorf("nodeID: got %q, want %q", gotNode, nodeID)
	}
	if gotSPKI != s {
		t.Error("spki mismatch")
	}
	if gotAddr != addr {
		t.Errorf("addr: got %q, want %q", gotAddr, addr)
	}
}

// TestCodecDecode_InvitePendingWrongSPKILength asserts that decodeInvitePendingCmd
// rejects a payload whose spki field is not exactly 32 bytes.
func TestCodecDecode_InvitePendingWrongSPKILength(t *testing.T) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString("inv-bad-spki")
	nodeOff := b.CreateString("node-bad-spki")
	shortSPKI := make([]byte, 17) // wrong length — triggers the len!=32 guard
	spkiOff := b.CreateByteVector(shortSPKI)
	addrOff := b.CreateString("addr")
	clusterpb.MetaInvitePendingCmdStart(b)
	clusterpb.MetaInvitePendingCmdAddInviteId(b, idOff)
	clusterpb.MetaInvitePendingCmdAddNodeId(b, nodeOff)
	clusterpb.MetaInvitePendingCmdAddSpki(b, spkiOff)
	clusterpb.MetaInvitePendingCmdAddAddress(b, addrOff)
	data := fbFinish(b, clusterpb.MetaInvitePendingCmdEnd(b))

	if _, _, _, _, err := decodeInvitePendingCmd(data); err == nil {
		t.Fatal("decodeInvitePendingCmd with 17-byte SPKI must error")
	}
}

// TestCodecDecode_InviteMintWrongPubLength asserts that decodeInviteMintCmd
// rejects a payload whose pub field is not exactly ed25519.PublicKeySize bytes.
func TestCodecDecode_InviteMintWrongPubLength(t *testing.T) {
	// Encode with a short pub (17 bytes instead of 32).
	shortPub := ed25519.PublicKey(make([]byte, 17))
	// encodeInviteMintCmd accepts ed25519.PublicKey ([]byte), so we can pass a
	// short key to build a payload with the wrong pub length.
	data, err := encodeInviteMintCmd("bad-pub", shortPub, 0)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, _, _, err := decodeInviteMintCmd(data); err == nil {
		t.Fatal("decodeInviteMintCmd with wrong-length pub must error")
	}
}

// TestCodecDecode_RegisterPendingLearnerWrongSPKILength asserts that
// decodeRegisterPendingLearnerCmd rejects a payload whose spki field is not
// exactly 32 bytes.
//
// Because encodeRegisterPendingLearnerCmd takes [32]byte (compile-time length),
// we build the payload using the raw FlatBuffers builder with a short SPKI.
func TestCodecDecode_RegisterPendingLearnerWrongSPKILength(t *testing.T) {
	b := clusterBuilderPool.Get()
	idOff := b.CreateString("node-bad-spki")
	shortSPKI := make([]byte, 17) // wrong length — triggers the len!=32 guard
	spkiOff := b.CreateByteVector(shortSPKI)
	addrOff := b.CreateString("addr")
	clusterpb.MetaRegisterPendingLearnerCmdStart(b)
	clusterpb.MetaRegisterPendingLearnerCmdAddNodeId(b, idOff)
	clusterpb.MetaRegisterPendingLearnerCmdAddSpki(b, spkiOff)
	clusterpb.MetaRegisterPendingLearnerCmdAddAddress(b, addrOff)
	data := fbFinish(b, clusterpb.MetaRegisterPendingLearnerCmdEnd(b))

	if _, _, _, err := decodeRegisterPendingLearnerCmd(data); err == nil {
		t.Fatal("decodeRegisterPendingLearnerCmd with 17-byte SPKI must error")
	}
}
