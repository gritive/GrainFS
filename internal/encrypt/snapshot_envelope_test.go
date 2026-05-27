package encrypt

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSnapshotEnvelopeHeaderRoundTrip(t *testing.T) {
	var sid [16]byte
	copy(sid[:], []byte("0123456789abcdef"))
	var cid [16]byte
	copy(cid[:], []byte("cluster-id-16byt"))
	hdr := snapshotEnvelopeHeader{
		formatVersion:    1,
		activeKEKVersion: 7,
		snapshotID:       sid,
		clusterID:        cid,
	}
	wrappedDEK := bytes.Repeat([]byte{0xAB}, 60)
	buf := hdr.encode(wrappedDEK)

	gotHdr, gotWrapped, rest, err := decodeSnapshotEnvelopeHeader(buf)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotHdr != hdr {
		t.Fatalf("header mismatch: got %+v want %+v", gotHdr, hdr)
	}
	if !bytes.Equal(gotWrapped, wrappedDEK) {
		t.Fatalf("wrapped DEK mismatch")
	}
	if len(rest) != 0 {
		t.Fatalf("expected no trailing body bytes, got %d", len(rest))
	}
}

func TestDecodeSnapshotEnvelopeHeaderRejectsBadMagic(t *testing.T) {
	bad := bytes.Repeat([]byte{0x00}, 64)
	if _, _, _, err := decodeSnapshotEnvelopeHeader(bad); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDecodeSnapshotEnvelopeHeaderRejectsShortBuffer(t *testing.T) {
	if _, _, _, err := decodeSnapshotEnvelopeHeader([]byte{0x47, 0x53}); err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func newTestKEKCID(t *testing.T) (kek []byte, cid [16]byte) {
	t.Helper()
	kek = make([]byte, KEKSize)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	copy(cid[:], []byte("cluster-id-16byt"))
	return kek, cid
}

func TestSnapshotEnvelopeSealOpenRoundTrip(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	body := []byte("the plaintext meta-fsm snapshot blob with secret object keys")
	var sid [16]byte
	copy(sid[:], []byte("snapshot-id-16by"))

	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 7, body)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(sealed, []byte("secret object keys")) {
		t.Fatal("plaintext leaked into sealed envelope")
	}
	hdr, plain, err := OpenSnapshotEnvelope(kek, cid[:], sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(plain, body) {
		t.Fatal("round-trip body mismatch")
	}
	if hdr.ActiveKEKVersion() != 7 || hdr.SnapshotID() != sid {
		t.Fatalf("header fields mismatch: %+v", hdr)
	}
}

func TestSnapshotEnvelopeEmptyBodyRoundTrip(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	var sid [16]byte
	copy(sid[:], []byte("snapshot-id-16by"))

	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, []byte{})
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	_, plain, err := OpenSnapshotEnvelope(kek, cid[:], sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(plain) != 0 {
		t.Fatalf("expected empty body, got %d bytes", len(plain))
	}
}

func TestSnapshotEnvelopeOpenRejectsWrongKEK(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	other := make([]byte, KEKSize)
	_, _ = rand.Read(other)
	var sid [16]byte
	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := OpenSnapshotEnvelope(other, cid[:], sealed); err == nil {
		t.Fatal("expected open failure under wrong KEK")
	}
}

func TestSnapshotEnvelopeOpenRejectsWrongCluster(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	var sid [16]byte
	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	var otherCID [16]byte
	copy(otherCID[:], []byte("OTHER-cluster-id"))
	if _, _, err := OpenSnapshotEnvelope(kek, otherCID[:], sealed); err == nil {
		t.Fatal("expected open failure under wrong cluster id (AAD bind)")
	}
}

func TestSnapshotEnvelopeOpenRejectsTamperedHeader(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	var sid [16]byte
	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	sealed[6] ^= 0xFF
	if _, _, err := OpenSnapshotEnvelope(kek, cid[:], sealed); err == nil {
		t.Fatal("expected open failure under header tamper (AAD bind)")
	}
}

func TestSnapshotEnvelopeOpenRejectsTamperedBody(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	var sid [16]byte
	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, bytes.Repeat([]byte("x"), 64))
	if err != nil {
		t.Fatal(err)
	}
	sealed[len(sealed)-1] ^= 0xFF
	if _, _, err := OpenSnapshotEnvelope(kek, cid[:], sealed); err == nil {
		t.Fatal("expected open failure under body tamper (AEAD verify)")
	}
}

func TestSnapshotEnvelopeOpenRejectsUnknownFormatVersion(t *testing.T) {
	kek, cid := newTestKEKCID(t)
	var sid [16]byte
	sealed, err := SealSnapshotEnvelope(kek, cid[:], sid, 1, []byte("body"))
	if err != nil {
		t.Fatal(err)
	}
	sealed[4], sealed[5] = 0xFF, 0xFF
	if _, _, err := OpenSnapshotEnvelope(kek, cid[:], sealed); err == nil {
		t.Fatal("expected open failure for unsupported format version")
	}
}
