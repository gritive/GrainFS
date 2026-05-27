package cluster

import "testing"

func TestFSMSealOpenSnapshotEnvelopeRoundTrip(t *testing.T) {
	fsm, _ := newTestMetaFSMWithKEKAndDEK(t)
	body := []byte("plaintext fsm blob")
	sealed, err := fsm.sealSnapshotEnvelope(body)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	plain, err := fsm.openSnapshotEnvelope(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if string(plain) != string(body) {
		t.Fatalf("round-trip mismatch: got %q", plain)
	}
}
