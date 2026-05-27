package cluster

import (
	"bytes"
	"testing"
)

// TestBootstrapSecretsPayloadRoundTrip_Full encodes a payload with all fields
// populated (multi-gen KEK list) and asserts every field survives the round-trip.
func TestBootstrapSecretsPayloadRoundTrip_Full(t *testing.T) {
	encKey := []byte("encryption-key-32-bytes----------")
	psk := []byte("transport-psk-bytes")
	gens := []KEKGen{
		{Gen: 1, Key: []byte("kek-gen-1-key")},
		{Gen: 2, Key: []byte("kek-gen-2-key-longer")},
		{Gen: 7, Key: []byte("kek-gen-7")},
	}

	data := encodeBootstrapSecretsPayload(encKey, gens, psk)
	gotEnc, gotGens, gotPSK, err := decodeBootstrapSecretsPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(gotEnc, encKey) {
		t.Errorf("encKey: got %q, want %q", gotEnc, encKey)
	}
	if !bytes.Equal(gotPSK, psk) {
		t.Errorf("psk: got %q, want %q", gotPSK, psk)
	}
	if len(gotGens) != len(gens) {
		t.Fatalf("gens len: got %d, want %d", len(gotGens), len(gens))
	}
	for i := range gens {
		if gotGens[i].Gen != gens[i].Gen {
			t.Errorf("gen[%d].Gen: got %d, want %d", i, gotGens[i].Gen, gens[i].Gen)
		}
		if !bytes.Equal(gotGens[i].Key, gens[i].Key) {
			t.Errorf("gen[%d].Key: got %q, want %q", i, gotGens[i].Key, gens[i].Key)
		}
	}
}

// TestBootstrapSecretsPayloadRoundTrip_Empty encodes an all-empty payload and
// asserts the decode yields empty/nil fields without error.
func TestBootstrapSecretsPayloadRoundTrip_Empty(t *testing.T) {
	data := encodeBootstrapSecretsPayload(nil, nil, nil)
	gotEnc, gotGens, gotPSK, err := decodeBootstrapSecretsPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(gotEnc) != 0 {
		t.Errorf("encKey: got %v, want empty", gotEnc)
	}
	if len(gotPSK) != 0 {
		t.Errorf("psk: got %v, want empty", gotPSK)
	}
	if len(gotGens) != 0 {
		t.Errorf("gens: got %v, want empty", gotGens)
	}
}

// TestBootstrapSecretsPayloadRoundTrip_SingleGen covers the common single-KEK
// case (no PSK), to verify the vector path for one element.
func TestBootstrapSecretsPayloadRoundTrip_SingleGen(t *testing.T) {
	encKey := []byte("only-enc-key")
	gens := []KEKGen{{Gen: 42, Key: []byte("solo-kek")}}

	data := encodeBootstrapSecretsPayload(encKey, gens, nil)
	gotEnc, gotGens, gotPSK, err := decodeBootstrapSecretsPayload(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(gotEnc, encKey) {
		t.Errorf("encKey mismatch")
	}
	if len(gotPSK) != 0 {
		t.Errorf("psk: got %v, want empty", gotPSK)
	}
	if len(gotGens) != 1 || gotGens[0].Gen != 42 || !bytes.Equal(gotGens[0].Key, gens[0].Key) {
		t.Errorf("gens: got %+v, want %+v", gotGens, gens)
	}
}

// TestSealedBootstrapRoundTrip encodes and decodes the outer SealedBootstrap
// envelope.
func TestSealedBootstrapRoundTrip(t *testing.T) {
	eph := []byte("ephemeral-public-key-bytes")
	ct := []byte("sealed-ciphertext-bytes-blob")

	data := encodeSealedBootstrap(eph, ct)
	gotEph, gotCT, err := decodeSealedBootstrap(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(gotEph, eph) {
		t.Errorf("ephemeralPub: got %q, want %q", gotEph, eph)
	}
	if !bytes.Equal(gotCT, ct) {
		t.Errorf("ciphertext: got %q, want %q", gotCT, ct)
	}
}

// TestSealedBootstrapRoundTrip_Empty asserts empty inputs decode to nil fields.
func TestSealedBootstrapRoundTrip_Empty(t *testing.T) {
	data := encodeSealedBootstrap(nil, nil)
	gotEph, gotCT, err := decodeSealedBootstrap(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(gotEph) != 0 || len(gotCT) != 0 {
		t.Errorf("expected empty fields, got eph=%v ct=%v", gotEph, gotCT)
	}
}
