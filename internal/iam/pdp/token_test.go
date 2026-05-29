package pdp

import (
	"encoding/json"
	"testing"
)

func TestTokenEnvelope_RoundTrip(t *testing.T) {
	env := TokenEnvelope{CTB64: "YWJj", DEKGen: 7}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseTokenEnvelope(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.CTB64 != "YWJj" || got.DEKGen != 7 {
		t.Fatalf("got %+v", got)
	}
}

func TestParseTokenEnvelope_RejectsPlaintextShape(t *testing.T) {
	if _, err := ParseTokenEnvelope([]byte(`my-plaintext-token`)); err == nil {
		t.Fatal("plaintext must be rejected by envelope validator")
	}
	if _, err := ParseTokenEnvelope([]byte(`{"ct_b64":"","dek_gen":1}`)); err == nil {
		t.Fatal("empty ciphertext must be rejected")
	}
	if _, err := ParseTokenEnvelope([]byte(`{"ct_b64":"!!!notbase64","dek_gen":1}`)); err == nil {
		t.Fatal("non-base64 ciphertext must be rejected")
	}
}

func TestSealOpenToken_RoundTrip(t *testing.T) {
	// In-memory fake wrap/unwrap to exercise SealToken/OpenToken plumbing.
	wrap := func(saID, ak, pt string) ([]byte, uint32, error) {
		return []byte("CT:" + saID + ":" + ak + ":" + pt), 3, nil
	}
	unwrap := func(saID, ak string, gen uint32, ct []byte) (string, error) {
		// trivial inverse for the fake
		want := "CT:" + saID + ":" + ak + ":"
		s := string(ct)
		if gen != 3 || len(s) < len(want) || s[:len(want)] != want {
			t.Fatalf("unwrap mismatch: gen=%d ct=%q", gen, s)
		}
		return s[len(want):], nil
	}
	envJSON, err := SealToken(wrap, "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	env, err := ParseTokenEnvelope(envJSON)
	if err != nil {
		t.Fatal(err)
	}
	got, err := OpenToken(unwrap, env)
	if err != nil {
		t.Fatal(err)
	}
	if got != "secret-token" {
		t.Fatalf("round-trip got %q", got)
	}
}

func TestFingerprint(t *testing.T) {
	fp := Fingerprint("hello")
	if len(fp) != 8 {
		t.Fatalf("fingerprint len = %d, want 8", len(fp))
	}
	if fp == Fingerprint("world") {
		t.Fatal("distinct tokens must have distinct fingerprints")
	}
}
