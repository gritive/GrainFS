package encrypt

import (
	"bytes"
	"testing"
)

// R1 swapped the logical/PITR WAL, packblob, and single-node PUT pipeline
// sealers from the static Encryptor (lock-free SealValueAADTo) to the gen-aware
// DEKKeeper (SealWithAAD, which takes k.mu.RLock per op). These benchmarks gate
// that swap: the DEK seal/open hot path must stay within run-to-run noise of the
// static analogue. Run as: -bench='Hot' -benchtime=15s -count=3 -run='^$',
// compare medians (per the 15s x 3-run rule).

const (
	benchHotPayload = 4096 // 4 KiB, the dominant logical-WAL/packblob entry size
)

func benchDEKKeeper(b *testing.B) *DEKKeeper {
	b.Helper()
	kek := bytes.Repeat([]byte{0x42}, KEKSize)
	clusterID := bytes.Repeat([]byte{0x11}, 16)
	k, err := NewDEKKeeper(kek, clusterID)
	if err != nil {
		b.Fatal(err)
	}
	return k
}

func benchHotAAD() []byte { return bytes.Repeat([]byte{0x7e}, 32) }

// BenchmarkDEKKeeperSealHot — the AFTER path (gen-aware DEK, per-op RLock).
func BenchmarkDEKKeeperSealHot(b *testing.B) {
	k := benchDEKKeeper(b)
	aad := benchHotAAD()
	plain := bytes.Repeat([]byte("x"), benchHotPayload)
	b.SetBytes(benchHotPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ct, _, err := k.SealWithAAD(plain, aad)
		if err != nil {
			b.Fatal(err)
		}
		if len(ct) == 0 {
			b.Fatal("empty ciphertext")
		}
	}
}

// BenchmarkDEKKeeperOpenHot — the AFTER open path (per-op RLock).
func BenchmarkDEKKeeperOpenHot(b *testing.B) {
	k := benchDEKKeeper(b)
	aad := benchHotAAD()
	plain := bytes.Repeat([]byte("x"), benchHotPayload)
	ct, gen, err := k.SealWithAAD(plain, aad)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(benchHotPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pt, err := k.OpenWithAAD(ct, gen, aad)
		if err != nil {
			b.Fatal(err)
		}
		if len(pt) != benchHotPayload {
			b.Fatal("short plaintext")
		}
	}
}

// BenchmarkEncryptorSealHot — the BEFORE analogue (static Encryptor, lock-free).
// Same 4 KiB + 32-byte AAD so the median is directly comparable to the DEK seal.
func BenchmarkEncryptorSealHot(b *testing.B) {
	enc := benchmarkEncryptor(b)
	aad := benchHotAAD()
	plain := bytes.Repeat([]byte("x"), benchHotPayload)
	b.SetBytes(benchHotPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ct, err := enc.SealValueAADTo(nil, aad, plain)
		if err != nil {
			b.Fatal(err)
		}
		if len(ct) == 0 {
			b.Fatal("empty ciphertext")
		}
	}
}

// BenchmarkEncryptorOpenHot — the BEFORE open analogue (static, lock-free).
func BenchmarkEncryptorOpenHot(b *testing.B) {
	enc := benchmarkEncryptor(b)
	aad := benchHotAAD()
	plain := bytes.Repeat([]byte("x"), benchHotPayload)
	ct, err := enc.SealValueAADTo(nil, aad, plain)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(benchHotPayload)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pt, err := enc.OpenValueAADTo(nil, aad, ct)
		if err != nil {
			b.Fatal(err)
		}
		if len(pt) != benchHotPayload {
			b.Fatal("short plaintext")
		}
	}
}
