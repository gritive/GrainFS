package storage_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/zeebo/xxh3"
)

func TestInternalETag(t *testing.T) {
	data := []byte("hello world")
	got := storage.InternalETag(data)

	if len(got) != 16 {
		t.Fatalf("InternalETag len = %d, want 16", len(got))
	}

	// 결정론적 확인
	if got != storage.InternalETag(data) {
		t.Fatal("InternalETag must be deterministic")
	}

	// 예상값 검증
	h := xxh3.Hash(data)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], h)
	want := hex.EncodeToString(buf[:])
	if got != want {
		t.Fatalf("InternalETag = %q, want %q", got, want)
	}
}

func TestVerifyETag_MD5(t *testing.T) {
	data := []byte("test data")
	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])

	ok, err := storage.VerifyETag(bytes.NewReader(data), etag)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("VerifyETag(MD5) should return true for correct ETag")
	}
}

func TestVerifyETag_MD5_Mismatch(t *testing.T) {
	data := []byte("test data")
	etag := "00000000000000000000000000000000" // 32자, 틀린 MD5

	ok, err := storage.VerifyETag(bytes.NewReader(data), etag)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("VerifyETag(MD5) should return false for wrong ETag")
	}
}

func TestVerifyETag_XXH3(t *testing.T) {
	data := []byte("internal data")
	etag := storage.InternalETag(data)

	ok, err := storage.VerifyETag(bytes.NewReader(data), etag)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("VerifyETag(xxhash3) should return true for correct ETag")
	}
}

func TestVerifyETag_XXH3_Mismatch(t *testing.T) {
	data := []byte("internal data")
	etag := "0000000000000000" // 16자, 틀린 xxhash3

	ok, err := storage.VerifyETag(bytes.NewReader(data), etag)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("VerifyETag(xxhash3) should return false for wrong ETag")
	}
}

func TestVerifyETag_UnknownLength(t *testing.T) {
	data := []byte("data")
	etag := "DEL" // 3자 — delete marker sentinel

	ok, err := storage.VerifyETag(bytes.NewReader(data), etag)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("VerifyETag with unknown-length ETag should return false, not error")
	}
}

// TestVerifyETag_LegacyMD5_WithXXH3Migration: 마이그레이션 후에도 기존 MD5 ETag가
// 올바르게 검증되는지 확인하는 회귀 테스트.
func TestVerifyETag_LegacyMD5_WithXXH3Migration(t *testing.T) {
	data := []byte("old nfs file content — written before migration")
	h := md5.Sum(data)
	legacyETag := hex.EncodeToString(h[:]) // 32자 MD5

	ok, err := storage.VerifyETag(bytes.NewReader(data), legacyETag)
	if err != nil {
		t.Fatalf("VerifyETag(legacy MD5): %v", err)
	}
	if !ok {
		t.Fatal("VerifyETag must verify legacy MD5 ETags during migration period")
	}
}
