package s3auth

import (
	"net/http"
	"testing"
)

func TestVerifyValidSignature(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "AKID", "SECRET", "us-east-1")

	accessKey, err := v.Verify(req)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if accessKey != "AKID" {
		t.Fatalf("expected AKID, got %s", accessKey)
	}
}

func TestVerifyInvalidSignature(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "AKID", "WRONG_SECRET", "us-east-1")

	_, err := v.Verify(req)
	if err == nil {
		t.Fatal("expected error for wrong secret")
	}
}

func TestVerifyMissingAuth(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)

	_, err := v.Verify(req)
	if err == nil {
		t.Fatal("expected error for missing auth")
	}
}

func TestVerifyUnknownAccessKey(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "AKID", SecretKey: "SECRET"},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:9000/test-bucket", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "UNKNOWN", "SECRET", "us-east-1")

	_, err := v.Verify(req)
	if err == nil {
		t.Fatal("expected error for unknown access key")
	}
}

func TestVerifyPutRequest(t *testing.T) {
	v := NewVerifier([]Credentials{
		{AccessKey: "mykey", SecretKey: "mysecret"},
	})

	req, _ := http.NewRequest(http.MethodPut, "http://localhost:9000/bucket/object.txt", nil)
	req.Host = "localhost:9000"
	SignRequest(req, "mykey", "mysecret", "us-east-1")

	accessKey, err := v.Verify(req)
	if err != nil {
		t.Fatalf("Verify PUT: %v", err)
	}
	if accessKey != "mykey" {
		t.Fatalf("expected mykey, got %s", accessKey)
	}
}
