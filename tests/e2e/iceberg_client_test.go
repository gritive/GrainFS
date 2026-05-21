package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// newIcebergSigV4Client returns an *http.Client whose Transport signs every
// outbound request with SigV4 using service=s3 (matching GrainFS's
// s3auth.Verifier scope check). region is the SigV4 signing region (use
// "us-east-1" unless the cluster runs with a non-default region).
//
// Iceberg REST callers (apache/iceberg-go, custom raw http) reach GrainFS
// through this helper in e2e tests.
func newIcebergSigV4Client(t testing.TB, accessKey, secretKey, region string) *http.Client {
	t.Helper()
	return &http.Client{
		Transport: &sigv4IcebergRoundTripper{
			accessKey: accessKey,
			secretKey: secretKey,
			region:    region,
			service:   "s3",
			inner:     &http.Transport{DisableKeepAlives: true},
		},
		Timeout: 30 * time.Second,
	}
}

type sigv4IcebergRoundTripper struct {
	accessKey, secretKey string
	region, service      string
	inner                http.RoundTripper
}

func (rt *sigv4IcebergRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		bodyBytes = b
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}
	hash := sha256.Sum256(bodyBytes)
	payloadHash := hex.EncodeToString(hash[:])
	// GrainFS's verifier derives the payload hash from the X-Amz-Content-Sha256
	// header (fallback "UNSIGNED-PAYLOAD"). Set it before signing so client and
	// server hash the same canonical request — see signedGet in
	// heal_receipt_api_test.go for the same pattern.
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	creds := aws.Credentials{
		AccessKeyID:     rt.accessKey,
		SecretAccessKey: rt.secretKey,
		Source:          "e2e-iceberg-helper",
	}
	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), creds, req, payloadHash, rt.service, rt.region, time.Now()); err != nil {
		return nil, err
	}
	return rt.inner.RoundTrip(req)
}
