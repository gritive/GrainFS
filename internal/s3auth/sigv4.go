package s3auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Credentials holds the access key and secret key.
type Credentials struct {
	AccessKey string
	SecretKey string
}

// Verifier verifies AWS Signature V4 requests.
type Verifier struct {
	creds map[string]string // accessKey -> secretKey
}

// NewVerifier creates a new SigV4 verifier.
func NewVerifier(creds []Credentials) *Verifier {
	m := make(map[string]string, len(creds))
	for _, c := range creds {
		m[c.AccessKey] = c.SecretKey
	}
	return &Verifier{creds: m}
}

// LookupSecret returns the secret key for the given access key, or empty if not found.
func (v *Verifier) LookupSecret(accessKey string) string {
	return v.creds[accessKey]
}

// Verify checks the Authorization header or query-string presigned parameters.
// Returns the access key if valid, or an error.
func (v *Verifier) Verify(r *http.Request) (string, error) {
	// Check for query-string presigned URL first
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return v.verifyPresigned(r)
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return "", fmt.Errorf("unsupported auth scheme")
	}

	parts := parseAuthHeader(auth)
	credential := parts["Credential"]
	signedHeaders := parts["SignedHeaders"]
	signature := parts["Signature"]

	if credential == "" || signedHeaders == "" || signature == "" {
		return "", fmt.Errorf("malformed Authorization header")
	}

	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", fmt.Errorf("malformed credential: %s", credential)
	}
	accessKey := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	secretKey, ok := v.creds[accessKey]
	if !ok {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	amzDate := r.Header.Get("X-Amz-Date")
	canonicalRequest := buildCanonicalRequest(r, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, scope, hex.EncodeToString(h[:]))

	expectedSig := calculateSignature(secretKey, date, region, service, stringToSign)

	if !hmac.Equal([]byte(signature), []byte(expectedSig)) {
		return "", fmt.Errorf("signature mismatch")
	}

	return accessKey, nil
}

// verifyPresigned verifies a query-string presigned URL.
func (v *Verifier) verifyPresigned(r *http.Request) (string, error) {
	q := r.URL.Query()

	algorithm := q.Get("X-Amz-Algorithm")
	if algorithm != "AWS4-HMAC-SHA256" {
		return "", fmt.Errorf("unsupported algorithm: %s", algorithm)
	}

	credential := q.Get("X-Amz-Credential")
	amzDate := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")

	if credential == "" || amzDate == "" || expiresStr == "" || signedHeaders == "" || signature == "" {
		return "", fmt.Errorf("missing presigned URL parameters")
	}

	// Check expiry
	reqTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return "", fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	expires, err := strconv.Atoi(expiresStr)
	if err != nil {
		return "", fmt.Errorf("invalid X-Amz-Expires: %w", err)
	}
	if time.Now().After(reqTime.Add(time.Duration(expires) * time.Second)) {
		return "", fmt.Errorf("presigned URL expired")
	}

	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return "", fmt.Errorf("malformed credential: %s", credential)
	}
	accessKey := credParts[0]
	date := credParts[1]
	region := credParts[2]
	service := credParts[3]

	secretKey, ok := v.creds[accessKey]
	if !ok {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	// Build canonical request for presigned URL: the query string includes all
	// X-Amz-* params except X-Amz-Signature
	canonicalRequest := buildPresignedCanonicalRequest(r, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, scope, hex.EncodeToString(h[:]))

	expectedSig := calculateSignature(secretKey, date, region, service, stringToSign)

	if !hmac.Equal([]byte(signature), []byte(expectedSig)) {
		return "", fmt.Errorf("signature mismatch")
	}

	return accessKey, nil
}

func parseAuthHeader(auth string) map[string]string {
	result := make(map[string]string)
	auth = strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 ")
	for _, part := range strings.Split(auth, ", ") {
		idx := strings.Index(part, "=")
		if idx == -1 {
			continue
		}
		result[strings.TrimSpace(part[:idx])] = part[idx+1:]
	}
	return result
}

func buildCanonicalRequest(r *http.Request, signedHeadersStr string) string {
	uri := r.URL.Path
	if uri == "" {
		uri = "/"
	}
	query := r.URL.RawQuery

	headerNames := strings.Split(signedHeadersStr, ";")
	sort.Strings(headerNames)

	var canonicalHeaders strings.Builder
	for _, h := range headerNames {
		val := ""
		if h == "host" {
			val = r.Host
		} else {
			val = r.Header.Get(http.CanonicalHeaderKey(h))
		}
		canonicalHeaders.WriteString(h + ":" + strings.TrimSpace(val) + "\n")
	}

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		r.Method, uri, query, canonicalHeaders.String(), signedHeadersStr, payloadHash)
}

func calculateSignature(secretKey, date, region, service, stringToSign string) string {
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte(service))
	signingKey := hmacSHA256(serviceKey, []byte("aws4_request"))
	sig := hmacSHA256(signingKey, []byte(stringToSign))
	return hex.EncodeToString(sig)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// buildPresignedCanonicalRequest creates the canonical request for presigned URL verification.
// For presigned URLs, the payload hash is always UNSIGNED-PAYLOAD and the query string
// excludes the X-Amz-Signature parameter.
func buildPresignedCanonicalRequest(r *http.Request, signedHeadersStr string) string {
	uri := r.URL.Path
	if uri == "" {
		uri = "/"
	}

	// Build canonical query string: include all query params except X-Amz-Signature, sorted
	q := r.URL.Query()
	q.Del("X-Amz-Signature")
	canonicalQuery := buildSortedQuery(q)

	headerNames := strings.Split(signedHeadersStr, ";")
	sort.Strings(headerNames)

	var canonicalHeaders strings.Builder
	for _, h := range headerNames {
		val := ""
		if h == "host" {
			val = r.Host
		} else {
			val = r.Header.Get(http.CanonicalHeaderKey(h))
		}
		canonicalHeaders.WriteString(h + ":" + strings.TrimSpace(val) + "\n")
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\nUNSIGNED-PAYLOAD",
		r.Method, uri, canonicalQuery, canonicalHeaders.String(), signedHeadersStr)
}

// buildSortedQuery produces a canonical query string (sorted, encoded).
func buildSortedQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		for _, v := range q[k] {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

// PresignURL generates a presigned URL valid for the given number of seconds.
func PresignURL(method, rawURL, accessKey, secretKey, region string, expires int) (string, error) {
	return PresignURLAt(method, rawURL, accessKey, secretKey, region, expires, time.Now().UTC())
}

// PresignURLAt generates a presigned URL using the specified time (for testing).
func PresignURLAt(method, rawURL, accessKey, secretKey, region string, expires int, now time.Time) (string, error) {
	now = now.UTC()
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse URL: %w", err)
	}

	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, date, region)
	signedHeaders := "host"

	q := u.Query()
	q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
	q.Set("X-Amz-Credential", credential)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(expires))
	q.Set("X-Amz-SignedHeaders", signedHeaders)

	canonicalQuery := buildSortedQuery(q)

	host := u.Host
	canonicalHeaders := "host:" + host + "\n"

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\nUNSIGNED-PAYLOAD",
		method, u.Path, canonicalQuery, canonicalHeaders, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, region)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, scope, hex.EncodeToString(h[:]))

	sig := calculateSignature(secretKey, date, region, "s3", stringToSign)

	q.Set("X-Amz-Signature", sig)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// SignRequest signs an HTTP request with AWS Signature V4 (for testing and internal use).
func SignRequest(r *http.Request, accessKey, secretKey, region string) {
	now := time.Now().UTC()
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := buildCanonicalRequest(r, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, region)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", amzDate, scope, hex.EncodeToString(h[:]))

	sig := calculateSignature(secretKey, date, region, "s3", stringToSign)
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", accessKey, date, region)

	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s, SignedHeaders=%s, Signature=%s",
		credential, signedHeaders, sig))
}
