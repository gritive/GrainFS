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
	creds map[string]string // accessKey -> secretKey (legacy static map; empty when IAM is the source of truth)

	// SecretLookup is consulted when the static creds map misses. IAM
	// wires this up to its Store; nil means static-only behavior (legacy).
	SecretLookup func(accessKey string) (secret string, ok bool)
}

// NewVerifier creates a new SigV4 verifier.
func NewVerifier(creds []Credentials) *Verifier {
	m := make(map[string]string, len(creds))
	for _, c := range creds {
		m[c.AccessKey] = c.SecretKey
	}
	return &Verifier{creds: m}
}

// LookupSecret returns the secret key for the given access key.
// Consults the static creds map first, then SecretLookup if non-nil.
// Returns empty string if neither has the key.
func (v *Verifier) LookupSecret(accessKey string) string {
	if s, ok := v.creds[accessKey]; ok {
		return s
	}
	if v.SecretLookup != nil {
		if s, ok := v.SecretLookup(accessKey); ok {
			return s
		}
	}
	return ""
}

// Verify checks the Authorization header or query-string presigned parameters.
// Returns the access key if valid, or an error.
func (v *Verifier) Verify(r *http.Request) (string, error) {
	// Check for query-string presigned URL first
	if hasPresignedAlgorithm(r) {
		return v.verifyPresigned(r)
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return "", fmt.Errorf("unsupported auth scheme")
	}

	parts := parseAuthHeaderFields(auth)
	credential := parts.Credential
	signedHeaders := parts.SignedHeaders
	signature := parts.Signature

	if credential == "" || signedHeaders == "" || signature == "" {
		return "", fmt.Errorf("malformed Authorization header")
	}

	accessKey, date, region, service, ok := parseCredentialParts(credential)
	if !ok {
		return "", fmt.Errorf("malformed credential: %s", credential)
	}

	secretKey := v.LookupSecret(accessKey)
	if secretKey == "" {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	amzDate := r.Header.Get("X-Amz-Date")
	canonicalRequest := buildCanonicalRequest(r, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	signingKey := DeriveSigningKey(secretKey, date, region, service)
	if !equalHexMAC(signature, hmacSHA256(signingKey, stringToSignBytes(amzDate, date, region, service, h))) {
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

	secretKey := v.LookupSecret(accessKey)
	if secretKey == "" {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	// Build canonical request for presigned URL: the query string includes all
	// X-Amz-* params except X-Amz-Signature
	canonicalRequest := buildPresignedCanonicalRequest(r, signedHeaders)

	h := sha256.Sum256([]byte(canonicalRequest))
	signingKey := DeriveSigningKey(secretKey, date, region, service)
	if !equalHexMAC(signature, hmacSHA256(signingKey, stringToSignBytes(amzDate, date, region, service, h))) {
		return "", fmt.Errorf("signature mismatch")
	}

	return accessKey, nil
}

type authHeaderFields struct {
	Credential    string
	SignedHeaders string
	Signature     string
}

func parseAuthHeaderFields(auth string) authHeaderFields {
	auth = strings.TrimPrefix(auth, "AWS4-HMAC-SHA256 ")
	var out authHeaderFields
	for auth != "" {
		part, rest, found := strings.Cut(auth, ",")
		part = strings.TrimSpace(part)
		key, val, ok := strings.Cut(part, "=")
		if ok {
			switch strings.TrimSpace(key) {
			case "Credential":
				out.Credential = val
			case "SignedHeaders":
				out.SignedHeaders = val
			case "Signature":
				out.Signature = val
			}
		}
		if !found {
			break
		}
		auth = rest
	}
	return out
}

func parseCredentialParts(credential string) (accessKey, date, region, service string, ok bool) {
	accessKey, rest, ok := strings.Cut(credential, "/")
	if !ok {
		return "", "", "", "", false
	}
	date, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", "", false
	}
	region, rest, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", "", false
	}
	service, _, ok = strings.Cut(rest, "/")
	if !ok {
		return "", "", "", "", false
	}
	return accessKey, date, region, service, accessKey != "" && date != "" && region != "" && service != ""
}

func hasPresignedAlgorithm(r *http.Request) bool {
	raw := r.URL.RawQuery
	for raw != "" {
		part, rest, found := strings.Cut(raw, "&")
		key, _, _ := strings.Cut(part, "=")
		if queryPartHasNonEmptyValue(part) && (key == "X-Amz-Algorithm" || unescapesTo(key, "X-Amz-Algorithm")) {
			return true
		}
		if !found {
			return false
		}
		raw = rest
	}
	return false
}

func queryPartHasNonEmptyValue(part string) bool {
	_, value, ok := strings.Cut(part, "=")
	return ok && value != ""
}

func unescapesTo(s, want string) bool {
	if !strings.Contains(s, "%") {
		return false
	}
	unescaped, err := url.QueryUnescape(s)
	return err == nil && unescaped == want
}

func buildCanonicalRequest(r *http.Request, signedHeadersStr string) string {
	uri := r.URL.EscapedPath()
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
			val = r.Header.Get(h)
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

// DeriveSigningKey computes the SigV4 signing key from credential components.
// The key is stable for the calendar day given the same (secret, date, region, service).
func DeriveSigningKey(secretKey, date, region, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte(service))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

func calculateSignature(secretKey, date, region, service, stringToSign string) string {
	signingKey := DeriveSigningKey(secretKey, date, region, service)
	return hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
}

// VerifyWithSigningKey verifies a request using a pre-derived signing key.
// Skips the 4-round key derivation — use when the signing key is cached.
// Still performs full canonical request construction and HMAC verification.
func (v *Verifier) VerifyWithSigningKey(r *http.Request, accessKey string, signingKey []byte) error {
	if hasPresignedAlgorithm(r) {
		return v.verifyPresignedWithKey(r, signingKey)
	}
	return v.verifyHeaderWithKey(r, accessKey, signingKey)
}

func (v *Verifier) verifyHeaderWithKey(r *http.Request, accessKey string, signingKey []byte) error {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return fmt.Errorf("unsupported auth scheme")
	}
	parts := parseAuthHeaderFields(auth)
	_, date, region, service, ok := parseCredentialParts(parts.Credential)
	if !ok {
		return fmt.Errorf("malformed credential")
	}
	signedHeaders := parts.SignedHeaders
	signature := parts.Signature
	if signedHeaders == "" || signature == "" {
		return fmt.Errorf("malformed Authorization header")
	}
	amzDate := r.Header.Get("X-Amz-Date")

	canonicalRequest := buildCanonicalRequest(r, signedHeaders)
	h := sha256.Sum256([]byte(canonicalRequest))

	if !equalHexMAC(signature, hmacSHA256(signingKey, stringToSignBytes(amzDate, date, region, service, h))) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

func (v *Verifier) verifyPresignedWithKey(r *http.Request, signingKey []byte) error {
	q := r.URL.Query()
	amzDate := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")
	credential := q.Get("X-Amz-Credential")

	if credential == "" || amzDate == "" || expiresStr == "" || signedHeaders == "" || signature == "" {
		return fmt.Errorf("missing presigned URL parameters")
	}
	reqTime, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date: %w", err)
	}
	expires, err := strconv.Atoi(expiresStr)
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Expires: %w", err)
	}
	if time.Now().After(reqTime.Add(time.Duration(expires) * time.Second)) {
		return fmt.Errorf("presigned URL expired")
	}

	credParts := strings.Split(credential, "/")
	if len(credParts) < 5 {
		return fmt.Errorf("malformed credential")
	}
	date, region, service := credParts[1], credParts[2], credParts[3]

	canonicalRequest := buildPresignedCanonicalRequest(r, signedHeaders)
	h := sha256.Sum256([]byte(canonicalRequest))

	if !equalHexMAC(signature, hmacSHA256(signingKey, stringToSignBytes(amzDate, date, region, service, h))) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// stringToSignBytes assembles the SigV4 string-to-sign as a single []byte
// for direct hmac consumption. The legacy verifier built this with two
// fmt.Sprintf calls plus hex.EncodeToString plus a []byte conversion —
// four hot-path allocations replaced by one slice and a stack hex array.
// Byte-equivalent output preserves signature compatibility (verified by
// the SignRequest/Verify round-trip tests).
func stringToSignBytes(amzDate, date, region, service string, canonicalReqHash [sha256.Size]byte) []byte {
	var hexBuf [sha256.Size * 2]byte
	hex.Encode(hexBuf[:], canonicalReqHash[:])
	n := len("AWS4-HMAC-SHA256\n") + len(amzDate) + 1 +
		len(date) + 1 + len(region) + 1 + len(service) + len("/aws4_request\n") +
		len(hexBuf)
	out := make([]byte, 0, n)
	out = append(out, "AWS4-HMAC-SHA256\n"...)
	out = append(out, amzDate...)
	out = append(out, '\n')
	out = append(out, date...)
	out = append(out, '/')
	out = append(out, region...)
	out = append(out, '/')
	out = append(out, service...)
	out = append(out, "/aws4_request\n"...)
	out = append(out, hexBuf[:]...)
	return out
}

func equalHexMAC(signature string, mac []byte) bool {
	if len(signature) != sha256.Size*2 {
		return false
	}
	var expected [sha256.Size * 2]byte
	hex.Encode(expected[:], mac)
	var diff byte
	for i, b := range expected {
		diff |= b ^ signature[i]
	}
	return diff == 0
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
	uri := r.URL.EscapedPath()
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
			val = r.Header.Get(h)
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
