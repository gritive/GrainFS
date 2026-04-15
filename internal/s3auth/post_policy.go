package s3auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// postPolicy represents a parsed S3 POST policy document.
type postPolicy struct {
	Expiration string        `json:"expiration"`
	Conditions []interface{} `json:"conditions"`
}

// SignPostPolicy generates the HMAC-SHA256 signature for a base64-encoded policy.
func SignPostPolicy(policyB64, secretKey, date, region, service string) string {
	signingKey := deriveSigningKey(secretKey, date, region, service)
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte(policyB64))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyPostPolicy checks the signature of a base64-encoded POST policy.
func VerifyPostPolicy(policyB64, signature, secretKey, date, region, service string) error {
	expected := SignPostPolicy(policyB64, secretKey, date, region, service)
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// ValidatePostPolicyExpiration checks whether the policy has expired.
func ValidatePostPolicyExpiration(policyB64 string) error {
	p, err := decodePolicy(policyB64)
	if err != nil {
		return err
	}

	exp, err := time.Parse("2006-01-02T15:04:05Z", p.Expiration)
	if err != nil {
		return fmt.Errorf("invalid expiration format: %w", err)
	}

	if time.Now().UTC().After(exp) {
		return fmt.Errorf("policy expired at %s", p.Expiration)
	}
	return nil
}

// ValidatePostPolicyConditions validates form fields against the policy conditions.
// Supported condition types: exact match ({"field":"value"}), starts-with, content-length-range.
func ValidatePostPolicyConditions(policyB64 string, formFields map[string]string) error {
	p, err := decodePolicy(policyB64)
	if err != nil {
		return err
	}

	for _, cond := range p.Conditions {
		switch c := cond.(type) {
		case map[string]interface{}:
			// Exact match: {"bucket": "mybucket"}
			for field, expected := range c {
				actual, ok := formFields[field]
				if !ok {
					return fmt.Errorf("condition field %q missing from form", field)
				}
				if actual != fmt.Sprint(expected) {
					return fmt.Errorf("condition field %q: expected %q, got %q", field, expected, actual)
				}
			}
		case []interface{}:
			if len(c) < 1 {
				continue
			}
			op, _ := c[0].(string)
			switch strings.ToLower(op) {
			case "starts-with":
				if len(c) != 3 {
					return fmt.Errorf("starts-with requires 3 elements")
				}
				field, _ := c[1].(string)
				prefix, _ := c[2].(string)
				field = strings.TrimPrefix(field, "$")
				actual := formFields[field]
				if !strings.HasPrefix(actual, prefix) {
					return fmt.Errorf("field %q must start with %q, got %q", field, prefix, actual)
				}
			case "content-length-range":
				// Validated separately with actual content length, skip here
			case "eq":
				if len(c) != 3 {
					continue
				}
				field, _ := c[1].(string)
				expected, _ := c[2].(string)
				field = strings.TrimPrefix(field, "$")
				if formFields[field] != expected {
					return fmt.Errorf("field %q must equal %q", field, expected)
				}
			}
		}
	}
	return nil
}

func decodePolicy(policyB64 string) (*postPolicy, error) {
	data, err := base64.StdEncoding.DecodeString(policyB64)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 policy: %w", err)
	}
	var p postPolicy
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("invalid policy JSON: %w", err)
	}
	return &p, nil
}

// deriveSigningKey computes the SigV4 signing key.
// hmacSHA256 is defined in sigv4.go and reused here.
func deriveSigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}
