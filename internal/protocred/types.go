package protocred

import (
	"crypto/sha256"
	"errors"
	"strings"
	"time"
)

type Protocol string

const (
	ProtocolS3      Protocol = "s3"
	ProtocolIceberg Protocol = "iceberg"
	ProtocolNFS     Protocol = "nfs"
	Protocol9P      Protocol = "9p"
	ProtocolNBD     Protocol = "nbd"
)

type Mode string

const (
	ModeRO Mode = "ro"
	ModeRW Mode = "rw"
)

var (
	ErrInvalid  = errors.New("protocol credential invalid")
	ErrNotFound = errors.New("protocol credential not found")
	ErrRevoked  = errors.New("protocol credential revoked")
	ErrExpired  = errors.New("protocol credential expired")
	ErrConflict = errors.New("protocol credential conflict")
)

type CreateRequest struct {
	SAID      string
	Protocol  Protocol
	Resource  string
	Mode      Mode
	ExpiresAt *time.Time
	CreatedBy string
}

type AuthenticateRequest struct {
	Protocol Protocol
	Resource string
	Mode     Mode
	Secret   string
}

type ListFilter struct {
	SAID     string
	Protocol Protocol
	Resource string
}

type Secret struct {
	ID             string
	Secret         string
	ConnectionHint map[string]string
}

type Credential struct {
	ID          string
	SAID        string
	Protocol    Protocol
	Resource    string
	Mode        Mode
	SecretHash  [sha256.Size]byte
	SecretHint  string
	SecretEnc   []byte
	CreatedAt   time.Time
	CreatedBy   string
	ExpiresAt   *time.Time
	RevokedAt   *time.Time
	LastUsedAt  *time.Time
	Generation  uint64
	StaleAt     *time.Time
	StaleReason string
}

func validateCreate(req CreateRequest) error {
	if req.SAID == "" || req.Resource == "" {
		return ErrInvalid
	}
	if !validProtocol(req.Protocol) || !validMode(req.Mode) || !validResource(req.Resource) {
		return ErrInvalid
	}
	return nil
}

func ValidateCreateRequest(req CreateRequest) error {
	return validateCreate(req)
}

func validProtocol(p Protocol) bool {
	switch p {
	case ProtocolS3, ProtocolIceberg, ProtocolNFS, Protocol9P, ProtocolNBD:
		return true
	default:
		return false
	}
}

func validMode(m Mode) bool {
	return m == ModeRO || m == ModeRW
}

func validResource(resource string) bool {
	for _, prefix := range []string{"bucket/", "volume/", "catalog/"} {
		if strings.HasPrefix(resource, prefix) && len(resource) > len(prefix) {
			return true
		}
	}
	return false
}

func resourceName(resource string) string {
	_, name, ok := strings.Cut(resource, "/")
	if !ok {
		return resource
	}
	return name
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	copy := *t
	return &copy
}

type SecretEnvelope interface {
	SealProtocolCredentialSecret(aad []byte, plaintext string) ([]byte, error)
	OpenProtocolCredentialSecret(aad []byte, ciphertext []byte) (string, error)
}

func SecretAAD(item Credential) []byte {
	out := make([]byte, 0, len(item.ID)+len(item.SAID)+len(item.Protocol)+len(item.Resource)+len(item.Mode)+32)
	out = append(out, "grainfs-protocol-credential-secret-v1"...)
	out = appendAADField(out, item.ID)
	out = appendAADField(out, item.SAID)
	out = appendAADField(out, string(item.Protocol))
	out = appendAADField(out, item.Resource)
	out = appendAADField(out, string(item.Mode))
	return out
}

func appendAADField(out []byte, s string) []byte {
	out = append(out, 0)
	out = append(out, s...)
	return out
}
