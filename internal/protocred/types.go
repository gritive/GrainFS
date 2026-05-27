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
)

type CreateRequest struct {
	SAID      string
	Protocol  Protocol
	Resource  string
	Mode      Mode
	ExpiresAt *time.Time
	CreatedBy string
}

type ListFilter struct {
	SAID     string
	Protocol Protocol
}

type Secret struct {
	ID             string
	Secret         string
	ConnectionHint map[string]string
}

type Credential struct {
	ID         string
	SAID       string
	Protocol   Protocol
	Resource   string
	Mode       Mode
	SecretHash [sha256.Size]byte
	SecretHint string
	CreatedAt  time.Time
	CreatedBy  string
	ExpiresAt  *time.Time
	RevokedAt  *time.Time
	LastUsedAt *time.Time
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
