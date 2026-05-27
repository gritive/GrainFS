package protocred

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"sort"
	"strings"
	"time"
)

type Service struct {
	store *Store
	now   func() time.Time
}

type Option func(*Service)

func WithNow(now func() time.Time) Option {
	return func(s *Service) {
		if now != nil {
			s.now = now
		}
	}
}

func NewService(store *Store, opts ...Option) *Service {
	if store == nil {
		store = NewStore()
	}
	s := &Service{store: store, now: func() time.Time { return time.Now().UTC() }}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Service) Create(req CreateRequest) (Secret, error) {
	item, secret, err := MaterializeCreate(req, s.now())
	if err != nil {
		return Secret{}, err
	}
	s.store.put(item)
	return secret, nil
}

// MaterializeCreate builds the persisted row and one-time plaintext secret for
// a credential create operation without mutating a store.
func MaterializeCreate(req CreateRequest, now time.Time) (Credential, Secret, error) {
	if err := validateCreate(req); err != nil {
		return Credential{}, Secret{}, err
	}
	idRand, err := randomString(12)
	if err != nil {
		return Credential{}, Secret{}, err
	}
	secretRand, err := randomString(32)
	if err != nil {
		return Credential{}, Secret{}, err
	}
	id := "pc_" + idRand
	secret := "pcsec_" + secretRand
	item := Credential{
		ID:         id,
		SAID:       req.SAID,
		Protocol:   req.Protocol,
		Resource:   req.Resource,
		Mode:       req.Mode,
		SecretHash: sha256.Sum256([]byte(secret)),
		SecretHint: secretHint(secret),
		CreatedAt:  now.UTC(),
		CreatedBy:  req.CreatedBy,
		ExpiresAt:  cloneTime(req.ExpiresAt),
		Generation: 1,
	}
	return item, Secret{ID: id, Secret: secret, ConnectionHint: connectionHint(item, secret)}, nil
}

func (s *Service) List(filter ListFilter) []Credential {
	items := s.store.list(filter)
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ID < items[j].ID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return cloneCredentials(items)
}

func (s *Service) Get(id string) (Credential, error) {
	item, ok := s.store.get(id)
	if !ok {
		return Credential{}, ErrNotFound
	}
	return cloneCredential(item), nil
}

func (s *Service) Authenticate(req AuthenticateRequest) (Credential, error) {
	if req.Secret == "" || !validProtocol(req.Protocol) || !validMode(req.Mode) || !validResource(req.Resource) {
		return Credential{}, ErrInvalid
	}
	secretHash := sha256.Sum256([]byte(req.Secret))
	items := s.store.list(ListFilter{Protocol: req.Protocol})
	now := s.now()
	for _, item := range items {
		if subtle.ConstantTimeCompare(secretHash[:], item.SecretHash[:]) != 1 {
			continue
		}
		if item.Resource != req.Resource || item.Mode != req.Mode {
			return Credential{}, ErrInvalid
		}
		if item.RevokedAt != nil {
			return Credential{}, ErrRevoked
		}
		if item.ExpiresAt != nil && !now.Before(*item.ExpiresAt) {
			return Credential{}, ErrExpired
		}
		return cloneCredential(item), nil
	}
	return Credential{}, ErrNotFound
}

func (s *Service) Rotate(id string) (Secret, error) {
	item, ok := s.store.get(id)
	if !ok {
		return Secret{}, ErrNotFound
	}
	hash, hint, secret, err := MaterializeRotate(item)
	if err != nil {
		return Secret{}, err
	}
	_, _ = s.store.update(id, func(item Credential) Credential {
		item.SecretHash = hash
		item.SecretHint = hint
		return item
	})
	return secret, nil
}

// MaterializeRotate builds the next secret material for an existing credential
// without mutating a store.
func MaterializeRotate(item Credential) ([sha256.Size]byte, string, Secret, error) {
	if item.RevokedAt != nil {
		return [sha256.Size]byte{}, "", Secret{}, ErrRevoked
	}
	secretRand, err := randomString(32)
	if err != nil {
		return [sha256.Size]byte{}, "", Secret{}, err
	}
	secret := "pcsec_" + secretRand
	hash := sha256.Sum256([]byte(secret))
	hint := secretHint(secret)
	updated := item
	updated.SecretHash = hash
	updated.SecretHint = hint
	return hash, hint, Secret{ID: item.ID, Secret: secret, ConnectionHint: connectionHint(updated, secret)}, nil
}

func (s *Service) Revoke(id string) error {
	now := s.now()
	_, ok := s.store.update(id, func(item Credential) Credential {
		item.RevokedAt = &now
		return item
	})
	if !ok {
		return ErrNotFound
	}
	return nil
}

func connectionHint(item Credential, secret string) map[string]string {
	name := resourceName(item.Resource)
	switch item.Protocol {
	case ProtocolNBD:
		return map[string]string{"export_name": name + "@" + secret}
	case ProtocolNFS:
		return map[string]string{"mount_path": name + "/" + item.ID}
	case Protocol9P:
		return map[string]string{"aname": item.ID + "@" + name}
	case ProtocolS3:
		return map[string]string{"access_key_id": item.ID}
	case ProtocolIceberg:
		return map[string]string{"client_id": item.ID}
	default:
		return nil
	}
}

func randomString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func secretHint(secret string) string {
	if len(secret) <= 8 {
		return "****"
	}
	return secret[:6] + "..." + secret[len(secret)-4:]
}

func cloneCredentials(items []Credential) []Credential {
	out := make([]Credential, len(items))
	for i, item := range items {
		out[i] = cloneCredential(item)
	}
	return out
}

func cloneCredential(item Credential) Credential {
	item.ExpiresAt = cloneTime(item.ExpiresAt)
	item.RevokedAt = cloneTime(item.RevokedAt)
	item.LastUsedAt = cloneTime(item.LastUsedAt)
	item.StaleAt = cloneTime(item.StaleAt)
	return item
}

func ParseProtocol(raw string) Protocol {
	return Protocol(strings.ToLower(raw))
}

func ParseMode(raw string) Mode {
	return Mode(strings.ToLower(raw))
}
