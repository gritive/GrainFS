package cluster

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/protocred"
)

// ProtocolCredentialProposeFunc proposes an encoded protocol credential
// command through Meta Raft and returns after local apply.
type ProtocolCredentialProposeFunc func(context.Context, MetaCmdType, []byte) error

// ProtocolCredentialService keeps protocol credential reads local while routing
// mutations through Meta Raft.
type ProtocolCredentialService struct {
	store    *protocred.Store
	propose  ProtocolCredentialProposeFunc
	now      func() time.Time
	envelope protocred.SecretEnvelope
}

type ProtocolCredentialServiceOption func(*ProtocolCredentialService)

func WithProtocolCredentialSecretEnvelope(envelope protocred.SecretEnvelope) ProtocolCredentialServiceOption {
	return func(s *ProtocolCredentialService) {
		s.envelope = envelope
	}
}

func NewProtocolCredentialService(store *protocred.Store, propose ProtocolCredentialProposeFunc, opts ...ProtocolCredentialServiceOption) *ProtocolCredentialService {
	if store == nil {
		store = protocred.NewStore()
	}
	s := &ProtocolCredentialService{
		store:   store,
		propose: propose,
		now:     func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *ProtocolCredentialService) Create(req protocred.CreateRequest) (protocred.Secret, error) {
	if s.propose == nil {
		return protocred.Secret{}, protocred.ErrInvalid
	}
	row, secret, err := protocred.MaterializeCreateWithEnvelope(req, s.now(), s.envelope)
	if err != nil {
		return protocred.Secret{}, err
	}
	requestID, err := protocolCredentialRequestID()
	if err != nil {
		return protocred.Secret{}, err
	}
	payload, err := encodeProtocolCredentialCreateCmd(ProtocolCredentialCreateCmd{
		RequestID:  requestID,
		Credential: row,
	})
	if err != nil {
		return protocred.Secret{}, err
	}
	if err := s.propose(context.Background(), MetaCmdTypeProtocolCredentialCreate, payload); err != nil {
		return protocred.Secret{}, err
	}
	return secret, nil
}

func (s *ProtocolCredentialService) List(filter protocred.ListFilter) []protocred.Credential {
	return protocred.NewService(s.store, protocred.WithSecretEnvelope(s.envelope)).List(filter)
}

func (s *ProtocolCredentialService) Get(id string) (protocred.Credential, error) {
	return protocred.NewService(s.store, protocred.WithSecretEnvelope(s.envelope)).Get(id)
}

func (s *ProtocolCredentialService) Authenticate(req protocred.AuthenticateRequest) (protocred.Credential, error) {
	return protocred.NewService(s.store, protocred.WithSecretEnvelope(s.envelope)).Authenticate(req)
}

func (s *ProtocolCredentialService) Rotate(id string) (protocred.Secret, error) {
	if s.propose == nil {
		return protocred.Secret{}, protocred.ErrInvalid
	}
	item, err := s.Get(id)
	if err != nil {
		return protocred.Secret{}, err
	}
	hash, hint, enc, secret, err := protocred.MaterializeRotateWithEnvelope(item, s.envelope)
	if err != nil {
		return protocred.Secret{}, err
	}
	requestID, err := protocolCredentialRequestID()
	if err != nil {
		return protocred.Secret{}, err
	}
	payload, err := encodeProtocolCredentialRotateCmd(ProtocolCredentialRotateCmd{
		RequestID:  requestID,
		ID:         id,
		SecretHash: hash,
		SecretHint: hint,
		RotatedAt:  s.now(),
		SecretEnc:  enc,
	})
	if err != nil {
		return protocred.Secret{}, err
	}
	if err := s.propose(context.Background(), MetaCmdTypeProtocolCredentialRotate, payload); err != nil {
		return protocred.Secret{}, err
	}
	return secret, nil
}

func (s *ProtocolCredentialService) Revoke(id string) error {
	if s.propose == nil {
		return protocred.ErrInvalid
	}
	requestID, err := protocolCredentialRequestID()
	if err != nil {
		return err
	}
	payload, err := encodeProtocolCredentialRevokeCmd(ProtocolCredentialRevokeCmd{
		RequestID: requestID,
		ID:        id,
		RevokedAt: s.now(),
	})
	if err != nil {
		return err
	}
	return s.propose(context.Background(), MetaCmdTypeProtocolCredentialRevoke, payload)
}

func protocolCredentialRequestID() (string, error) {
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("protocol credential request id: %w", err)
	}
	return "pcreq_" + base64.RawURLEncoding.EncodeToString(b[:]), nil
}
