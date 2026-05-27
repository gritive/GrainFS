package admin

import (
	"context"
	"errors"
	"time"

	"github.com/gritive/GrainFS/internal/protocred"
)

func CreateCredential(ctx context.Context, d *Deps, req CredentialCreateReq) (CredentialResp, error) {
	if d.ProtocolCredentials == nil {
		return CredentialResp{}, NewUnsupported("protocol credential admin not configured on this node", nil)
	}
	expiresAt, err := parseCredentialTime(req.ExpiresAt)
	if err != nil {
		return CredentialResp{}, NewInvalid("invalid expires_at: " + err.Error())
	}
	secret, err := d.ProtocolCredentials.Create(protocred.CreateRequest{
		SAID:      req.SAID,
		Protocol:  protocred.ParseProtocol(req.Protocol),
		Resource:  req.Resource,
		Mode:      protocred.ParseMode(req.Mode),
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return CredentialResp{}, credentialError(err)
	}
	item, err := d.ProtocolCredentials.Get(secret.ID)
	if err != nil {
		return CredentialResp{}, credentialError(err)
	}
	return credentialResp(item, secret), nil
}

func ListCredentials(ctx context.Context, d *Deps, req CredentialListReq) (CredentialListResp, error) {
	if d.ProtocolCredentials == nil {
		return CredentialListResp{}, NewUnsupported("protocol credential admin not configured on this node", nil)
	}
	items := d.ProtocolCredentials.List(protocred.ListFilter{SAID: req.SAID, Protocol: protocred.ParseProtocol(req.Protocol)})
	out := make([]CredentialResp, len(items))
	for i, item := range items {
		out[i] = credentialResp(item, protocred.Secret{})
	}
	return CredentialListResp{Credentials: out}, nil
}

func GetCredential(ctx context.Context, d *Deps, id string) (CredentialResp, error) {
	if d.ProtocolCredentials == nil {
		return CredentialResp{}, NewUnsupported("protocol credential admin not configured on this node", nil)
	}
	item, err := d.ProtocolCredentials.Get(id)
	if err != nil {
		return CredentialResp{}, credentialError(err)
	}
	return credentialResp(item, protocred.Secret{}), nil
}

func RotateCredential(ctx context.Context, d *Deps, id string) (CredentialResp, error) {
	if d.ProtocolCredentials == nil {
		return CredentialResp{}, NewUnsupported("protocol credential admin not configured on this node", nil)
	}
	secret, err := d.ProtocolCredentials.Rotate(id)
	if err != nil {
		return CredentialResp{}, credentialError(err)
	}
	item, err := d.ProtocolCredentials.Get(id)
	if err != nil {
		return CredentialResp{}, credentialError(err)
	}
	return credentialResp(item, secret), nil
}

func RevokeCredential(ctx context.Context, d *Deps, id string) (CredentialRevokeResp, error) {
	if d.ProtocolCredentials == nil {
		return CredentialRevokeResp{}, NewUnsupported("protocol credential admin not configured on this node", nil)
	}
	if err := d.ProtocolCredentials.Revoke(id); err != nil {
		return CredentialRevokeResp{}, credentialError(err)
	}
	return CredentialRevokeResp{ID: id, Revoked: true}, nil
}

func credentialResp(item protocred.Credential, secret protocred.Secret) CredentialResp {
	return CredentialResp{
		ID:             item.ID,
		SAID:           item.SAID,
		Protocol:       string(item.Protocol),
		Resource:       item.Resource,
		Mode:           string(item.Mode),
		Secret:         secret.Secret,
		SecretHint:     item.SecretHint,
		ConnectionHint: secret.ConnectionHint,
		CreatedAt:      formatCredentialTime(&item.CreatedAt),
		ExpiresAt:      formatCredentialTime(item.ExpiresAt),
		RevokedAt:      formatCredentialTime(item.RevokedAt),
		LastUsedAt:     formatCredentialTime(item.LastUsedAt),
	}
}

func parseCredentialTime(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func formatCredentialTime(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func credentialError(err error) *Error {
	switch {
	case errors.Is(err, protocred.ErrNotFound):
		return NewNotFound("protocol credential not found")
	case errors.Is(err, protocred.ErrRevoked):
		return NewConflict("protocol credential is revoked", nil)
	case errors.Is(err, protocred.ErrInvalid):
		return NewInvalid(err.Error())
	default:
		return NewInternal(err.Error())
	}
}
