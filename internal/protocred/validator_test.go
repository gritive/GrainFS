package protocred

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttachValidatorAllowsMatchingCredential(t *testing.T) {
	now := time.Unix(300, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	validator := NewAttachValidator(store, WithValidatorNow(func() time.Time { return now }))

	decision, err := validator.ValidateAttach(context.Background(), AttachRequest{
		Protocol:        ProtocolNFS,
		Resource:        "volume/devdisk",
		CredentialID:    secret.ID,
		PresentedSecret: secret.Secret,
		RequestedMode:   ModeRW,
		Strict:          true,
	})
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.Equal(t, AttachReasonAllowed, decision.Reason)
	require.Equal(t, "node-a", decision.SAID)
	require.Equal(t, ModeRW, decision.Mode)
	require.Equal(t, secret.ID, decision.CredentialID)
}

func TestAttachValidatorAllowsSecretOnlyCredentialLookup(t *testing.T) {
	now := time.Unix(350, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	validator := NewAttachValidator(store, WithValidatorNow(func() time.Time { return now }))

	req := attachReq(secret, secret.Secret)
	req.CredentialID = ""
	decision, err := validator.ValidateAttach(context.Background(), req)
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.Equal(t, AttachReasonAllowed, decision.Reason)
	require.Equal(t, secret.ID, decision.CredentialID)
}

func TestAttachValidatorRejectsRevokedExpiredWrongSecretAndSADisabled(t *testing.T) {
	now := time.Unix(400, 0).UTC()
	expiredAt := now.Add(-time.Second)
	store, expired := validatorStoreWithCredential(t, now, &expiredAt)
	validator := NewAttachValidator(store, WithValidatorNow(func() time.Time { return now }))

	decision, err := validator.ValidateAttach(context.Background(), attachReq(expired, expired.Secret))
	require.ErrorIs(t, err, ErrExpired)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonExpired, decision.Reason)

	store, active := validatorStoreWithCredential(t, now, nil)
	validator = NewAttachValidator(store, WithValidatorNow(func() time.Time { return now }))
	decision, err = validator.ValidateAttach(context.Background(), attachReq(active, "wrong"))
	require.ErrorIs(t, err, ErrInvalid)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonSecretMismatch, decision.Reason)

	_, err = store.ApplyRevoke(active.ID, now)
	require.NoError(t, err)
	decision, err = validator.ValidateAttach(context.Background(), attachReq(active, active.Secret))
	require.ErrorIs(t, err, ErrRevoked)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonRevoked, decision.Reason)

	store, disabled := validatorStoreWithCredential(t, now, nil)
	validator = NewAttachValidator(store,
		WithValidatorNow(func() time.Time { return now }),
		WithServiceAccountDisabled(func(saID string) bool { return saID == "node-a" }),
	)
	decision, err = validator.ValidateAttach(context.Background(), attachReq(disabled, disabled.Secret))
	require.ErrorIs(t, err, ErrRevoked)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonSADisabled, decision.Reason)
}

func TestAttachValidatorHandlesStaleStrictAndCompatModes(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	_, err := store.ApplyMarkStale(secret.ID, now, "policy_detached")
	require.NoError(t, err)
	validator := NewAttachValidator(store, WithValidatorNow(func() time.Time { return now }))

	strictReq := attachReq(secret, secret.Secret)
	strictReq.Strict = true
	decision, err := validator.ValidateAttach(context.Background(), strictReq)
	require.ErrorIs(t, err, ErrRevoked)
	require.False(t, decision.Allowed)
	require.True(t, decision.Stale)
	require.Equal(t, AttachReasonStaleStrict, decision.Reason)
	require.Equal(t, "policy_detached", decision.StaleReason)

	compatReq := strictReq
	compatReq.Strict = false
	decision, err = validator.ValidateAttach(context.Background(), compatReq)
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.True(t, decision.Stale)
	require.Equal(t, AttachReasonStaleCompat, decision.Reason)
	require.Equal(t, "policy_detached", decision.StaleReason)
}

func TestAttachValidatorCacheInvalidatesOnGenerationChange(t *testing.T) {
	now := time.Unix(600, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	validator := NewAttachValidator(store,
		WithValidatorNow(func() time.Time { return now }),
		WithAttachAllowTTL(time.Minute),
	)

	req := attachReq(secret, secret.Secret)
	decision, err := validator.ValidateAttach(context.Background(), req)
	require.NoError(t, err)
	require.True(t, decision.Allowed)

	_, err = store.ApplyRevoke(secret.ID, now)
	require.NoError(t, err)
	decision, err = validator.ValidateAttach(context.Background(), req)
	require.ErrorIs(t, err, ErrRevoked)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonRevoked, decision.Reason)
}

func TestAttachValidatorCacheInvalidatesOnServiceAccountDisable(t *testing.T) {
	now := time.Unix(650, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	disabled := false
	validator := NewAttachValidator(store,
		WithValidatorNow(func() time.Time { return now }),
		WithAttachAllowTTL(time.Minute),
		WithServiceAccountDisabled(func(string) bool { return disabled }),
	)
	req := attachReq(secret, secret.Secret)

	decision, err := validator.ValidateAttach(context.Background(), req)
	require.NoError(t, err)
	require.True(t, decision.Allowed)

	disabled = true
	decision, err = validator.ValidateAttach(context.Background(), req)
	require.ErrorIs(t, err, ErrRevoked)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonSADisabled, decision.Reason)
}

func TestAttachValidatorCacheCapsAllowAtCredentialExpiry(t *testing.T) {
	now := time.Unix(700, 0).UTC()
	expiresAt := now.Add(time.Second)
	store, secret := validatorStoreWithCredential(t, now, &expiresAt)
	clock := now
	validator := NewAttachValidator(store,
		WithValidatorNow(func() time.Time { return clock }),
		WithAttachAllowTTL(time.Minute),
	)
	req := attachReq(secret, secret.Secret)

	decision, err := validator.ValidateAttach(context.Background(), req)
	require.NoError(t, err)
	require.True(t, decision.Allowed)

	clock = expiresAt
	decision, err = validator.ValidateAttach(context.Background(), req)
	require.ErrorIs(t, err, ErrExpired)
	require.False(t, decision.Allowed)
	require.Equal(t, AttachReasonExpired, decision.Reason)
}

func TestAttachValidatorDoesNotCacheDenyLongerThanOneSecond(t *testing.T) {
	now := time.Unix(800, 0).UTC()
	store, secret := validatorStoreWithCredential(t, now, nil)
	clock := now
	validator := NewAttachValidator(store,
		WithValidatorNow(func() time.Time { return clock }),
		WithAttachDenyTTL(time.Minute),
	)
	req := attachReq(secret, secret.Secret)

	_, err := store.ApplyRevoke(secret.ID, now)
	require.NoError(t, err)
	decision, err := validator.ValidateAttach(context.Background(), req)
	require.ErrorIs(t, err, ErrRevoked)
	require.Equal(t, AttachReasonRevoked, decision.Reason)

	item, err := NewService(store).Get(secret.ID)
	require.NoError(t, err)
	item.RevokedAt = nil
	item.Generation++
	store.Restore([]Credential{item})

	clock = now.Add(time.Second)
	decision, err = validator.ValidateAttach(context.Background(), req)
	require.NoError(t, err)
	require.True(t, decision.Allowed)
}

func TestAttachValidatorPropagatesContextCancellation(t *testing.T) {
	store, _ := validatorStoreWithCredential(t, time.Unix(900, 0).UTC(), nil)
	validator := NewAttachValidator(store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := validator.ValidateAttach(ctx, AttachRequest{})
	require.True(t, errors.Is(err, context.Canceled), "err = %v", err)
}

func validatorStoreWithCredential(t *testing.T, now time.Time, expiresAt *time.Time) (*Store, Secret) {
	t.Helper()
	store := NewStore()
	row, secret, err := MaterializeCreate(CreateRequest{
		SAID:      "node-a",
		Protocol:  ProtocolNFS,
		Resource:  "volume/devdisk",
		Mode:      ModeRW,
		ExpiresAt: expiresAt,
	}, now)
	require.NoError(t, err)
	row.ID = secret.ID
	row.SecretHash = sha256.Sum256([]byte(secret.Secret))
	_, err = store.ApplyCreate(row)
	require.NoError(t, err)
	return store, secret
}

func attachReq(secret Secret, presentedSecret string) AttachRequest {
	return AttachRequest{
		Protocol:        ProtocolNFS,
		Resource:        "volume/devdisk",
		CredentialID:    secret.ID,
		PresentedSecret: presentedSecret,
		RequestedMode:   ModeRW,
		Strict:          true,
	}
}
