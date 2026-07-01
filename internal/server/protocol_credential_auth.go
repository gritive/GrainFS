package server

import (
	"context"
	"net/http"

	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/s3auth"
)

type protocolCredentialAuth struct {
	store    *protocred.Store
	envelope protocred.SecretEnvelope
	verifier *s3auth.CachingVerifier
}

func newProtocolCredentialAuth(store *protocred.Store, envelope protocred.SecretEnvelope) *protocolCredentialAuth {
	a := &protocolCredentialAuth{store: store, envelope: envelope}
	inner := s3auth.NewVerifier(nil)
	inner.SecretLookup = a.lookupSecret
	a.verifier = s3auth.NewCachingVerifier(inner, 4096, 0)
	return a
}

func (a *protocolCredentialAuth) lookupSecret(accessKey string) (string, bool) {
	secret, item, err := protocred.NewService(a.store, protocred.WithSecretEnvelope(a.envelope)).OpenSecret(accessKey)
	if err != nil {
		return "", false
	}
	switch item.Protocol {
	case protocred.ProtocolS3:
		return secret, true
	default:
		return "", false
	}
}

func (a *protocolCredentialAuth) credential(accessKey string) (protocred.Credential, bool) {
	item, err := protocred.NewService(a.store, protocred.WithSecretEnvelope(a.envelope)).Get(accessKey)
	if err != nil {
		return protocred.Credential{}, false
	}
	return item, true
}

func (a *protocolCredentialAuth) authenticate(ctx context.Context, r *http.Request, protocol protocred.Protocol, resource string, allowRO bool) (context.Context, *authnFailure) {
	accessKey, err := a.verifier.VerifyFresh(r)
	if err != nil {
		return ctx, &authnFailure{
			status:  consts.StatusForbidden,
			code:    "AccessDenied",
			message: err.Error(),
			reason:  "protocol_credential_authn",
		}
	}
	secret, item, err := protocred.NewService(a.store, protocred.WithSecretEnvelope(a.envelope)).OpenSecret(accessKey)
	if err != nil || item.Protocol != protocol {
		return ctx, &authnFailure{
			status:  consts.StatusUnauthorized,
			code:    "InvalidAccessKeyId",
			message: "invalid protocol credential",
			reason:  "protocol_credential_invalid",
		}
	}
	decision, err := validateProtocolCredentialRequest(ctx, a.store, protocol, resource, accessKey, secret, allowRO)
	if err != nil {
		return ctx, &authnFailure{
			status:  consts.StatusForbidden,
			code:    "AccessDenied",
			message: "invalid protocol credential: " + decision.Reason,
			reason:  "protocol_credential_" + decision.Reason,
		}
	}
	ctx = WithAccessKey(ctx, accessKey)
	ctx = iam.WithPrincipal(ctx, decision.SAID)
	return ctx, nil
}

func validateProtocolCredentialRequest(ctx context.Context, store *protocred.Store, protocol protocred.Protocol, resource, credentialID, secret string, allowRO bool) (protocred.AttachDecision, error) {
	validator := protocred.NewAttachValidator(store)
	req := protocred.AttachRequest{
		Protocol:        protocol,
		Resource:        resource,
		CredentialID:    credentialID,
		PresentedSecret: secret,
		RequestedMode:   protocred.ModeRW,
		Strict:          true,
	}
	decision, err := validator.ValidateAttach(ctx, req)
	if err == nil || !allowRO {
		return decision, err
	}
	req.RequestedMode = protocred.ModeRO
	return validator.ValidateAttach(ctx, req)
}
